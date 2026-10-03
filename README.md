# Content-Addressed Dedup Store

A content-addressed, chunk-deduplicating object store backed entirely by the
local filesystem. No external services are required.

## Features

- **Content addressing**: each object is identified by the SHA-256 digest of
  its content. Identical content always produces the same identifier.
- **Chunk-level deduplication**: objects are split into content-defined chunks
  using a Gear rolling hash. Identical chunks (within or across objects) are
  stored only once. Inserting or deleting bytes in the middle of an object
  leaves most chunks unchanged.
- **Atomic commits**: writes are staged in a temporary directory and published
  via atomic `rename`. A partially written object is never visible to readers.
- **Crash recovery**: on reopen, committed objects are intact and verifiable.
  Stale temporary files from interrupted writes are reclaimed by GC.
- **Garbage collection**: unreferenced chunks are reclaimed. GC coordinates
  with in-flight writes and is safe to interrupt and re-run.
- **Integrity verification**: every read verifies chunk hashes, chunk sizes,
  total size, and the full object digest. Corruption is reported with the
  specific object and chunk affected.
- **Versioned semantics**: manifests record the hash algorithm and chunking
  parameters used at write time. Old objects remain readable after defaults
  change.

## Addressing Semantics

- **Object ID** = `sha256:` + hex(SHA-256(content)). The ID is derived from the
  full object bytes, so identical content always converges to the same ID.
- **Chunk ID** = `sha256:` + hex(SHA-256(chunk bytes)). Chunks are
  content-addressed and shared across objects.
- Object IDs and chunk IDs include an algorithm prefix (e.g. `sha256:`) so
  that future algorithm changes do not ambiguity old identifiers.

## Chunking

Objects are split using a content-defined chunking (CDC) algorithm based on a
Gear rolling hash:

- A 64-bit rolling hash is maintained as bytes are fed through.
- A chunk boundary is cut when `(hash & (avgSize - 1)) == 0`, after the minimum
  chunk size is reached.
- Chunks are capped at the maximum size.
- Because boundaries depend on content (not position), inserting or deleting
  bytes in the middle only affects the chunks near the edit point.

Default parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| MinChunkSize | 2048 | Minimum chunk size in bytes |
| AvgChunkSize | 8192 | Target average chunk size (must be a power of two) |
| MaxChunkSize | 65536 | Maximum chunk size in bytes |

The chunking algorithm version is `cdc-gear-v1`. The Gear table is generated
deterministically from a fixed seed, so chunk boundaries are stable across
runs.

## Storage Layout

```
<root>/
  chunks/
    <aa>/<bb><cc>...   # chunk files, named by digest
  objects/
    <aa>/<bb><cc>...   # manifest files, named by object ID
  tmp/
    write-<random>/    # staging area for in-progress writes
  spool/
    read-<random>      # verified read spool files (unlinked immediately)
  lock                 # flock coordination file
```

Manifests are JSON documents:

```json
{
  "version": 1,
  "hash_algorithm": "sha256",
  "chunking_algorithm": "cdc-gear-v1",
  "chunking_params": {
    "min_size": 2048,
    "avg_size": 8192,
    "max_size": 65536
  },
  "total_size": 12345,
  "chunks": [
    {"hash": "sha256:...", "size": 8192},
    {"hash": "sha256:...", "size": 4153}
  ]
}
```

## Write Path

1. The object is streamed through the chunker. Each chunk is written to a
   temporary staging file and fsynced.
2. The full content hash is computed incrementally.
3. After all chunks are staged, chunks are published by atomic `rename` into
   `chunks/`. Already-present chunks are skipped (dedup).
4. The manifest is written to a temporary file, fsynced, and atomically
   renamed into `objects/`.
5. Directory fsyncs ensure rename durability.

Because chunks are published before the manifest, a reader never sees a
manifest pointing at missing data. Because all publishing uses atomic rename,
a reader always sees either the old complete state or the new complete state.

## Read Path

1. The manifest is read and parsed. A corrupt or missing manifest is reported.
2. Each chunk is read, its size and hash verified against the manifest.
3. Verified bytes are spooled to a temporary file (unlinked immediately).
4. The total size and full object hash are verified against the object ID.
5. Only after all checks pass is the spool file seeked to the start and served.

No corrupt data is ever returned to the caller.

## Concurrency Model

- Writers hold a shared lock (`sync.RWMutex.RLock` + `flock LOCK_SH`). Multiple
  writers proceed concurrently; reads are not blocked.
- GC and Delete hold an exclusive lock (`sync.RWMutex.Lock` + `flock LOCK_EX`).
  This ensures GC never scans a manifest set that is changing.
- Reads require no lock: they read immutable chunk files and atomically
  renamed manifests.
- Cross-process coordination uses `flock`, so multiple store processes on the
  same root coordinate correctly.

A slow writer holds a shared lock for its duration. This delays GC (which needs
the exclusive lock) but does not block other writers or readers.

## Garbage Collection

GC performs the following steps under the exclusive lock:

1. Remove stale temporary files from `tmp/` (no writers are active).
2. Scan all manifests and collect the set of referenced chunk hashes.
3. Scan all chunk files. Delete any chunk not in the referenced set.
4. Remove empty subdirectories.

GC is safe to interrupt (e.g. process kill or context cancellation) and
re-run. A partial GC leaves the store in a consistent state: re-running
computes the correct referenced set and deletes remaining unreferenced chunks.

**GC never deletes a chunk that is referenced by a committed object.** The
exclusive lock guarantees no manifest is committed or deleted during the scan.

## Integrity Verification

`Verify()` scans every object and chunk and reports:

- Objects checked / corrupt
- Chunks checked / missing / corrupt
- Unreferenced chunks (GC candidates)
- Per-error details identifying the specific object and chunk

`CorruptionError` includes `ObjectID`, `ChunkID` (if applicable), and a
human-readable `Reason`. Use `errors.As` to extract it.

## Versioning

- The manifest format version is recorded (`version: 1`).
- The hash algorithm is recorded per manifest. Objects are read using the
  algorithm recorded in their manifest, so changing the default hash algorithm
  does not affect existing objects.
- The chunking algorithm and parameters are recorded per manifest. Reading
  does not use chunking parameters (it just reads the listed chunks), so
  changing chunking defaults never makes an old object unreadable.
- New writes always use the current store configuration.

## Configuration

```go
type Config struct {
    Root          string // required: store directory
    MinChunkSize  int    // default 2048
    AvgChunkSize  int    // default 8192, must be power of two
    MaxChunkSize  int    // default 65536
    HashAlgorithm string // default "sha256"
}
```

## API

```go
func Open(config Config) (*Store, error)
func (s *Store) Close() error
func (s *Store) Write(ctx context.Context, r io.Reader) (Hash, error)
func (s *Store) Read(ctx context.Context, id Hash) (io.ReadCloser, error)
func (s *Store) Delete(ctx context.Context, id Hash) error
func (s *Store) GC(ctx context.Context) (GCStats, error)
func (s *Store) Verify(ctx context.Context) (VerifyReport, error)
func (s *Store) Stats() (Stats, error)
func (s *Store) Config() Config
```

## Testing

```bash
go test ./... -race -count=1
```

Tests cover: write/read roundtrip, empty objects, cross-object and
within-object dedup, boundary drift, concurrent writes and reads, crash
recovery, GC reclamation, GC dedup protection, GC concurrent with writes, GC
interruption and re-run, corrupt chunk/manifest/missing chunk detection,
verify, versioned chunking params, large objects, and config validation.
