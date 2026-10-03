package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Distinguishable error classes.
var (
	ErrNotFound             = errors.New("object not found")
	ErrCorruptObject        = errors.New("corrupt object")
	ErrUnsupportedAlgorithm = errors.New("unsupported hash algorithm")
	ErrUnsupportedVersion   = errors.New("unsupported manifest version")
	ErrInvalidConfig        = errors.New("invalid configuration")
)

// CorruptionError identifies a specific object (and optionally chunk) that
// failed integrity verification.
type CorruptionError struct {
	ObjectID Hash
	ChunkID  Hash
	Reason   string
}

func (e *CorruptionError) Error() string {
	if e.ChunkID != "" {
		return fmt.Sprintf("corruption in object %s chunk %s: %s", e.ObjectID, e.ChunkID, e.Reason)
	}
	return fmt.Sprintf("corruption in object %s: %s", e.ObjectID, e.Reason)
}

func (e *CorruptionError) Unwrap() error { return ErrCorruptObject }

// Config controls store behaviour. Zero values are replaced by defaults.
type Config struct {
	Root          string
	MinChunkSize  int
	AvgChunkSize  int
	MaxChunkSize  int
	HashAlgorithm string
}

func (c *Config) withDefaults() {
	if c.MinChunkSize == 0 {
		c.MinChunkSize = 2048
	}
	if c.AvgChunkSize == 0 {
		c.AvgChunkSize = 8192
	}
	if c.MaxChunkSize == 0 {
		c.MaxChunkSize = 65536
	}
	if c.HashAlgorithm == "" {
		c.HashAlgorithm = AlgoSHA256
	}
}

func (c *Config) validate() error {
	if c.MinChunkSize <= 0 || c.AvgChunkSize <= 0 || c.MaxChunkSize <= 0 {
		return fmt.Errorf("%w: chunk sizes must be positive", ErrInvalidConfig)
	}
	if c.MinChunkSize > c.AvgChunkSize || c.AvgChunkSize > c.MaxChunkSize {
		return fmt.Errorf("%w: require min <= avg <= max", ErrInvalidConfig)
	}
	if c.AvgChunkSize&(c.AvgChunkSize-1) != 0 {
		return fmt.Errorf("%w: avg chunk size must be a power of two", ErrInvalidConfig)
	}
	return nil
}

// GCStats reports what a garbage collection run did.
type GCStats struct {
	ChunksDeleted    int64
	BytesReclaimed   int64
	ChunksScanned    int64
	ObjectsScanned   int64
	CorruptManifests int64
}

// VerifyReport reports the result of a store-wide integrity check.
type VerifyReport struct {
	ObjectsChecked     int64
	ObjectsCorrupt     int64
	ChunksChecked      int64
	ChunksMissing      int64
	ChunksCorrupt      int64
	UnreferencedChunks int64
	Errors             []error
}

// Stats provides a snapshot of store contents.
type Stats struct {
	Objects      int64
	Chunks       int64
	ChunkBytes   int64
	Unreferenced int64
}

// Stats returns a snapshot of store contents.
func (s *Store) Stats() (Stats, error) {
	var st Stats
	referenced := make(map[Hash]bool)
	err := filepath.WalkDir(s.objectsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		st.Objects++
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		var m Manifest
		if uerr := json.Unmarshal(data, &m); uerr != nil {
			return nil
		}
		for _, ce := range m.Chunks {
			referenced[ce.Hash] = true
		}
		return nil
	})
	if err != nil {
		return st, err
	}
	err = filepath.WalkDir(s.chunksDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		st.Chunks++
		if info, ierr := d.Info(); ierr == nil {
			st.ChunkBytes += info.Size()
		}
		h := s.hashFromChunkPath(path)
		if !referenced[h] {
			st.Unreferenced++
		}
		return nil
	})
	return st, err
}

// Store is a content-addressed, chunk-deduplicating object store backed by
// the local filesystem.
type Store struct {
	config     Config
	root       string
	chunksDir  string
	objectsDir string
	tmpDir     string
	spoolDir   string

	// inFlight coordinates writers (shared) against GC/delete (exclusive)
	// within the process. Cross-process coordination uses fileLock.
	inFlight sync.RWMutex
	fileLock *fileLock
}

// Open opens (or creates) a store rooted at config.Root.
func Open(config Config) (*Store, error) {
	if config.Root == "" {
		return nil, fmt.Errorf("%w: root is required", ErrInvalidConfig)
	}
	config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	if _, err := NewHasher(config.HashAlgorithm); err != nil {
		return nil, err
	}

	s := &Store{
		config:     config,
		root:       config.Root,
		chunksDir:  filepath.Join(config.Root, "chunks"),
		objectsDir: filepath.Join(config.Root, "objects"),
		tmpDir:     filepath.Join(config.Root, "tmp"),
		spoolDir:   filepath.Join(config.Root, "spool"),
	}
	for _, d := range []string{s.chunksDir, s.objectsDir, s.tmpDir, s.spoolDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return nil, err
		}
	}
	fl, err := newFileLock(filepath.Join(config.Root, "lock"))
	if err != nil {
		return nil, err
	}
	s.fileLock = fl
	return s, nil
}

// Close releases store resources.
func (s *Store) Close() error {
	if s.fileLock != nil {
		return s.fileLock.close()
	}
	return nil
}

// Config returns a copy of the store's configuration.
func (s *Store) Config() Config { return s.config }

// Write stages and commits an object. The object ID is derived from the
// content. Concurrent writes of the same content converge to one object.
func (s *Store) Write(ctx context.Context, r io.Reader) (Hash, error) {
	s.inFlight.RLock()
	defer s.inFlight.RUnlock()
	if err := s.fileLock.lockShared(); err != nil {
		return "", err
	}
	defer s.fileLock.unlock()

	tmpDir, err := os.MkdirTemp(s.tmpDir, "write-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpDir)

	chunker := NewChunker(s.config.MinChunkSize, s.config.AvgChunkSize, s.config.MaxChunkSize)
	hasher, err := NewHasher(s.config.HashAlgorithm)
	if err != nil {
		return "", err
	}
	var entries []ChunkEntry
	totalSize := int64(0)

	buf := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}
		n, err := r.Read(buf)
		if n > 0 {
			hasher.Write(buf[:n])
			totalSize += int64(n)
			chunks, cerr := chunker.Write(buf[:n])
			if cerr != nil {
				return "", cerr
			}
			for _, ch := range chunks {
				chHash, herr := HashBytes(s.config.HashAlgorithm, ch)
				if herr != nil {
					return "", herr
				}
				entries = append(entries, ChunkEntry{Hash: chHash, Size: int64(len(ch))})
				if serr := s.stageChunk(tmpDir, chHash, ch); serr != nil {
					return "", serr
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	final, err := chunker.Flush()
	if err != nil {
		return "", err
	}
	if len(final) > 0 {
		chHash, herr := HashBytes(s.config.HashAlgorithm, final)
		if herr != nil {
			return "", herr
		}
		entries = append(entries, ChunkEntry{Hash: chHash, Size: int64(len(final))})
		if serr := s.stageChunk(tmpDir, chHash, final); serr != nil {
			return "", serr
		}
	}

	objectID := Hash(s.config.HashAlgorithm + ":" + hex.EncodeToString(hasher.Sum(nil)))

	// Publish chunks before the manifest so a reader never sees a manifest
	// pointing at missing data.
	for _, e := range entries {
		if err := s.publishChunk(tmpDir, e.Hash); err != nil {
			return "", err
		}
	}

	m := Manifest{
		Version:           ManifestVersion,
		HashAlgorithm:     s.config.HashAlgorithm,
		ChunkingAlgorithm: ChunkingAlgorithmCDC,
		ChunkingParams: ChunkingParams{
			MinSize: s.config.MinChunkSize,
			AvgSize: s.config.AvgChunkSize,
			MaxSize: s.config.MaxChunkSize,
		},
		TotalSize: totalSize,
		Chunks:    entries,
	}
	if err := s.publishManifest(tmpDir, objectID, &m); err != nil {
		return "", err
	}

	return objectID, nil
}

// Read returns a verified, complete object. The caller must close the
// returned reader. The object is fully verified before any bytes are
// served, so corrupt data is never returned.
func (s *Store) Read(ctx context.Context, id Hash) (io.ReadCloser, error) {
	m, err := s.readManifest(id)
	if err != nil {
		return nil, err
	}
	if m.Version != ManifestVersion {
		return nil, fmt.Errorf("%w: manifest version %d", ErrUnsupportedVersion, m.Version)
	}
	if _, err := NewHasher(m.HashAlgorithm); err != nil {
		return nil, err
	}

	// Spool verified bytes to a temp file (unlinked immediately so no
	// residue survives a crash). Serve from the spool after full
	// verification succeeds.
	spool, err := os.CreateTemp(s.spoolDir, "read-*")
	if err != nil {
		return nil, err
	}
	spoolPath := spool.Name()
	os.Remove(spoolPath)

	hasher, err := NewHasher(m.HashAlgorithm)
	if err != nil {
		spool.Close()
		return nil, err
	}
	totalSize := int64(0)
	for _, ce := range m.Chunks {
		select {
		case <-ctx.Done():
			spool.Close()
			return nil, ctx.Err()
		default:
		}
		chunkData, err := os.ReadFile(s.chunkPath(ce.Hash))
		if err != nil {
			spool.Close()
			if os.IsNotExist(err) {
				return nil, &CorruptionError{ObjectID: id, ChunkID: ce.Hash, Reason: "chunk missing"}
			}
			return nil, &CorruptionError{ObjectID: id, ChunkID: ce.Hash, Reason: err.Error()}
		}
		if int64(len(chunkData)) != ce.Size {
			spool.Close()
			return nil, &CorruptionError{ObjectID: id, ChunkID: ce.Hash, Reason: fmt.Sprintf("chunk size mismatch: got %d, want %d", len(chunkData), ce.Size)}
		}
		actualHash, err := HashBytes(m.HashAlgorithm, chunkData)
		if err != nil {
			spool.Close()
			return nil, err
		}
		if actualHash != ce.Hash {
			spool.Close()
			return nil, &CorruptionError{ObjectID: id, ChunkID: ce.Hash, Reason: fmt.Sprintf("chunk hash mismatch: got %s, want %s", actualHash, ce.Hash)}
		}
		hasher.Write(chunkData)
		if _, err := spool.Write(chunkData); err != nil {
			spool.Close()
			return nil, err
		}
		totalSize += int64(len(chunkData))
	}
	if totalSize != m.TotalSize {
		spool.Close()
		return nil, &CorruptionError{ObjectID: id, Reason: fmt.Sprintf("total size mismatch: got %d, want %d", totalSize, m.TotalSize)}
	}
	actualID := Hash(m.HashAlgorithm + ":" + hex.EncodeToString(hasher.Sum(nil)))
	if actualID != id {
		spool.Close()
		return nil, &CorruptionError{ObjectID: id, Reason: fmt.Sprintf("object hash mismatch: got %s", actualID)}
	}
	if err := spool.Sync(); err != nil {
		spool.Close()
		return nil, err
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		spool.Close()
		return nil, err
	}
	return spool, nil
}

// Delete removes an object's manifest. Its chunks become eligible for GC.
func (s *Store) Delete(ctx context.Context, id Hash) error {
	s.inFlight.Lock()
	defer s.inFlight.Unlock()
	if err := s.fileLock.lockExclusive(); err != nil {
		return err
	}
	defer s.fileLock.unlock()

	path := s.objectPath(id)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	s.cleanEmptyDirs(filepath.Dir(path))
	return nil
}

// GC deletes chunks not referenced by any committed object. It is safe to
// interrupt and re-run.
func (s *Store) GC(ctx context.Context) (GCStats, error) {
	s.inFlight.Lock()
	defer s.inFlight.Unlock()
	if err := s.fileLock.lockExclusive(); err != nil {
		return GCStats{}, err
	}
	defer s.fileLock.unlock()

	// No writers are active (exclusive lock held), so stale temp dirs from
	// crashed writers can be removed safely. We remove only the contents of
	// tmpDir (not the directory itself) so concurrent reads' spool creation
	// never sees a missing directory.
	if err := removeDirContents(s.tmpDir); err != nil {
		return GCStats{}, err
	}

	referenced := make(map[Hash]bool)
	stats := GCStats{}

	err := filepath.WalkDir(s.objectsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		stats.ObjectsScanned++
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			stats.CorruptManifests++
			return nil
		}
		var m Manifest
		if uerr := json.Unmarshal(data, &m); uerr != nil {
			stats.CorruptManifests++
			return nil
		}
		for _, ce := range m.Chunks {
			referenced[ce.Hash] = true
		}
		return nil
	})
	if err != nil {
		return stats, err
	}

	err = filepath.WalkDir(s.chunksDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		stats.ChunksScanned++
		h := s.hashFromChunkPath(path)
		if !referenced[h] {
			info, ierr := d.Info()
			if ierr == nil {
				stats.BytesReclaimed += info.Size()
			}
			if derr := os.Remove(path); derr != nil {
				return derr
			}
			stats.ChunksDeleted++
		}
		return nil
	})
	if err != nil {
		return stats, err
	}

	s.cleanEmptyDirs(s.chunksDir)
	s.cleanEmptyDirs(s.objectsDir)
	return stats, nil
}

// Verify checks the integrity of every committed object and chunk.
func (s *Store) Verify(ctx context.Context) (VerifyReport, error) {
	report := VerifyReport{}
	referenced := make(map[Hash]bool)

	err := filepath.WalkDir(s.objectsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		report.ObjectsChecked++
		objID := s.objectIDFromPath(path)
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			report.ObjectsCorrupt++
			report.Errors = append(report.Errors, &CorruptionError{ObjectID: objID, Reason: "cannot read manifest: " + rerr.Error()})
			return nil
		}
		var m Manifest
		if uerr := json.Unmarshal(data, &m); uerr != nil {
			report.ObjectsCorrupt++
			report.Errors = append(report.Errors, &CorruptionError{ObjectID: objID, Reason: "cannot parse manifest: " + uerr.Error()})
			return nil
		}
		if m.Version != ManifestVersion {
			report.ObjectsCorrupt++
			report.Errors = append(report.Errors, &CorruptionError{ObjectID: objID, Reason: fmt.Sprintf("unsupported manifest version %d", m.Version)})
			return nil
		}
		hasher, herr := NewHasher(m.HashAlgorithm)
		if herr != nil {
			report.ObjectsCorrupt++
			report.Errors = append(report.Errors, &CorruptionError{ObjectID: objID, Reason: herr.Error()})
			return nil
		}
		totalSize := int64(0)
		for _, ce := range m.Chunks {
			referenced[ce.Hash] = true
			report.ChunksChecked++
			chunkData, cerr := os.ReadFile(s.chunkPath(ce.Hash))
			if cerr != nil {
				report.ChunksMissing++
				report.Errors = append(report.Errors, &CorruptionError{ObjectID: objID, ChunkID: ce.Hash, Reason: "chunk missing"})
				continue
			}
			if int64(len(chunkData)) != ce.Size {
				report.ChunksCorrupt++
				report.Errors = append(report.Errors, &CorruptionError{ObjectID: objID, ChunkID: ce.Hash, Reason: "chunk size mismatch"})
				continue
			}
			actualHash, berr := HashBytes(m.HashAlgorithm, chunkData)
			if berr != nil {
				report.Errors = append(report.Errors, berr)
				continue
			}
			if actualHash != ce.Hash {
				report.ChunksCorrupt++
				report.Errors = append(report.Errors, &CorruptionError{ObjectID: objID, ChunkID: ce.Hash, Reason: fmt.Sprintf("chunk hash mismatch: got %s", actualHash)})
				continue
			}
			hasher.Write(chunkData)
			totalSize += int64(len(chunkData))
		}
		if totalSize != m.TotalSize {
			report.ObjectsCorrupt++
			report.Errors = append(report.Errors, &CorruptionError{ObjectID: objID, Reason: "total size mismatch"})
			return nil
		}
		actualID := Hash(m.HashAlgorithm + ":" + hex.EncodeToString(hasher.Sum(nil)))
		if actualID != objID {
			report.ObjectsCorrupt++
			report.Errors = append(report.Errors, &CorruptionError{ObjectID: objID, Reason: fmt.Sprintf("object hash mismatch: got %s", actualID)})
		}
		return nil
	})
	if err != nil {
		return report, err
	}

	err = filepath.WalkDir(s.chunksDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		h := s.hashFromChunkPath(path)
		if !referenced[h] {
			report.UnreferencedChunks++
		}
		return nil
	})
	if err != nil {
		return report, err
	}

	return report, nil
}

// --- path helpers ---

func (s *Store) chunkPath(h Hash) string {
	d := h.Digest()
	return filepath.Join(s.chunksDir, d[:2], d[2:])
}

func (s *Store) objectPath(h Hash) string {
	d := h.Digest()
	return filepath.Join(s.objectsDir, d[:2], d[2:])
}

func (s *Store) hashFromChunkPath(path string) Hash {
	rel, err := filepath.Rel(s.chunksDir, path)
	if err != nil {
		return ""
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) != 2 {
		return ""
	}
	return Hash(s.config.HashAlgorithm + ":" + parts[0] + parts[1])
}

func (s *Store) objectIDFromPath(path string) Hash {
	rel, err := filepath.Rel(s.objectsDir, path)
	if err != nil {
		return ""
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) != 2 {
		return ""
	}
	return Hash(s.config.HashAlgorithm + ":" + parts[0] + parts[1])
}

// --- staging / publishing ---

func (s *Store) stageChunk(tmpDir string, h Hash, data []byte) error {
	dir := filepath.Join(tmpDir, "chunks")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	path := filepath.Join(dir, string(h))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (s *Store) publishChunk(tmpDir string, h Hash) error {
	dst := s.chunkPath(h)
	if _, err := os.Stat(dst); err == nil {
		return nil // already present (dedup)
	}
	src := filepath.Join(tmpDir, "chunks", string(h))
	if _, err := os.Stat(src); err != nil {
		return nil // already published (within-object dedup)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dst))
}

func (s *Store) publishManifest(tmpDir string, id Hash, m *Manifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(tmpDir, "manifest")
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	dst := s.objectPath(id)
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dst))
}

func (s *Store) readManifest(id Hash) (*Manifest, error) {
	if !id.Valid() {
		return nil, ErrNotFound
	}
	data, err := os.ReadFile(s.objectPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, &CorruptionError{ObjectID: id, Reason: "cannot parse manifest: " + err.Error()}
	}
	return &m, nil
}

func removeDirContents(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (s *Store) cleanEmptyDirs(root string) {
	filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == root {
			return nil
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil
		}
		if len(entries) == 0 {
			os.Remove(path)
		}
		return nil
	})
}
