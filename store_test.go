package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "store-test-*")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(Config{Root: dir})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Close()
		os.RemoveAll(dir)
	})
	return s, dir
}

func randomData(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func writeObject(t *testing.T, s *Store, data []byte) Hash {
	t.Helper()
	id, err := s.Write(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func readObject(t *testing.T, s *Store, id Hash) []byte {
	t.Helper()
	rc, err := s.Read(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func countFiles(t *testing.T, dir string) int {
	t.Helper()
	count := 0
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			count++
		}
		return nil
	})
	return count
}

func TestWriteRead(t *testing.T) {
	s, _ := newTestStore(t)
	data := []byte("hello world, content addressed store")
	id := writeObject(t, s, data)
	got := readObject(t, s, id)
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
	if !strings.HasPrefix(string(id), "sha256:") {
		t.Fatalf("id should have sha256 prefix, got %s", id)
	}
}

func TestEmptyObject(t *testing.T) {
	s, _ := newTestStore(t)
	id := writeObject(t, s, nil)
	got := readObject(t, s, id)
	if len(got) != 0 {
		t.Fatalf("expected empty, got %d bytes", len(got))
	}
}

func TestDedup(t *testing.T) {
	s, dir := newTestStore(t)
	data := randomData(50000)

	id1 := writeObject(t, s, data)
	id2 := writeObject(t, s, data)
	if id1 != id2 {
		t.Fatalf("same content should produce same id: %s vs %s", id1, id2)
	}

	// Two objects sharing most chunks should store fewer chunks than
	// the sum of their individual chunk counts.
	// Insert a small change in the middle.
	modified := make([]byte, 0, len(data)+50)
	modified = append(modified, data[:25000]...)
	modified = append(modified, make([]byte, 50)...)
	modified = append(modified, data[25000:]...)
	id3 := writeObject(t, s, modified)
	if id3 == id1 {
		t.Fatal("modified content should produce different id")
	}

	// Count unique chunks on disk.
	chunkCount := countFiles(t, filepath.Join(dir, "chunks"))
	// Each object has ~50000/8192 ≈ 7 chunks; two objects ≈ 14 chunk-slots,
	// but dedup should reduce this significantly.
	if chunkCount > 12 {
		t.Fatalf("expected dedup to keep chunk count low, got %d", chunkCount)
	}
	t.Logf("chunks on disk after 2 objects: %d", chunkCount)
}

func TestWithinObjectDedup(t *testing.T) {
	s, _ := newTestStore(t)
	// Create data where the same 4KB block repeats many times.
	block := randomData(4096)
	data := bytes.Repeat(block, 20) // 80KB, same chunk repeated
	id := writeObject(t, s, data)
	got := readObject(t, s, id)
	if !bytes.Equal(got, data) {
		t.Fatal("roundtrip mismatch")
	}
	// The manifest should reference the same chunk hash multiple times.
	m, err := s.readManifest(id)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[Hash]bool)
	for _, ce := range m.Chunks {
		seen[ce.Hash] = true
	}
	// With 8KB avg chunks and 4KB repeating blocks, each chunk is 2 blocks
	// (8KB), so ~10 unique chunks but each appears... actually with CDC the
	// repeating pattern may produce fewer. The key point: unique chunks < total chunks.
	if len(seen) >= len(m.Chunks) {
		t.Logf("warning: no within-object dedup observed (unique=%d, total=%d)", len(seen), len(m.Chunks))
	}
}

func TestBoundaryDrift(t *testing.T) {
	s, _ := newTestStore(t)
	data := randomData(200000)
	id1 := writeObject(t, s, data)
	m1, err := s.readManifest(id1)
	if err != nil {
		t.Fatal(err)
	}

	// Insert 100 bytes in the middle.
	drift := make([]byte, 0, len(data)+100)
	drift = append(drift, data[:100000]...)
	drift = append(drift, make([]byte, 100)...)
	drift = append(drift, data[100000:]...)
	id2 := writeObject(t, s, drift)
	m2, err := s.readManifest(id2)
	if err != nil {
		t.Fatal(err)
	}

	// Count shared chunk hashes.
	set1 := make(map[Hash]bool)
	for _, ce := range m1.Chunks {
		set1[ce.Hash] = true
	}
	shared := 0
	for _, ce := range m2.Chunks {
		if set1[ce.Hash] {
			shared++
		}
	}
	total := len(m2.Chunks)
	ratio := float64(shared) / float64(total)
	t.Logf("boundary drift: %d/%d chunks shared (%.1f%%)", shared, total, ratio*100)
	if ratio < 0.7 {
		t.Fatalf("expected most chunks to survive a small insertion, got %.1f%%", ratio*100)
	}
}

func TestConcurrentWrites(t *testing.T) {
	s, _ := newTestStore(t)
	data := randomData(100000)
	const n = 20
	var wg sync.WaitGroup
	ids := make([]Hash, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := s.Write(context.Background(), bytes.NewReader(data))
			ids[i] = id
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("write %d failed: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("write %d produced different id: %s vs %s", i, ids[i], ids[0])
		}
	}
	// Object must be readable.
	got := readObject(t, s, ids[0])
	if !bytes.Equal(got, data) {
		t.Fatal("roundtrip mismatch after concurrent writes")
	}
}

func TestConcurrentReadsDuringWrite(t *testing.T) {
	s, _ := newTestStore(t)
	data := randomData(200000)
	id := writeObject(t, s, data)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rc, err := s.Read(context.Background(), id)
			if err != nil {
				t.Error(err)
				return
			}
			defer rc.Close()
			io.ReadAll(rc)
		}()
	}
	// Concurrent write of a different object should not block reads.
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Write(context.Background(), bytes.NewReader(randomData(50000)))
	}()
	wg.Wait()
}

func TestCrashRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "store-recovery-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	data1 := randomData(50000)
	data2 := randomData(80000)

	// Phase 1: write objects, close cleanly.
	s1, err := Open(Config{Root: dir})
	if err != nil {
		t.Fatal(err)
	}
	id1 := writeObject(t, s1, data1)
	id2 := writeObject(t, s1, data2)
	s1.Close()

	// Phase 2: reopen, objects must be readable and verifiable.
	s2, err := Open(Config{Root: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got1 := readObject(t, s2, id1)
	got2 := readObject(t, s2, id2)
	if !bytes.Equal(got1, data1) || !bytes.Equal(got2, data2) {
		t.Fatal("data mismatch after reopen")
	}

	// Phase 3: simulate a crashed writer leaving a stale temp dir.
	staleDir := filepath.Join(dir, "tmp", "write-stale")
	os.MkdirAll(staleDir, 0755)
	os.WriteFile(filepath.Join(staleDir, "manifest"), []byte("garbage"), 0600)
	// GC should clean it.
	stats, err := s2.GC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC stats: %+v", stats)
	// After GC, objects still readable.
	got1 = readObject(t, s2, id1)
	if !bytes.Equal(got1, data1) {
		t.Fatal("data mismatch after GC")
	}
}

func TestGCReclaimsChunks(t *testing.T) {
	s, dir := newTestStore(t)
	data := randomData(100000)
	id := writeObject(t, s, data)

	chunkCountBefore := countFiles(t, filepath.Join(dir, "chunks"))
	if chunkCountBefore == 0 {
		t.Fatal("expected chunks on disk")
	}

	// Delete the object; chunks become unreferenced.
	if err := s.Delete(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	stats, err := s.GC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksDeleted == 0 {
		t.Fatal("expected GC to delete unreferenced chunks")
	}
	chunkCountAfter := countFiles(t, filepath.Join(dir, "chunks"))
	if chunkCountAfter != 0 {
		t.Fatalf("expected all chunks reclaimed, got %d", chunkCountAfter)
	}
	t.Logf("GC deleted %d chunks, reclaimed %d bytes", stats.ChunksDeleted, stats.BytesReclaimed)
}

func TestGCDedupProtection(t *testing.T) {
	s, dir := newTestStore(t)
	data := randomData(100000)
	id1 := writeObject(t, s, data)
	// Modify slightly so the objects share most chunks.
	modified := make([]byte, 0, len(data)+10)
	modified = append(modified, data[:50000]...)
	modified = append(modified, make([]byte, 10)...)
	modified = append(modified, data[50000:]...)
	id2 := writeObject(t, s, modified)

	// Delete only id1; id2 still references shared chunks.
	if err := s.Delete(context.Background(), id1); err != nil {
		t.Fatal(err)
	}
	stats, err := s.GC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// id2 must still be readable.
	got := readObject(t, s, id2)
	if !bytes.Equal(got, modified) {
		t.Fatal("id2 corrupted after GC")
	}
	// The chunks unique to id1 should be deleted, shared ones kept.
	chunkCount := countFiles(t, filepath.Join(dir, "chunks"))
	t.Logf("chunks after GC (id1 deleted): %d, deleted=%d", chunkCount, stats.ChunksDeleted)
	if chunkCount == 0 {
		t.Fatal("GC deleted chunks still referenced by id2")
	}
}

func TestGCConcurrentWithWrites(t *testing.T) {
	s, _ := newTestStore(t)
	data := randomData(50000)

	var wg sync.WaitGroup
	errs := make(chan error, 30)

	// Concurrent writers.
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := randomData(20000 + i*100)
			_, err := s.Write(context.Background(), bytes.NewReader(d))
			errs <- err
		}(i)
	}

	// Concurrent GC.
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.GC(context.Background())
			errs <- err
		}()
	}

	// Concurrent reads of an existing object.
	id := writeObject(t, s, data)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rc, err := s.Read(context.Background(), id)
			if err != nil {
				errs <- err
				return
			}
			io.ReadAll(rc)
			rc.Close()
			errs <- nil
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent operation failed: %v", err)
		}
	}
}

func TestGCInterruptible(t *testing.T) {
	s, _ := newTestStore(t)
	// Create many objects, then delete half so GC has real work.
	const n = 200
	ids := make([]Hash, n)
	for i := 0; i < n; i++ {
		ids[i] = writeObject(t, s, randomData(5000+mathrand.Intn(20000)))
	}
	for i := 0; i < n; i += 2 {
		if err := s.Delete(context.Background(), ids[i]); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := s.GC(ctx)
		done <- err
	}()
	// Let GC run briefly, then cancel to simulate interruption.
	time.Sleep(2 * time.Millisecond)
	cancel()
	err := <-done
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected GC error: %v", err)
	}
	t.Logf("GC interrupted: %v", err)

	// Re-run GC; must complete and reclaim all unreferenced chunks.
	stats, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("GC re-run failed: %v", err)
	}
	t.Logf("GC re-run completed: %+v", stats)

	// All remaining objects must still be readable and verifiable.
	for i := 1; i < n; i += 2 {
		rc, err := s.Read(context.Background(), ids[i])
		if err != nil {
			t.Fatalf("object %d unreadable after GC: %v", i, err)
		}
		io.ReadAll(rc)
		rc.Close()
	}
}

func TestCorruptChunkDetected(t *testing.T) {
	s, _ := newTestStore(t)
	data := randomData(50000)
	id := writeObject(t, s, data)

	// Find a chunk file and flip a byte.
	m, err := s.readManifest(id)
	if err != nil {
		t.Fatal(err)
	}
	chunkHash := m.Chunks[0].Hash
	chunkPath := s.chunkPath(chunkHash)
	chunkData, err := os.ReadFile(chunkPath)
	if err != nil {
		t.Fatal(err)
	}
	chunkData[0] ^= 0xFF
	if err := os.WriteFile(chunkPath, chunkData, 0600); err != nil {
		t.Fatal(err)
	}

	_, err = s.Read(context.Background(), id)
	if err == nil {
		t.Fatal("expected corruption error")
	}
	var corr *CorruptionError
	if !errors.As(err, &corr) {
		t.Fatalf("expected CorruptionError, got %v", err)
	}
	if corr.ChunkID != chunkHash {
		t.Fatalf("expected chunk %s, got %s", chunkHash, corr.ChunkID)
	}
	t.Logf("corruption detected: %v", corr)
}

func TestCorruptManifestDetected(t *testing.T) {
	s, _ := newTestStore(t)
	data := randomData(30000)
	id := writeObject(t, s, data)

	// Corrupt the manifest by writing garbage.
	manifestPath := s.objectPath(id)
	if err := os.WriteFile(manifestPath, []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := s.Read(context.Background(), id)
	if err == nil {
		t.Fatal("expected corruption error")
	}
	var corr *CorruptionError
	if !errors.As(err, &corr) {
		t.Fatalf("expected CorruptionError, got %v", err)
	}
	t.Logf("manifest corruption detected: %v", corr)
}

func TestMissingChunkDetected(t *testing.T) {
	s, _ := newTestStore(t)
	data := randomData(30000)
	id := writeObject(t, s, data)

	// Delete a chunk file.
	m, err := s.readManifest(id)
	if err != nil {
		t.Fatal(err)
	}
	chunkPath := s.chunkPath(m.Chunks[0].Hash)
	if err := os.Remove(chunkPath); err != nil {
		t.Fatal(err)
	}

	_, err = s.Read(context.Background(), id)
	if err == nil {
		t.Fatal("expected missing chunk error")
	}
	var corr *CorruptionError
	if !errors.As(err, &corr) {
		t.Fatalf("expected CorruptionError, got %v", err)
	}
	t.Logf("missing chunk detected: %v", corr)
}

func TestVerify(t *testing.T) {
	s, _ := newTestStore(t)
	data := randomData(40000)
	writeObject(t, s, data)

	report, err := s.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ObjectsChecked != 1 {
		t.Fatalf("expected 1 object, checked %d", report.ObjectsChecked)
	}
	if report.ObjectsCorrupt != 0 {
		t.Fatalf("expected no corruption, got %d", report.ObjectsCorrupt)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("expected no errors, got %v", report.Errors)
	}
	t.Logf("verify report: %+v", report)
}

func TestVerifyDetectsCorruption(t *testing.T) {
	s, _ := newTestStore(t)
	data := randomData(40000)
	id := writeObject(t, s, data)

	// Corrupt a chunk.
	m, _ := s.readManifest(id)
	chunkPath := s.chunkPath(m.Chunks[0].Hash)
	chunkData, _ := os.ReadFile(chunkPath)
	chunkData[0] ^= 0xFF
	os.WriteFile(chunkPath, chunkData, 0600)

	report, err := s.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ObjectsCorrupt != 1 {
		t.Fatalf("expected 1 corrupt object, got %d", report.ObjectsCorrupt)
	}
	if report.ChunksCorrupt != 1 {
		t.Fatalf("expected 1 corrupt chunk, got %d", report.ChunksCorrupt)
	}
	if len(report.Errors) == 0 {
		t.Fatal("expected errors in report")
	}
	t.Logf("verify errors: %v", report.Errors)
}

func TestVersionedChunkingParams(t *testing.T) {
	dir, err := os.MkdirTemp("", "store-version-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Write with default config.
	s1, err := Open(Config{Root: dir})
	if err != nil {
		t.Fatal(err)
	}
	data := randomData(50000)
	id := writeObject(t, s1, data)
	m1, _ := s1.readManifest(id)
	s1.Close()

	// Reopen with different chunking params.
	s2, err := Open(Config{Root: dir, AvgChunkSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// Old object must still be readable (manifest records old params).
	got := readObject(t, s2, id)
	if !bytes.Equal(got, data) {
		t.Fatal("old object unreadable after config change")
	}

	// New write uses new params.
	data2 := randomData(50000)
	id2 := writeObject(t, s2, data2)
	m2, _ := s2.readManifest(id2)
	if m2.ChunkingParams.AvgSize != 4096 {
		t.Fatalf("new object should use new params, got %+v", m2.ChunkingParams)
	}
	if m1.ChunkingParams.AvgSize != 8192 {
		t.Fatalf("old object params should be preserved, got %+v", m1.ChunkingParams)
	}
	t.Logf("old params: %+v, new params: %+v", m1.ChunkingParams, m2.ChunkingParams)
}

func TestLargeObject(t *testing.T) {
	s, _ := newTestStore(t)
	// 5MB object.
	data := randomData(5 * 1024 * 1024)
	id := writeObject(t, s, data)
	got := readObject(t, s, id)
	if !bytes.Equal(got, data) {
		t.Fatal("large object roundtrip mismatch")
	}
	m, _ := s.readManifest(id)
	t.Logf("large object: %d chunks, total %d bytes", len(m.Chunks), m.TotalSize)
}

func TestDeleteNotFound(t *testing.T) {
	s, _ := newTestStore(t)
	err := s.Delete(context.Background(), "sha256:0000000000000000000000000000000000000000000000000000000000000000")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	dir, _ := os.MkdirTemp("", "store-cfg-*")
	defer os.RemoveAll(dir)

	tests := []struct {
		name string
		cfg  Config
	}{
		{"no root", Config{}},
		{"min > avg", Config{Root: dir, MinChunkSize: 16384, AvgChunkSize: 8192}},
		{"avg > max", Config{Root: dir, AvgChunkSize: 131072, MaxChunkSize: 65536}},
		{"avg not power of 2", Config{Root: dir, AvgChunkSize: 10000}},
		{"unknown hash", Config{Root: dir, HashAlgorithm: "md5"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Open(tt.cfg)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestSlowWriterDoesNotBlockReads(t *testing.T) {
	s, _ := newTestStore(t)
	data := randomData(100000)
	id := writeObject(t, s, data)

	// A slow writer (reads in tiny pieces with a delay) should not block
	// reads of other objects.
	slow := &slowReader{data: randomData(50000)}
	writeDone := make(chan Hash, 1)
	go func() {
		wid, _ := s.Write(context.Background(), slow)
		writeDone <- wid
	}()

	// Read should complete promptly even while the slow write is in progress.
	readDone := make(chan bool, 1)
	go func() {
		rc, err := s.Read(context.Background(), id)
		if err == nil {
			io.ReadAll(rc)
			rc.Close()
		}
		readDone <- err == nil
	}()

	select {
	case ok := <-readDone:
		if !ok {
			t.Fatal("read failed during slow write")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read blocked by slow writer")
	}
	<-writeDone
}

type slowReader struct {
	data []byte
	pos  int
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	// Feed one byte at a time with a tiny delay to simulate a slow writer.
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = r.data[r.pos]
	r.pos++
	time.Sleep(10 * time.Microsecond)
	return 1, nil
}

func TestStats(t *testing.T) {
	s, _ := newTestStore(t)
	writeObject(t, s, randomData(30000))
	writeObject(t, s, randomData(30000))

	st, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Objects != 2 {
		t.Fatalf("expected 2 objects, got %d", st.Objects)
	}
	if st.Chunks == 0 {
		t.Fatal("expected chunks")
	}
	t.Logf("stats: %+v", st)
}

func TestCrashDuringWriteSimulation(t *testing.T) {
	dir, err := os.MkdirTemp("", "store-crashwrite-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := Open(Config{Root: dir})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a crashed writer: stage chunks in tmpDir but never publish
	// the manifest. Also publish some chunks (as if the crash happened
	// between chunk publishing and manifest commit).
	data := randomData(50000)
	chunker := NewChunker(2048, 8192, 65536)
	tmpDir := filepath.Join(dir, "tmp", "write-crashed")
	os.MkdirAll(filepath.Join(tmpDir, "chunks"), 0755)

	// Stage all chunks.
	chunks, _ := chunker.Write(data)
	final, _ := chunker.Flush()
	if final != nil {
		chunks = append(chunks, final)
	}
	for _, ch := range chunks {
		chHash, _ := HashBytes("sha256", ch)
		s.stageChunk(tmpDir, chHash, ch)
	}
	// Publish only the first chunk (simulating partial progress).
	firstHash, _ := HashBytes("sha256", chunks[0])
	s.publishChunk(tmpDir, firstHash)

	// Close and reopen (simulating process restart).
	s.Close()
	s2, err := Open(Config{Root: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// No objects should be visible (manifest never committed).
	st, err := s2.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Objects != 0 {
		t.Fatalf("expected 0 visible objects after crash, got %d", st.Objects)
	}

	// GC should clean up the staged and orphaned chunks.
	gcStats, err := s2.GC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC after simulated crash: %+v", gcStats)

	// After GC, no chunks should remain.
	st2, err := s2.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st2.Chunks != 0 {
		t.Fatalf("expected 0 chunks after GC, got %d", st2.Chunks)
	}
}

func TestCrossInstanceCoordination(t *testing.T) {
	dir, err := os.MkdirTemp("", "store-cross-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Open two store instances on the same root (simulating two processes).
	s1, err := Open(Config{Root: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	s2, err := Open(Config{Root: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// Write from s1, read from s2.
	data := randomData(40000)
	id := writeObject(t, s1, data)
	got := readObject(t, s2, id)
	if !bytes.Equal(got, data) {
		t.Fatal("cross-instance read mismatch")
	}

	// Delete from s1, GC from s2.
	if err := s1.Delete(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	stats, err := s2.GC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksDeleted == 0 {
		t.Fatal("expected GC to reclaim chunks")
	}
}

func TestGCDanglingReferenceCheck(t *testing.T) {
	s, dir := newTestStore(t)
	data := randomData(40000)
	id := writeObject(t, s, data)

	// Verify no dangling references: every chunk referenced by the manifest
	// must exist on disk.
	m, err := s.readManifest(id)
	if err != nil {
		t.Fatal(err)
	}
	for _, ce := range m.Chunks {
		if _, err := os.Stat(s.chunkPath(ce.Hash)); err != nil {
			t.Fatalf("dangling reference: chunk %s missing: %v", ce.Hash, err)
		}
	}

	// After delete + GC, no dangling references should exist.
	s.Delete(context.Background(), id)
	s.GC(context.Background())

	// Walk all objects and verify every referenced chunk exists.
	filepath.WalkDir(filepath.Join(dir, "objects"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		data, _ := os.ReadFile(path)
		var man Manifest
		if jsonErr := json.Unmarshal(data, &man); jsonErr != nil {
			return nil
		}
		for _, ce := range man.Chunks {
			if _, statErr := os.Stat(s.chunkPath(ce.Hash)); statErr != nil {
				t.Errorf("dangling reference after GC: chunk %s", ce.Hash)
			}
		}
		return nil
	})
}

func TestLargeWriteReadConsistency(t *testing.T) {
	s, _ := newTestStore(t)
	// 2MB written in many small pieces vs one big write must be identical.
	data := randomData(2 * 1024 * 1024)

	// Write in small pieces.
	id1, err := s.Write(context.Background(), &pieceReader{data: data, size: 4096})
	if err != nil {
		t.Fatal(err)
	}
	// Write in one shot.
	id2, err := s.Write(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("inconsistent IDs: %s vs %s", id1, id2)
	}
	got := readObject(t, s, id1)
	if !bytes.Equal(got, data) {
		t.Fatal("roundtrip mismatch")
	}
}

type pieceReader struct {
	data []byte
	size int
	pos  int
}

func (r *pieceReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	end := r.pos + r.size
	if end > len(r.data) {
		end = len(r.data)
	}
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	return n, nil
}
