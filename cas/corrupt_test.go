package cas

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

// 篡改一个块的内容，期望 Get/Verify 报 ErrChunkCorrupt 且能定位对象与块。
func TestDetectChunkCorruption(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	id, err := s.PutBytes(ctx, randBytes(20*1024))
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.loadManifest(id)
	if err != nil {
		t.Fatal(err)
	}
	victim := m.Chunks[1]
	p := s.chunkPath(victim.Hash)
	data, _ := os.ReadFile(p)
	data[0] ^= 0xFF
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = s.Get(ctx, id)
	var corrupt *ErrChunkCorrupt
	if !errors.As(err, &corrupt) {
		t.Fatalf("want ErrChunkCorrupt, got %v", err)
	}
	if corrupt.ObjectID != id || corrupt.Chunk != victim.Hash || corrupt.Index != 1 {
		t.Fatalf("error does not locate object/chunk: %+v", corrupt)
	}
	if err := s.Verify(ctx, id); !errors.As(err, &corrupt) {
		t.Fatalf("verify: want ErrChunkCorrupt, got %v", err)
	}
}

// 删除一个块，期望报 ErrChunkMissing（悬空引用可被发现）。
func TestDetectChunkMissing(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	id, _ := s.PutBytes(ctx, randBytes(20*1024))
	m, _ := s.loadManifest(id)
	victim := m.Chunks[0]
	os.Remove(s.chunkPath(victim.Hash))

	_, err := s.Get(ctx, id)
	var missing *ErrChunkMissing
	if !errors.As(err, &missing) {
		t.Fatalf("want ErrChunkMissing, got %v", err)
	}
	if missing.ObjectID != id || missing.Chunk != victim.Hash {
		t.Fatalf("error does not locate object/chunk: %+v", missing)
	}
}

// 篡改清单内容，期望报 ErrManifestCorrupt。
func TestDetectManifestCorruption(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	id, _ := s.PutBytes(ctx, randBytes(5000))
	p := s.objectPath(id)
	data, _ := os.ReadFile(p)
	// 破坏 JSON 结构
	if err := os.WriteFile(p, data[:len(data)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := s.Get(ctx, id)
	var mc *ErrManifestCorrupt
	if !errors.As(err, &mc) {
		t.Fatalf("want ErrManifestCorrupt, got %v", err)
	}
	if !strings.Contains(mc.Error(), string(id)[:20]) {
		t.Fatalf("error should mention object id: %v", mc)
	}
}

// 清单被替换为另一对象的清单（标识与内容不符）。
func TestDetectManifestIDMismatch(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	id1, _ := s.PutBytes(ctx, randBytes(5000))
	id2, _ := s.PutBytes(ctx, randBytes(6000))
	data2, _ := os.ReadFile(s.objectPath(id2))
	os.WriteFile(s.objectPath(id1), data2, 0o644)

	_, err := s.Get(ctx, id1)
	var mc *ErrManifestCorrupt
	if !errors.As(err, &mc) {
		t.Fatalf("want ErrManifestCorrupt, got %v", err)
	}
}

// 块被截断（尺寸不符）也要报损坏而非静默返回错误数据。
func TestDetectChunkTruncation(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	id, _ := s.PutBytes(ctx, randBytes(5000))
	m, _ := s.loadManifest(id)
	victim := m.Chunks[0]
	p := s.chunkPath(victim.Hash)
	data, _ := os.ReadFile(p)
	os.WriteFile(p, data[:len(data)-1], 0o644)

	_, err := s.Get(ctx, id)
	var corrupt *ErrChunkCorrupt
	if !errors.As(err, &corrupt) {
		t.Fatalf("want ErrChunkCorrupt, got %v", err)
	}
}
