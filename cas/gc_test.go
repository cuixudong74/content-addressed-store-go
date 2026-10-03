package cas

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// 删除对象后 GC 回收其全部块。
func TestGCReclaimsUnreferenced(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	id, _ := s.PutBytes(ctx, randBytes(50*1024))
	before := countFiles(t, filepath.Join(s.root, "chunks"))
	if before == 0 {
		t.Fatal("expected chunks")
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	stats, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksRemoved != before {
		t.Fatalf("removed %d, want %d", stats.ChunksRemoved, before)
	}
	if got := countFiles(t, filepath.Join(s.root, "chunks")); got != 0 {
		t.Fatalf("chunks left after GC: %d", got)
	}
}

// 共享块：删除一个对象后，仍被另一对象引用的块不得回收。
func TestGCKeepSharedChunks(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	shared := randBytes(30 * 1024)
	a := append(randBytes(3000), shared...)
	b := append(randBytes(4000), shared...)
	idA, _ := s.PutBytes(ctx, a)
	idB, _ := s.PutBytes(ctx, b)

	if err := s.Delete(ctx, idA); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GC(ctx); err != nil {
		t.Fatal(err)
	}
	// b 必须完整可读
	if err := s.Verify(ctx, idB); err != nil {
		t.Fatalf("shared chunks wrongly collected: %v", err)
	}
	got, err := s.Get(ctx, idB)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(b) {
		t.Fatal("size mismatch")
	}
	for i := range got {
		if got[i] != b[i] {
			t.Fatal("content mismatch")
		}
	}
}

// GC 被中断（context 取消）后可安全重入并继续完成。
func TestGCReentrant(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	var ids []ObjectID
	for i := 0; i < 5; i++ {
		id, _ := s.PutBytes(ctx, randBytes(20*1024))
		ids = append(ids, id)
	}
	// 删除一半
	for _, id := range ids[:3] {
		s.Delete(ctx, id)
	}
	// 用已取消的 context 模拟中断：可能部分完成或直接返回
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	s.GC(canceled) // 允许返回错误，关键是不能破坏状态
	// 重新执行必须完成回收
	stats, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksRemoved == 0 {
		t.Fatal("reentrant GC made no progress")
	}
	// 存活对象完整
	for _, id := range ids[3:] {
		if err := s.Verify(ctx, id); err != nil {
			t.Fatalf("live object damaged by GC: %v", err)
		}
	}
}

// 崩溃遗留的孤儿块（无清单引用）被 GC 回收。
func TestGCOrphanChunks(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	// 手工制造孤儿块
	orphan := randBytes(1000)
	h := hashBytes(s.opts.HashAlgo, orphan)
	if err := s.writeChunkIfAbsent(h, orphan); err != nil {
		t.Fatal(err)
	}
	stats, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksRemoved != 1 {
		t.Fatalf("want 1 orphan removed, got %d", stats.ChunksRemoved)
	}
}

// 宽限期保护新写入的未引用块。
func TestGCGracePeriod(t *testing.T) {
	s := testStore(t, func(o *Options) { o.GCGracePeriod = time.Hour })
	ctx := context.Background()
	orphan := randBytes(1000)
	h := hashBytes(s.opts.HashAlgo, orphan)
	s.writeChunkIfAbsent(h, orphan)
	stats, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksRemoved != 0 {
		t.Fatalf("grace period violated: removed %d", stats.ChunksRemoved)
	}
}
