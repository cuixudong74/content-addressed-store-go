package cas

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// failAfterReader 在流出 n 字节后报错，模拟写入中途崩溃。
type failAfterReader struct {
	r    io.Reader
	left int
}

func (f *failAfterReader) Read(p []byte) (int, error) {
	if f.left <= 0 {
		return 0, errors.New("simulated crash")
	}
	if len(p) > f.left {
		p = p[:f.left]
	}
	n, err := f.r.Read(p)
	f.left -= n
	return n, err
}

// 写入中途失败：对象不可见，重开后无可见产物、无悬空引用。
func TestCrashMidWrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	opts := DefaultOptions()
	opts.Chunker = ChunkerParams{Version: ChunkerVersion, Algorithm: AlgorithmGearCDC, MinSize: 64, AvgSize: 256, MaxSize: 1024}
	opts.GCGracePeriod = 0
	ctx := context.Background()

	s, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	data := randBytes(100 * 1024)
	_, err = s.Put(ctx, &failAfterReader{r: bytes.NewReader(data), left: 50 * 1024})
	if err == nil {
		t.Fatal("expected write failure")
	}
	// 未提交的对象不得有任何可见清单
	ids, _ := s.List(ctx)
	if len(ids) != 0 {
		t.Fatalf("uncommitted object visible: %v", ids)
	}
	s.Close()

	// 重新打开（模拟进程重启）
	s2, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	ids, _ = s2.List(ctx)
	if len(ids) != 0 {
		t.Fatalf("uncommitted object visible after reopen: %v", ids)
	}
	// 崩溃遗留的孤儿块由 GC 清理，不留垃圾
	stats, err := s2.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChunksRemoved == 0 {
		t.Fatal("expected orphan chunks to be reclaimed")
	}
	if n := countFiles(t, filepath.Join(dir, "chunks")); n != 0 {
		t.Fatalf("orphan chunks left: %d", n)
	}
}

// 已提交对象在“进程终止”后重新打开必须完整可读且可校验。
func TestCommittedSurvivesReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	opts := DefaultOptions()
	opts.Chunker = ChunkerParams{Version: ChunkerVersion, Algorithm: AlgorithmGearCDC, MinSize: 64, AvgSize: 256, MaxSize: 1024}
	ctx := context.Background()

	s, _ := Open(dir, &opts)
	data := randBytes(80 * 1024)
	id, err := s.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(data) {
		t.Fatal("size mismatch after reopen")
	}
	for i := range got {
		if got[i] != data[i] {
			t.Fatal("content mismatch after reopen")
		}
	}
	if err := s2.Verify(ctx, id); err != nil {
		t.Fatal(err)
	}
}

// tmp 目录中的残留临时文件在 Open 时被清理。
func TestTmpCleanedOnOpen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, _ := Open(dir, nil)
	s.Close()
	// 植入崩溃遗留的临时文件
	junk := filepath.Join(dir, "tmp", "w-stale")
	if err := os.WriteFile(junk, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	entries, _ := os.ReadDir(filepath.Join(dir, "tmp"))
	if len(entries) != 0 {
		t.Fatalf("stale tmp files not cleaned: %d", len(entries))
	}
}
