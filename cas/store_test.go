package cas

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func testStore(t *testing.T, mutate func(*Options)) *Store {
	t.Helper()
	opts := DefaultOptions()
	opts.Chunker = ChunkerParams{Version: ChunkerVersion, Algorithm: AlgorithmGearCDC, MinSize: 64, AvgSize: 256, MaxSize: 1024}
	opts.GCGracePeriod = 0
	if mutate != nil {
		mutate(&opts)
	}
	s, err := Open(filepath.Join(t.TempDir(), "store"), &opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func TestPutGetRoundtrip(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	for _, n := range []int{0, 1, 63, 64, 100, 1024, 100 * 1024} {
		data := randBytes(n)
		id, err := s.PutBytes(ctx, data)
		if err != nil {
			t.Fatalf("put %d bytes: %v", n, err)
		}
		got, err := s.Get(ctx, id)
		if err != nil {
			t.Fatalf("get %d bytes: %v", n, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("roundtrip mismatch at %d bytes", n)
		}
		if err := s.Verify(ctx, id); err != nil {
			t.Fatalf("verify %d bytes: %v", n, err)
		}
	}
}

func TestPutDedupSameContent(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	data := randBytes(50 * 1024)
	id1, err := s.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := s.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("same content got different ids: %s vs %s", id1, id2)
	}
	// 重复写入不应新增块
	n1 := countFiles(t, filepath.Join(s.root, "chunks"))
	if _, err := s.PutBytes(ctx, data); err != nil {
		t.Fatal(err)
	}
	n2 := countFiles(t, filepath.Join(s.root, "chunks"))
	if n1 != n2 {
		t.Fatalf("duplicate put created chunks: %d -> %d", n1, n2)
	}
}

func countFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			n++
		}
		return nil
	})
	return n
}

func TestBlockLevelDedup(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	shared := randBytes(30 * 1024)
	a := append(randBytes(5000), shared...)
	b := append(randBytes(8000), shared...)
	if _, err := s.PutBytes(ctx, a); err != nil {
		t.Fatal(err)
	}
	n1 := countFiles(t, filepath.Join(s.root, "chunks"))
	if _, err := s.PutBytes(ctx, b); err != nil {
		t.Fatal(err)
	}
	n2 := countFiles(t, filepath.Join(s.root, "chunks"))
	// b 相对 a 只有 8KB 唯一内容：新增块数应接近 8KB/avg，
	// 远小于 b 全量 38KB 对应的块数（约 150）
	if n2-n1 > 45 {
		t.Fatalf("block dedup ineffective: %d new chunks for mostly-shared object", n2-n1)
	}
	t.Logf("new chunks for shared-content object: %d", n2-n1)
}

func TestWriteSizeIndependence(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	data := randBytes(200 * 1024)
	id1, err := s.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := s.Put(ctx, &fixedFeedReader{r: bytes.NewReader(data), n: 7})
	if err != nil {
		t.Fatal(err)
	}
	id3, err := s.Put(ctx, &fixedFeedReader{r: bytes.NewReader(data), n: 99991})
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 || id1 != id3 {
		t.Fatalf("write size changed identity: %s %s %s", id1, id2, id3)
	}
}

func TestGetNotFound(t *testing.T) {
	s := testStore(t, nil)
	_, err := s.Get(context.Background(), ObjectID("sha256:0000000000000000000000000000000000000000000000000000000000000000"))
	var nf *ErrObjectNotFound
	if !errors.As(err, &nf) {
		t.Fatalf("want ErrObjectNotFound, got %v", err)
	}
}

func TestDeleteAndList(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	id1, _ := s.PutBytes(ctx, randBytes(1000))
	id2, _ := s.PutBytes(ctx, randBytes(2000))
	ids, err := s.List(ctx)
	if err != nil || len(ids) != 2 {
		t.Fatalf("list: %v %v", ids, err)
	}
	if err := s.Delete(ctx, id1); err != nil {
		t.Fatal(err)
	}
	ids, _ = s.List(ctx)
	if len(ids) != 1 || ids[0] != id2 {
		t.Fatalf("after delete: %v", ids)
	}
	var nf *ErrObjectNotFound
	if err := s.Delete(ctx, id1); !errors.As(err, &nf) {
		t.Fatalf("delete twice: %v", err)
	}
}

func TestStat(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	data := randBytes(10 * 1024)
	id, _ := s.PutBytes(ctx, data)
	info, err := s.Stat(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(data)) || info.Chunks == 0 {
		t.Fatalf("bad stat: %+v", info)
	}
}

func TestStreamingReader(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	data := randBytes(100 * 1024)
	id, _ := s.PutBytes(ctx, data)
	r, err := s.Reader(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got bytes.Buffer
	buf := make([]byte, 13) // 故意用不对齐的读大小
	for {
		n, err := r.Read(buf)
		got.Write(buf[:n])
		if err != nil {
			break
		}
	}
	if !bytes.Equal(got.Bytes(), data) {
		t.Fatal("streaming read mismatch")
	}
}
