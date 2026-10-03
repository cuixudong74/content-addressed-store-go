package cas

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// 分块参数变化后：旧对象仍按创建时的语义可读，新写入使用新参数，
// 两者标识互不干扰。
func TestChunkerParamChangeKeepsOldReadable(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	small := ChunkerParams{Version: ChunkerVersion, Algorithm: AlgorithmGearCDC, MinSize: 64, AvgSize: 256, MaxSize: 1024}
	opts1 := DefaultOptions()
	opts1.Chunker = small
	s1, err := Open(dir, &opts1)
	if err != nil {
		t.Fatal(err)
	}
	data := randBytes(60 * 1024)
	id1, err := s1.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	info1, _ := s1.Stat(ctx, id1)
	s1.Close()

	// 用不同的分块参数重新打开
	large := ChunkerParams{Version: ChunkerVersion, Algorithm: AlgorithmGearCDC, MinSize: 1024, AvgSize: 4096, MaxSize: 16384}
	opts2 := DefaultOptions()
	opts2.Chunker = large
	s2, err := Open(dir, &opts2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// 旧对象按创建时的语义读取，内容不变
	got, err := s2.Get(ctx, id1)
	if err != nil {
		t.Fatalf("old object unreadable after param change: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("old object content changed after param change")
	}
	// 旧清单仍记录旧参数
	m, err := s2.loadManifest(id1)
	if err != nil {
		t.Fatal(err)
	}
	if m.Chunker.AvgSize != small.AvgSize {
		t.Fatalf("old manifest params changed: %+v", m.Chunker)
	}

	// 同一内容用新参数写入得到不同的标识与不同的分块
	id2, err := s2.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if id2 == id1 {
		t.Fatal("param change should produce new identity")
	}
	info2, _ := s2.Stat(ctx, id2)
	if info2.Chunks >= info1.Chunks {
		t.Fatalf("larger chunks should produce fewer blocks: %d vs %d", info2.Chunks, info1.Chunks)
	}
	// 两个版本都可读
	got2, err := s2.Get(ctx, id2)
	if err != nil || !bytes.Equal(got2, data) {
		t.Fatalf("new object broken: %v", err)
	}
	if err := s2.Verify(ctx, id1); err != nil {
		t.Fatal(err)
	}
}

// 哈希算法变化后：旧对象仍按原算法校验可读。
func TestHashAlgoChangeKeepsOldReadable(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	opts1 := DefaultOptions()
	opts1.Chunker = ChunkerParams{Version: ChunkerVersion, Algorithm: AlgorithmGearCDC, MinSize: 64, AvgSize: 256, MaxSize: 1024}
	s1, _ := Open(dir, &opts1)
	data := randBytes(10 * 1024)
	id1, err := s1.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(id1), "sha256:") {
		t.Fatalf("unexpected id scheme: %s", id1)
	}
	s1.Close()

	opts2 := opts1
	opts2.HashAlgo = HashSHA512
	s2, err := Open(dir, &opts2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// 旧对象（sha256）仍可读可校验
	if err := s2.Verify(ctx, id1); err != nil {
		t.Fatalf("sha256 object unreadable under sha512 default: %v", err)
	}
	// 新对象用 sha512
	id2, err := s2.PutBytes(ctx, randBytes(5000))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(id2), "sha512:") {
		t.Fatalf("new writes should use sha512: %s", id2)
	}
	// GC 跨算法正确保留两种块
	if _, err := s2.GC(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s2.Verify(ctx, id1); err != nil {
		t.Fatal(err)
	}
	if err := s2.Verify(ctx, id2); err != nil {
		t.Fatal(err)
	}
}

// 未知算法/版本的清单一律拒绝，不得静默误读。
func TestRejectUnknownVersions(t *testing.T) {
	s := testStore(t, nil)
	bad := []byte(`{"version":999,"hashAlgo":"sha256","chunker":{"version":1,"algorithm":"gear-cdc","minSize":64,"avgSize":256,"maxSize":1024},"size":0,"chunks":[]}`)
	if _, err := decodeManifest(bad); err == nil {
		t.Fatal("should reject unknown manifest version")
	}
	bad2 := []byte(`{"version":1,"hashAlgo":"md5","chunker":{"version":1,"algorithm":"gear-cdc","minSize":64,"avgSize":256,"maxSize":1024},"size":0,"chunks":[]}`)
	if _, err := decodeManifest(bad2); err == nil {
		t.Fatal("should reject unknown hash algorithm")
	}
	_ = s
}
