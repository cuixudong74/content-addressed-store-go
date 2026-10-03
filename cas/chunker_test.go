package cas

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func testParams() ChunkerParams {
	return ChunkerParams{Version: ChunkerVersion, Algorithm: AlgorithmGearCDC, MinSize: 64, AvgSize: 256, MaxSize: 1024}
}

func splitAll(t *testing.T, data []byte, feedSize int) [][]byte {
	t.Helper()
	p := testParams()
	var r io.Reader = bytes.NewReader(data)
	if feedSize > 0 {
		r = &fixedFeedReader{r: r, n: feedSize}
	}
	c := newChunker(r, p)
	var chunks [][]byte
	for {
		b, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, b)
	}
	return chunks
}

// fixedFeedReader 模拟调用方每次只喂 n 字节。
type fixedFeedReader struct {
	r io.Reader
	n int
}

func (f *fixedFeedReader) Read(p []byte) (int, error) {
	if len(p) > f.n {
		p = p[:f.n]
	}
	return f.r.Read(p)
}

func TestChunkerReassembles(t *testing.T) {
	data := make([]byte, 100*1024)
	rand.Read(data)
	chunks := splitAll(t, data, 0)
	var got []byte
	for _, c := range chunks {
		got = append(got, c...)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("reassembled data mismatch")
	}
	for i, c := range chunks {
		if len(c) > testParams().MaxSize {
			t.Fatalf("chunk %d exceeds max: %d", i, len(c))
		}
		if i < len(chunks)-1 && len(c) < testParams().MinSize {
			t.Fatalf("chunk %d below min: %d", i, len(c))
		}
	}
}

func TestChunkerFeedSizeIndependent(t *testing.T) {
	data := make([]byte, 200*1024)
	rand.Read(data)
	base := splitAll(t, data, 0)
	for _, feed := range []int{1, 7, 100, 4096, 65536} {
		got := splitAll(t, data, feed)
		if len(got) != len(base) {
			t.Fatalf("feed=%d: chunk count %d != %d", feed, len(got), len(base))
		}
		for i := range base {
			if !bytes.Equal(got[i], base[i]) {
				t.Fatalf("feed=%d: chunk %d differs", feed, i)
			}
		}
	}
}

func TestChunkerInsertShift(t *testing.T) {
	data := make([]byte, 300*1024)
	rand.Read(data)
	// 在中间插入 100 字节
	pos := 150 * 1024
	mut := append([]byte{}, data[:pos]...)
	mut = append(mut, make([]byte, 100)...)
	mut = append(mut, data[pos:]...)

	a := splitAll(t, data, 0)
	b := splitAll(t, mut, 0)
	setA := map[[32]byte]bool{}
	key := func(c []byte) [32]byte {
		var k [32]byte
		h := newHasher(HashSHA256)
		h.Write(c)
		copy(k[:], h.Sum(nil))
		return k
	}
	for _, c := range a {
		setA[key(c)] = true
	}
	shared := 0
	for _, c := range b {
		if setA[key(c)] {
			shared++
		}
	}
	// 插入 100 字节不应让绝大部分块失效
	if shared < len(b)*8/10 {
		t.Fatalf("too few shared chunks after insert: %d/%d", shared, len(b))
	}
	t.Logf("shared %d/%d chunks after 100-byte insert", shared, len(b))
}
