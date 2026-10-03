package cas

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// 同一对象的并发写入必须收敛到同一标识，且内容完整可读。
func TestConcurrentSameObjectWrites(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	data := randBytes(300 * 1024)

	const n = 16
	ids := make([]ObjectID, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := s.Put(ctx, &fixedFeedReader{r: bytes.NewReader(data), n: 1 + i*777})
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("concurrent writes diverged: %s vs %s", ids[0], ids[i])
		}
	}
	got, err := s.Get(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("content mismatch after concurrent writes")
	}
}

// 读写并发：读到的要么是完整版本要么不存在；慢写入不阻塞无关读写。
func TestConcurrentReadWrite(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()

	objects := make([][]byte, 8)
	ids := make([]ObjectID, 8)
	for i := range objects {
		objects[i] = randBytes(20 * 1024)
		id, err := s.PutBytes(ctx, objects[i])
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	// 一个故意很慢的写入者
	slowData := randBytes(200 * 1024)
	slow := &slowReader{r: bytes.NewReader(slowData), delay: time.Millisecond}

	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := s.Put(ctx, slow); err != nil {
			errCh <- fmt.Errorf("slow put: %w", err)
		}
	}()

	// 慢写入进行中，无关对象的读写应迅速完成
	start := time.Now()
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				got, err := s.Get(ctx, ids[i])
				if err != nil {
					errCh <- err
					return
				}
				if !bytes.Equal(got, objects[i]) {
					errCh <- fmt.Errorf("object %d corrupt during concurrency", i)
					return
				}
				// 并发写入新对象
				if _, err := s.PutBytes(ctx, randBytes(4096)); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("unrelated I/O blocked by slow writer: %v", elapsed)
	}
}

type slowReader struct {
	r     io.Reader
	delay time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	if len(p) > 512 {
		p = p[:512]
	}
	return s.r.Read(p)
}

// 并发读写 + 删除 + GC 同时发生：已提交对象始终可读，结果确定。
func TestConcurrentGCWithIO(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()

	// 常驻对象：整个测试期间必须可读
	pinned := randBytes(50 * 1024)
	pinID, err := s.PutBytes(ctx, pinned)
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, 128)

	// 写入者：不断写入临时对象，部分删除
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				id, err := s.PutBytes(ctx, randBytes(8*1024))
				if err != nil {
					errCh <- err
					return
				}
				if j%2 == 0 {
					s.Delete(ctx, id)
				}
			}
		}(i)
	}
	// 读者：反复读取常驻对象
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			got, err := s.Get(ctx, pinID)
			if err != nil {
				errCh <- fmt.Errorf("read pinned during GC: %w", err)
				return
			}
			if !bytes.Equal(got, pinned) {
				errCh <- fmt.Errorf("pinned object corrupt during GC")
				return
			}
		}
	}()
	// 回收者：反复 GC
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := s.GC(ctx); err != nil {
				errCh <- fmt.Errorf("gc: %w", err)
				return
			}
		}
	}()

	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	// 最终状态：常驻对象完整可校验
	if err := s.Verify(ctx, pinID); err != nil {
		t.Fatalf("pinned object failed verify after concurrent GC: %v", err)
	}
}
