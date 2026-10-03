package cas

import (
	"os"
	"path/filepath"
	"syscall"
)

// fileLock 是基于 flock 的进程间/进程内读写锁：
// 写入路径持共享锁（彼此不互斥，慢写入不阻塞无关写入），
// GC 持排他锁（等待所有在途写入提交后再回收，结果确定）。
// 每次加锁打开独立 fd，避免同一 fd 上的锁状态互相干扰。
type fileLock struct {
	path string
}

func openFileLock(root string) (*fileLock, error) {
	p := filepath.Join(root, "store.lock")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	f.Close()
	return &fileLock{path: p}, nil
}

// acquire 打开独立 fd 并加锁，返回释放函数。
func (l *fileLock) acquire(op int) func() {
	f, err := os.OpenFile(l.path, os.O_RDWR, 0o644)
	if err != nil {
		return func() {}
	}
	if err := syscall.Flock(int(f.Fd()), op); err != nil {
		f.Close()
		return func() {}
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}
}

// rlock 获取共享锁。
func (l *fileLock) rlock() func() { return l.acquire(syscall.LOCK_SH) }

// lock 获取排他锁。
func (l *fileLock) lock() func() { return l.acquire(syscall.LOCK_EX) }
