package store

import (
	"os"
	"syscall"
)

// fileLock wraps a flock-based advisory lock for cross-process coordination.
type fileLock struct {
	f *os.File
}

func newFileLock(path string) (*fileLock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	return &fileLock{f: f}, nil
}

// lockShared acquires a shared (read) lock. Multiple processes may hold
// shared locks simultaneously.
func (l *fileLock) lockShared() error {
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_SH)
}

// lockExclusive acquires an exclusive (write) lock, waiting for all shared
// locks to be released.
func (l *fileLock) lockExclusive() error {
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_EX)
}

func (l *fileLock) unlock() error {
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}

func (l *fileLock) close() error {
	return l.f.Close()
}
