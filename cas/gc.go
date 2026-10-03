package cas

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// GCStats 是一次垃圾回收的统计。
type GCStats struct {
	ChunksScanned  int
	ChunksRemoved  int
	BytesReclaimed int64
	ObjectsKept    int
}

// GC 回收无任何已提交对象引用的块。
//
// 确定性：GC 持排他锁，等待所有在途写入提交后才开始标记，
// 因此与并发写入、删除、读取同时发生时结果唯一确定——
// 已提交对象引用的块绝不会被删除。
//
// 可重入：回收在任意点被打断（含进程崩溃）后，再次调用即可继续；
// 删除未引用块是幂等操作，不产生悬空引用。
//
// 宽限期：修改时间新于 GCGracePeriod 的未引用块会被保留，
// 为不走锁的异常写入者提供额外保护（默认 1 分钟，可配置）。
func (s *Store) GC(ctx context.Context) (GCStats, error) {
	unlock := s.lock.lock()
	defer unlock()

	var stats GCStats

	// 标记阶段：收集所有已提交对象引用的块。
	// 任一清单无法解析时中止回收（保守策略，宁可不回收也不错删）。
	live := make(map[Hash]bool)
	ids, err := s.List(ctx)
	if err != nil {
		return stats, err
	}
	stats.ObjectsKept = len(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		m, err := s.loadManifest(id)
		if err != nil {
			return stats, fmt.Errorf("cas: gc aborted: %w", err)
		}
		for _, c := range m.Chunks {
			live[c.Hash] = true
		}
	}

	// 清扫阶段：删除未被引用且越过宽限期的块。
	cutoff := time.Now().Add(-s.opts.GCGracePeriod)
	base := filepath.Join(s.root, "chunks")
	err = filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		stats.ChunksScanned++
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		comp := splitPath(rel)
		if len(comp) != 3 {
			return nil
		}
		h := Hash(comp[0] + ":" + comp[2])
		if live[h] {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil // 已被并发清理，忽略
		}
		if fi.ModTime().After(cutoff) {
			return nil // 宽限期内，保守保留
		}
		if err := os.Remove(path); err != nil {
			return nil // 已被并发清理，忽略
		}
		stats.ChunksRemoved++
		stats.BytesReclaimed += fi.Size()
		return nil
	})
	if err != nil {
		return stats, err
	}
	// 清理空目录（尽力而为）。
	pruneEmptyDirs(base)
	return stats, nil
}

func pruneEmptyDirs(base string) {
	filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == base {
			return nil
		}
		os.Remove(path) // 仅当目录为空时成功
		return nil
	})
}
