package cas

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Options 是打开存储时的配置。分块参数与哈希算法只影响新写入；
// 已提交对象永远按其清单中记录的参数读取。
type Options struct {
	// HashAlgo 是新写入对象使用的哈希算法，默认 sha256。
	HashAlgo string
	// Chunker 是新写入对象使用的分块参数，默认 DefaultChunkerParams()。
	Chunker ChunkerParams
	// GCGracePeriod 是垃圾回收的宽限期：修改时间新于该时长的
	// 未引用块不会被回收，以免误删正在写入但尚未提交的块。
	// 0 表示无宽限期；负值表示使用默认值（1 分钟）。
	GCGracePeriod time.Duration
}

// ObjectInfo 是对象的元信息。
type ObjectInfo struct {
	ID      ObjectID
	Size    int64
	Chunks  int
	Created time.Time
}

// Store 是基于本地文件系统的内容寻址去重对象存储。
//
// 磁盘布局：
//
//	root/config.json        存储级默认配置
//	root/chunks/<algo>/<hex[:2]>/<hex>   块数据，按内容寻址
//	root/objects/<algo>/<hex[:2]>/<hex>  对象清单，按内容寻址
//	root/tmp/               写入中的临时文件（重启时清理）
//
// 所有提交都通过 “写临时文件 + fsync + rename” 完成，
// 读取方只会看到完整提交的版本。
type Store struct {
	root string
	opts Options
	lock *fileLock
}

// DefaultOptions 返回默认配置。
func DefaultOptions() Options {
	return Options{
		HashAlgo:      HashSHA256,
		Chunker:       DefaultChunkerParams(),
		GCGracePeriod: time.Minute,
	}
}

type storedConfig struct {
	HashAlgo      string        `json:"hashAlgo"`
	Chunker       ChunkerParams `json:"chunker"`
	GCGracePeriod time.Duration `json:"gcGracePeriod"`
}

// Open 打开（必要时创建）位于 root 的存储，并清理上次崩溃遗留的临时文件。
// opts 为 nil 时沿用存储已有配置，首次创建时使用默认值。
func Open(root string, opts *Options) (*Store, error) {
	for _, d := range []string{"chunks", "objects", "tmp"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	// 清理崩溃遗留的临时文件：它们从未被 rename 提交，不可见也不可引用。
	tmpDir := filepath.Join(root, "tmp")
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		os.RemoveAll(filepath.Join(tmpDir, e.Name()))
	}

	cfgPath := filepath.Join(root, "config.json")
	var eff Options
	if opts != nil {
		eff = *opts
	} else if data, err := os.ReadFile(cfgPath); err == nil {
		var sc storedConfig
		if err := json.Unmarshal(data, &sc); err != nil {
			return nil, fmt.Errorf("cas: read config: %w", err)
		}
		eff = Options{HashAlgo: sc.HashAlgo, Chunker: sc.Chunker, GCGracePeriod: sc.GCGracePeriod}
	} else {
		eff = DefaultOptions()
	}
	if eff.HashAlgo == "" {
		eff.HashAlgo = HashSHA256
	}
	if !knownHashAlgo(eff.HashAlgo) {
		return nil, fmt.Errorf("cas: unknown hash algorithm %q", eff.HashAlgo)
	}
	if eff.Chunker.Algorithm == "" {
		eff.Chunker = DefaultChunkerParams()
	}
	if err := eff.Chunker.validate(); err != nil {
		return nil, err
	}
	if eff.GCGracePeriod < 0 {
		eff.GCGracePeriod = time.Minute
	}
	// 持久化生效配置（仅影响新写入的默认值）。
	sc := storedConfig{HashAlgo: eff.HashAlgo, Chunker: eff.Chunker, GCGracePeriod: eff.GCGracePeriod}
	data, _ := json.MarshalIndent(sc, "", "  ")
	if err := writeFileAtomic(filepath.Join(root, "tmp"), cfgPath, data); err != nil {
		return nil, err
	}
	lk, err := openFileLock(root)
	if err != nil {
		return nil, err
	}
	return &Store{root: root, opts: eff, lock: lk}, nil
}

// Close 关闭存储。当前实现无后台资源，保留用于未来扩展。
func (s *Store) Close() error { return nil }

// Options 返回存储的生效配置副本。
func (s *Store) Options() Options { return s.opts }

// chunkPath 返回块哈希对应的存储路径。
func (s *Store) chunkPath(h Hash) string {
	hex := hexOf(h)
	return filepath.Join(s.root, "chunks", algoOf(h), hex[:2], hex)
}

// objectPath 返回对象标识对应的清单路径。
func (s *Store) objectPath(id ObjectID) string {
	hex := hexOf(Hash(id))
	return filepath.Join(s.root, "objects", algoOf(Hash(id)), hex[:2], hex)
}

// writeFileAtomic 把 data 原子地提交到 dst：先写 tmp 目录中的临时文件，
// fsync 后 rename，再 fsync 父目录。崩溃只可能留下 tmp 中的孤儿文件，
// 不会让 dst 出现半截内容。
func writeFileAtomic(tmpDir, dst string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(tmpDir, "w-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) // rename 后为空操作
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	syncDir(filepath.Dir(dst))
	return nil
}

func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	d.Sync()
	d.Close()
}

// writeChunkIfAbsent 把块写入存储；已存在相同内容寻址路径时跳过（去重）。
func (s *Store) writeChunkIfAbsent(h Hash, data []byte) error {
	p := s.chunkPath(h)
	if _, err := os.Stat(p); err == nil {
		return nil // 同内容块已存在，只保留一份
	}
	return writeFileAtomic(filepath.Join(s.root, "tmp"), p, data)
}

// Put 流式写入一个对象，返回其内容派生的标识。
// 同一内容重复写入只落一份；提交前对象对任何读取方不可见；
// 并发写入同一内容收敛到同一份结果。
func (s *Store) Put(ctx context.Context, r io.Reader) (ObjectID, error) {
	return s.putWith(ctx, r, s.opts.HashAlgo, s.opts.Chunker)
}

// PutBytes 是 Put 的便捷封装。
func (s *Store) PutBytes(ctx context.Context, b []byte) (ObjectID, error) {
	return s.Put(ctx, bytes.NewReader(b))
}

func (s *Store) putWith(ctx context.Context, r io.Reader, algo string, cp ChunkerParams) (ObjectID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// 整个写入持共享锁：写入之间互不阻塞（并发写同一内容收敛到同一结果），
	// GC 需持排他锁，因此回收绝不会与在途写入交错，结果确定。
	unlock := s.lock.rlock()
	defer unlock()
	m := &Manifest{Version: ManifestVersion, HashAlgo: algo, Chunker: cp}
	c := newChunker(r, cp)
	tmpDir := filepath.Join(s.root, "tmp")
	var off int64
	for {
		chunk, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		h := hashBytes(algo, chunk)
		if err := s.writeChunkIfAbsent(h, chunk); err != nil {
			return "", err
		}
		m.Chunks = append(m.Chunks, ChunkRef{Hash: h, Size: int64(len(chunk)), Offset: off})
		off += int64(len(chunk))
		if err := ctx.Err(); err != nil {
			return "", err
		}
	}
	m.Size = off
	data, err := m.encode()
	if err != nil {
		return "", err
	}
	id := m.id()
	if err := s.commitManifest(id, data, tmpDir); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) commitManifest(id ObjectID, data []byte, tmpDir string) error {
	p := s.objectPath(id)
	if _, err := os.Stat(p); err == nil {
		return nil // 同一内容已提交过
	}
	return writeFileAtomic(tmpDir, p, data)
}

// loadManifest 读取并解析对象清单，同时校验清单内容与其标识一致。
func (s *Store) loadManifest(id ObjectID) (*Manifest, error) {
	p := s.objectPath(id)
	data, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &ErrObjectNotFound{ID: id}
		}
		return nil, err
	}
	m, err := decodeManifest(data)
	if err != nil {
		return nil, &ErrManifestCorrupt{ObjectID: id, Reason: err.Error()}
	}
	if m.id() != id {
		return nil, &ErrManifestCorrupt{ObjectID: id, Reason: "manifest content does not match object id"}
	}
	return m, nil
}

// Get 读取整个对象，并对每个块做完整性校验。
func (s *Store) Get(ctx context.Context, id ObjectID) ([]byte, error) {
	m, err := s.loadManifest(id)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, m.Size)
	for i, ref := range m.Chunks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := s.readChunkVerified(id, i, ref)
		if err != nil {
			return nil, err
		}
		out = append(out, data...)
	}
	return out, nil
}

// readChunkVerified 读取一个块并校验其摘要与尺寸。
func (s *Store) readChunkVerified(id ObjectID, idx int, ref ChunkRef) ([]byte, error) {
	data, err := os.ReadFile(s.chunkPath(ref.Hash))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &ErrChunkMissing{ObjectID: id, Chunk: ref.Hash, Index: idx}
		}
		return nil, err
	}
	if int64(len(data)) != ref.Size {
		return nil, &ErrChunkCorrupt{ObjectID: id, Chunk: ref.Hash, Index: idx,
			Actual: Hash(fmt.Sprintf("size:%d", len(data)))}
	}
	actual := hashBytes(algoOf(ref.Hash), data)
	if actual != ref.Hash {
		return nil, &ErrChunkCorrupt{ObjectID: id, Chunk: ref.Hash, Index: idx, Actual: actual}
	}
	return data, nil
}

// Reader 以流式方式读取对象，逐块校验。
func (s *Store) Reader(ctx context.Context, id ObjectID) (io.ReadCloser, error) {
	m, err := s.loadManifest(id)
	if err != nil {
		return nil, err
	}
	return &objectReader{s: s, id: id, m: m}, nil
}

type objectReader struct {
	s   *Store
	id  ObjectID
	m   *Manifest
	idx int
	buf []byte
}

func (r *objectReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.idx >= len(r.m.Chunks) {
			return 0, io.EOF
		}
		data, err := r.s.readChunkVerified(r.id, r.idx, r.m.Chunks[r.idx])
		if err != nil {
			return 0, err
		}
		r.buf = data
		r.idx++
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *objectReader) Close() error { return nil }

// Stat 返回对象元信息。
func (s *Store) Stat(ctx context.Context, id ObjectID) (ObjectInfo, error) {
	m, err := s.loadManifest(id)
	if err != nil {
		return ObjectInfo{}, err
	}
	fi, err := os.Stat(s.objectPath(id))
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{ID: id, Size: m.Size, Chunks: len(m.Chunks), Created: fi.ModTime()}, nil
}

// Delete 删除对象清单；其引用的块由后续 GC 回收。
// 删除不存在的对象返回 ErrObjectNotFound。
func (s *Store) Delete(ctx context.Context, id ObjectID) error {
	p := s.objectPath(id)
	if _, err := os.Stat(p); errors.Is(err, fs.ErrNotExist) {
		return &ErrObjectNotFound{ID: id}
	}
	if err := os.Remove(p); err != nil {
		return err
	}
	syncDir(filepath.Dir(p))
	return nil
}

// List 列出全部已提交对象（按标识排序，结果确定）。
func (s *Store) List(ctx context.Context) ([]ObjectID, error) {
	var ids []ObjectID
	base := filepath.Join(s.root, "objects")
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		parts := filepath.SplitList(rel) // 不会分割，仅为占位
		_ = parts
		// rel 形如 <algo>/<hh>/<hex...>
		comp := splitPath(rel)
		if len(comp) != 3 {
			return nil
		}
		ids = append(ids, ObjectID(comp[0]+":"+comp[2]))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func splitPath(p string) []string {
	var out []string
	for {
		dir, file := filepath.Split(p)
		if file != "" {
			out = append([]string{file}, out...)
		}
		if dir == "" {
			break
		}
		p = dir[:len(dir)-1]
	}
	return out
}

// Verify 校验对象全部块的完整性，失败时返回可区分的错误类型
// （*ErrChunkMissing / *ErrChunkCorrupt / *ErrManifestCorrupt），
// 错误信息可定位到具体对象与块。
func (s *Store) Verify(ctx context.Context, id ObjectID) error {
	m, err := s.loadManifest(id)
	if err != nil {
		return err
	}
	for i, ref := range m.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := s.readChunkVerified(id, i, ref); err != nil {
			return err
		}
	}
	return nil
}
