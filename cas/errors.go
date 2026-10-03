package cas

import "fmt"

// ErrObjectNotFound 表示对象清单不存在。
type ErrObjectNotFound struct {
	ID ObjectID
}

func (e *ErrObjectNotFound) Error() string {
	return fmt.Sprintf("cas: object not found: %s", e.ID)
}

// ErrChunkMissing 表示清单引用的块在存储中缺失（悬空引用）。
type ErrChunkMissing struct {
	ObjectID ObjectID
	Chunk    Hash
	Index    int
}

func (e *ErrChunkMissing) Error() string {
	return fmt.Sprintf("cas: object %s: chunk %d (%s) missing", e.ObjectID, e.Index, e.Chunk)
}

// ErrChunkCorrupt 表示块内容与其摘要不一致。
type ErrChunkCorrupt struct {
	ObjectID ObjectID
	Chunk    Hash
	Index    int
	Actual   Hash
}

func (e *ErrChunkCorrupt) Error() string {
	return fmt.Sprintf("cas: object %s: chunk %d corrupt: want %s, got %s",
		e.ObjectID, e.Index, e.Chunk, e.Actual)
}

// ErrManifestCorrupt 表示对象清单本身无法解析或与对象标识不符。
type ErrManifestCorrupt struct {
	ObjectID ObjectID
	Reason   string
}

func (e *ErrManifestCorrupt) Error() string {
	return fmt.Sprintf("cas: object %s: manifest corrupt: %s", e.ObjectID, e.Reason)
}
