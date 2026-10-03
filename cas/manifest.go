package cas

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ManifestVersion 是清单编码格式的版本。
const ManifestVersion = 1

// ChunkRef 是清单中对一个块的引用。
type ChunkRef struct {
	Hash   Hash  `json:"hash"`
	Size   int64 `json:"size"`
	Offset int64 `json:"offset"`
}

// Manifest 描述一个已提交对象：分块参数、哈希算法与有序块列表。
// 对象标识 = 清单规范编码的摘要，因此同一内容、同一参数永远得到
// 同一标识；参数或算法变化只会产生新标识，不影响既有对象。
type Manifest struct {
	Version  int           `json:"version"`
	HashAlgo string        `json:"hashAlgo"`
	Chunker  ChunkerParams `json:"chunker"`
	Size     int64         `json:"size"`
	Chunks   []ChunkRef    `json:"chunks"`
}

// encode 返回清单的规范编码。结构体字段顺序固定、无 map，
// encoding/json 的输出是确定性的。
func (m *Manifest) encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// decodeManifest 解析清单编码并做基本合法性检查。
func decodeManifest(data []byte) (*Manifest, error) {
	var m Manifest
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	if m.Version != ManifestVersion {
		return nil, fmt.Errorf("unsupported manifest version %d", m.Version)
	}
	if !knownHashAlgo(m.HashAlgo) {
		return nil, fmt.Errorf("unknown hash algorithm %q", m.HashAlgo)
	}
	if err := m.Chunker.validate(); err != nil {
		return nil, err
	}
	var size int64
	for i, c := range m.Chunks {
		if c.Offset != size {
			return nil, fmt.Errorf("chunk %d: offset %d, want %d", i, c.Offset, size)
		}
		if c.Size <= 0 {
			return nil, fmt.Errorf("chunk %d: invalid size %d", i, c.Size)
		}
		if algoOf(c.Hash) != m.HashAlgo {
			return nil, fmt.Errorf("chunk %d: hash algorithm mismatch", i)
		}
		size += c.Size
	}
	if size != m.Size {
		return nil, fmt.Errorf("total size %d, want %d", size, m.Size)
	}
	return &m, nil
}

// id 由清单内容派生对象标识。
func (m *Manifest) id() ObjectID {
	data, err := m.encode()
	if err != nil {
		return ""
	}
	return ObjectID(hashBytes(m.HashAlgo, data))
}
