package store

// ManifestVersion is the current manifest format version.
const ManifestVersion = 1

// ChunkingAlgorithmCDC identifies the content-defined chunking algorithm.
const ChunkingAlgorithmCDC = "cdc-gear-v1"

// ChunkingParams records the chunking parameters used to produce a manifest.
// These are stored per-object so old objects remain readable even if the
// store defaults change.
type ChunkingParams struct {
	MinSize int `json:"min_size"`
	AvgSize int `json:"avg_size"`
	MaxSize int `json:"max_size"`
}

// ChunkEntry describes one chunk of an object.
type ChunkEntry struct {
	Hash Hash  `json:"hash"`
	Size int64 `json:"size"`
}

// Manifest is the per-object index of chunks. It is stored at the path
// derived from the object's content hash.
type Manifest struct {
	Version           int            `json:"version"`
	HashAlgorithm     string         `json:"hash_algorithm"`
	ChunkingAlgorithm string         `json:"chunking_algorithm"`
	ChunkingParams    ChunkingParams `json:"chunking_params"`
	TotalSize         int64          `json:"total_size"`
	Chunks            []ChunkEntry   `json:"chunks"`
}
