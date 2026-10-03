package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
)

// Hash is a content identifier with an algorithm prefix, e.g. "sha256:<hex>".
// The prefix allows future algorithm changes without breaking existing objects.
type Hash string

const (
	AlgoSHA256 = "sha256"
)

// hashAlgorithm registry. Add new algorithms here; old objects keep using
// the algorithm recorded in their manifest.
var hashAlgorithms = map[string]func() hash.Hash{
	AlgoSHA256: sha256.New,
}

// NewHasher returns a hash.Hash for the named algorithm.
func NewHasher(algo string) (hash.Hash, error) {
	fn, ok := hashAlgorithms[algo]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedAlgorithm, algo)
	}
	return fn(), nil
}

// HashBytes computes the content hash of data using the named algorithm.
func HashBytes(algo string, data []byte) (Hash, error) {
	h, err := NewHasher(algo)
	if err != nil {
		return "", err
	}
	h.Write(data)
	return Hash(algo + ":" + hex.EncodeToString(h.Sum(nil))), nil
}

// Algorithm returns the algorithm prefix of the hash.
func (h Hash) Algorithm() string {
	for i := 0; i < len(h); i++ {
		if h[i] == ':' {
			return string(h[:i])
		}
	}
	return ""
}

// Digest returns the hex digest portion of the hash.
func (h Hash) Digest() string {
	for i := 0; i < len(h); i++ {
		if h[i] == ':' {
			return string(h[i+1:])
		}
	}
	return string(h)
}

// Valid reports whether the hash has a known algorithm and a well-formed digest.
func (h Hash) Valid() bool {
	algo := h.Algorithm()
	if _, ok := hashAlgorithms[algo]; !ok {
		return false
	}
	d := h.Digest()
	if len(d) != 64 {
		return false
	}
	_, err := hex.DecodeString(d)
	return err == nil
}
