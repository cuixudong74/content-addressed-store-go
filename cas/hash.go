package cas

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"hash"
	"strings"
)

// 哈希算法标识，带版本语义：算法集合一旦发布只增不改。
// 已提交对象的清单中记录了其使用的算法，永远按当时的算法读取。
const (
	HashSHA256 = "sha256"
	HashSHA512 = "sha512"
)

// Hash 是一次内容摘要的字符串形式，形如 "sha256:abcd..."。
type Hash string

// ObjectID 是对象（清单）的标识，由清单内容派生。
type ObjectID string

// newHasher 按算法名构造哈希器；未知算法返回 nil。
func newHasher(algo string) hash.Hash {
	switch algo {
	case HashSHA256:
		return sha256.New()
	case HashSHA512:
		return sha512.New()
	default:
		return nil
	}
}

// knownHashAlgo 报告算法名是否受支持。
func knownHashAlgo(algo string) bool {
	return newHasher(algo) != nil
}

// hashBytes 计算 data 的摘要并返回带算法前缀的 Hash。
func hashBytes(algo string, data []byte) Hash {
	h := newHasher(algo)
	if h == nil {
		return ""
	}
	h.Write(data)
	return Hash(algo + ":" + hex.EncodeToString(h.Sum(nil)))
}

// algoOf 返回 Hash 的算法前缀部分。
func algoOf(h Hash) string {
	if i := strings.IndexByte(string(h), ':'); i >= 0 {
		return string(h[:i])
	}
	return ""
}

// hexOf 返回 Hash 的十六进制部分。
func hexOf(h Hash) string {
	if i := strings.IndexByte(string(h), ':'); i >= 0 {
		return string(h[i+1:])
	}
	return string(h)
}
