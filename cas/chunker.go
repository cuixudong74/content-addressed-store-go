package cas

import (
	"errors"
	"fmt"
	"io"
	"math/bits"
)

// ChunkerVersion 标识分块算法版本。参数或算法变化时必须递增，
// 已提交对象仍按清单中记录的版本读取。
const ChunkerVersion = 1

// AlgorithmGearCDC 是基于 Gear 滚动哈希的内容定义分块（FastCDC 风格归一化）。
const AlgorithmGearCDC = "gear-cdc"

// ChunkerParams 描述一次写入使用的分块参数，会被完整记录进对象清单。
type ChunkerParams struct {
	Version   int    `json:"version"`
	Algorithm string `json:"algorithm"`
	MinSize   int    `json:"minSize"`
	AvgSize   int    `json:"avgSize"`
	MaxSize   int    `json:"maxSize"`
}

// DefaultChunkerParams 返回当前版本的默认分块参数。
func DefaultChunkerParams() ChunkerParams {
	return ChunkerParams{
		Version:   ChunkerVersion,
		Algorithm: AlgorithmGearCDC,
		MinSize:   16 * 1024,
		AvgSize:   64 * 1024,
		MaxSize:   256 * 1024,
	}
}

// validate 检查参数是否合法。
func (p ChunkerParams) validate() error {
	if p.Version != ChunkerVersion {
		return fmt.Errorf("cas: unsupported chunker version %d", p.Version)
	}
	if p.Algorithm != AlgorithmGearCDC {
		return fmt.Errorf("cas: unsupported chunker algorithm %q", p.Algorithm)
	}
	if p.MinSize <= 0 || p.AvgSize <= p.MinSize || p.MaxSize <= p.AvgSize {
		return fmt.Errorf("cas: invalid chunk sizes min=%d avg=%d max=%d", p.MinSize, p.AvgSize, p.MaxSize)
	}
	if p.AvgSize&(p.AvgSize-1) != 0 {
		return fmt.Errorf("cas: avgSize must be a power of two, got %d", p.AvgSize)
	}
	return nil
}

// gearTable 是确定性生成的 256 项随机表（splitmix64，固定种子），
// 表内容属于算法版本的一部分，版本不变表不变。
var gearTable = func() [256]uint64 {
	var t [256]uint64
	state := uint64(0x9E3779B97F4A7C15)
	for i := range t {
		state += 0x9E3779B97F4A7C15
		z := state
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		t[i] = z ^ (z >> 31)
	}
	return t
}()

// chunker 是流式内容定义分块器：切分结果只取决于字节流本身，
// 与调用方每次喂入的字节数无关，因此同一内容无论写入方式如何
// 都得到一致的块序列。
type chunker struct {
	r       io.Reader
	params  ChunkerParams
	maskS   uint64 // 平均长度之前使用的严格掩码
	maskL   uint64 // 平均长度之后使用的宽松掩码
	buf     []byte // 预读窗口
	off     int    // buf 中未消费起点
	eof     bool
	readErr error // 底层读错误，延迟到数据消费完后返回
}

// newChunker 按参数构造从 r 读取的分块器。
func newChunker(r io.Reader, p ChunkerParams) *chunker {
	nbits := bits.TrailingZeros(uint(p.AvgSize))
	return &chunker{
		r:      r,
		params: p,
		maskS:  uint64(1)<<(nbits+1) - 1,
		maskL:  uint64(1)<<(nbits-1) - 1,
	}
}

// fill 尽量把 buf 补充到 n 字节；返回是否到达 EOF。
func (c *chunker) fill(n int) bool {
	for len(c.buf)-c.off < n && !c.eof {
		if c.off > 0 {
			copy(c.buf, c.buf[c.off:])
			c.buf = c.buf[:len(c.buf)-c.off]
			c.off = 0
		}
		need := n - len(c.buf)
		if need < 32*1024 {
			need = 32 * 1024
		}
		tmp := make([]byte, need)
		m, err := io.ReadAtLeast(c.r, tmp, 1)
		c.buf = append(c.buf, tmp[:m]...)
		if err != nil {
			c.eof = true
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				// 记录读错误，Next 会在缓冲数据消费完后返回它
				c.readErr = err
			}
		}
	}
	return len(c.buf)-c.off < n
}

// Next 从流中切出下一块；返回 (nil, io.EOF) 表示流结束。
func (c *chunker) Next() ([]byte, error) {
	if c.eof && c.off >= len(c.buf) {
		if c.readErr != nil {
			err := c.readErr
			c.readErr = nil
			return nil, err
		}
		return nil, io.EOF
	}
	p := c.params
	// 保证窗口里至少有 MaxSize 字节（或已到 EOF）
	c.fill(p.MaxSize)
	avail := len(c.buf) - c.off
	if avail == 0 {
		if c.readErr != nil {
			err := c.readErr
			c.readErr = nil
			return nil, err
		}
		return nil, io.EOF
	}

	limit := avail
	if limit > p.MaxSize {
		limit = p.MaxSize
	}
	data := c.buf[c.off : c.off+limit]

	cut := 0
	if limit <= p.MinSize {
		cut = limit
	} else {
		var fp uint64
		i := p.MinSize
		normal := p.AvgSize
		if normal > limit {
			normal = limit
		}
		for ; i < normal; i++ {
			fp = (fp << 1) + gearTable[data[i]]
			if fp&c.maskS == 0 {
				cut = i + 1
				break
			}
		}
		if cut == 0 {
			for ; i < limit; i++ {
				fp = (fp << 1) + gearTable[data[i]]
				if fp&c.maskL == 0 {
					cut = i + 1
					break
				}
			}
		}
		if cut == 0 {
			cut = limit
		}
	}
	out := make([]byte, cut)
	copy(out, data[:cut])
	c.off += cut
	return out, nil
}
