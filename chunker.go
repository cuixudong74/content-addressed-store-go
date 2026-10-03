package store

// Chunker splits a byte stream into content-defined chunks using a Gear
// rolling hash. Cut points are determined by content, so inserting or
// deleting bytes in the middle only affects nearby chunks.
type Chunker struct {
	minSize int
	avgSize int
	maxSize int
	mask    uint64
	h       uint64
	buf     []byte
}

// NewChunker creates a content-defined chunker. avgSize must be a power of two.
func NewChunker(minSize, avgSize, maxSize int) *Chunker {
	return &Chunker{
		minSize: minSize,
		avgSize: avgSize,
		maxSize: maxSize,
		mask:    uint64(avgSize - 1),
	}
}

// Write feeds data and returns any chunks that have been cut. The returned
// chunks are freshly allocated and owned by the caller.
func (c *Chunker) Write(p []byte) ([][]byte, error) {
	var out [][]byte
	for _, b := range p {
		c.buf = append(c.buf, b)
		c.h = (c.h << 1) ^ gearTable[b]
		if len(c.buf) >= c.minSize && (c.h&c.mask) == 0 {
			ch := make([]byte, len(c.buf))
			copy(ch, c.buf)
			out = append(out, ch)
			c.buf = c.buf[:0]
			c.h = 0
		} else if len(c.buf) >= c.maxSize {
			ch := make([]byte, len(c.buf))
			copy(ch, c.buf)
			out = append(out, ch)
			c.buf = c.buf[:0]
			c.h = 0
		}
	}
	return out, nil
}

// Flush returns the final buffered chunk, or nil if the stream was empty.
func (c *Chunker) Flush() ([]byte, error) {
	if len(c.buf) == 0 {
		return nil, nil
	}
	ch := make([]byte, len(c.buf))
	copy(ch, c.buf)
	c.buf = c.buf[:0]
	c.h = 0
	return ch, nil
}

// gearTable is the deterministic Gear hash table (algorithm version cdc-gear-v1).
// Generated with a fixed seed so the table is stable across runs and versions.
var gearTable [256]uint64

func init() {
	// SplitMix64 to expand a fixed seed into 256 deterministic uint64 values.
	const seed uint64 = 0x9e3779b97f4a7c15
	x := seed
	for i := 0; i < 256; i++ {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z = z ^ (z >> 31)
		gearTable[i] = z
	}
}
