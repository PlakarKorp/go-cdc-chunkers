package chunkers_test

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math/rand"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
)

// oneByteReader yields at most one byte per Read, stressing the buffered
// reader's refill and compaction paths.
type oneByteReader struct{ r io.Reader }

func (o *oneByteReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return o.r.Read(p[:1])
}

func collectHashes(t *testing.T, c *chunkers.Chunker) ([][32]byte, int) {
	t.Helper()
	var hs [][32]byte
	total := 0
	for {
		b, err := c.Next()
		if len(b) > 0 {
			hs = append(hs, sha256.Sum256(b))
			total += len(b)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
	}
	return hs, total
}

func hashesEqual(a, b [][32]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestBufferEquivalence asserts that chunk boundaries are identical regardless
// of how the scan buffer is provided (internal vs caller-owned), its size
// (exactly MaxSize vs larger), and how the underlying reader delivers bytes
// (all-at-once vs one-byte-at-a-time). This is the core correctness guarantee
// for the buffered-reader rewrite and for NewChunkerBuffer.
func TestBufferEquivalence(t *testing.T) {
	algos := []string{
		"fastcdc-v1.0.0", "fastcdc", "kfastcdc",
		"jc", "jc-v1.1.0",
		"ultracdc", "ultracdc-v1.0.0",
	}
	sizes := []int{0, 1, 100, 2048, 65535, 65536, 65537, 200000, 1 << 20, 5 << 20}
	opts := func() *chunkers.ChunkerOpts {
		return &chunkers.ChunkerOpts{MinSize: 2048, MaxSize: 65536, NormalSize: 8192, Key: make([]byte, 32)}
	}

	r := rand.New(rand.NewSource(7))
	for _, algo := range algos {
		for _, sz := range sizes {
			data := make([]byte, sz)
			r.Read(data)

			ca, err := chunkers.NewChunker(algo, bytes.NewReader(data), opts())
			if err != nil {
				t.Fatalf("%s sz=%d NewChunker: %v", algo, sz, err)
			}
			ha, ta := collectHashes(t, ca)

			cb, err := chunkers.NewChunkerBuffer(algo, bytes.NewReader(data), opts(), make([]byte, opts().MaxSize))
			if err != nil {
				t.Fatalf("%s sz=%d chunkers.NewChunkerBuffer(MaxSize): %v", algo, sz, err)
			}
			hb, tb := collectHashes(t, cb)

			cc, err := chunkers.NewChunkerBuffer(algo, bytes.NewReader(data), opts(), make([]byte, opts().MaxSize*4))
			if err != nil {
				t.Fatalf("%s sz=%d chunkers.NewChunkerBuffer(4xMaxSize): %v", algo, sz, err)
			}
			hc, tc := collectHashes(t, cc)

			cd, err := chunkers.NewChunker(algo, &oneByteReader{bytes.NewReader(data)}, opts())
			if err != nil {
				t.Fatalf("%s sz=%d chunkers.NewChunker(1-byte): %v", algo, sz, err)
			}
			hd, td := collectHashes(t, cd)

			if ta != sz || tb != sz || tc != sz || td != sz {
				t.Fatalf("%s sz=%d: byte totals differ A=%d B=%d C=%d D=%d", algo, sz, ta, tb, tc, td)
			}
			if !hashesEqual(ha, hb) || !hashesEqual(ha, hc) || !hashesEqual(ha, hd) {
				t.Fatalf("%s sz=%d: chunk boundaries differ across buffer/reader variants "+
					"(chunks A=%d B=%d C=%d D=%d)", algo, sz, len(ha), len(hb), len(hc), len(hd))
			}
		}
	}
}

// TestNewChunkerBuffer_SizeContract checks the MaxSize floor: a buffer of
// exactly MaxSize is accepted, anything smaller is rejected.
func TestNewChunkerBuffer_SizeContract(t *testing.T) {
	opts := &chunkers.ChunkerOpts{MinSize: 2048, MaxSize: 65536, NormalSize: 8192}

	if _, err := chunkers.NewChunkerBuffer("fastcdc-v1.0.0", bytes.NewReader(nil), opts, make([]byte, opts.MaxSize-1)); err != chunkers.ErrBufferTooSmall {
		t.Fatalf("buffer < MaxSize: want chunkers.ErrBufferTooSmall, got %v", err)
	}
	if _, err := chunkers.NewChunkerBuffer("fastcdc-v1.0.0", bytes.NewReader(nil), opts, make([]byte, opts.MaxSize)); err != nil {
		t.Fatalf("buffer == MaxSize must be accepted, got %v", err)
	}
}

// TestNewChunkerBuffer_DefaultedMaxSize verifies the floor uses the *defaulted*
// MaxSize (a nil/zero option is filled in before the buffer is checked).
func TestNewChunkerBuffer_DefaultedMaxSize(t *testing.T) {
	// opts with MaxSize=0 -> defaulted to the implementation's default (64KiB
	// for fastcdc). A 64KiB buffer must therefore be accepted.
	opts := &chunkers.ChunkerOpts{MinSize: 2048, NormalSize: 8192}
	if _, err := chunkers.NewChunkerBuffer("fastcdc-v1.0.0", bytes.NewReader(nil), opts, make([]byte, 64*1024)); err != nil {
		t.Fatalf("defaulted MaxSize buffer rejected: %v", err)
	}
}

// TestReset_ReusesBuffer ensures Reset on a buffer-backed chunker reuses the
// caller-provided buffer rather than allocating a new one. We assert via
// allocation accounting: Reset+Next should allocate only the new bytes.Reader,
// never a fresh MaxSize scan buffer.
func TestReset_ReusesBuffer(t *testing.T) {
	opts := &chunkers.ChunkerOpts{MinSize: 2048, MaxSize: 65536, NormalSize: 8192}
	data := make([]byte, 1<<20)
	buf := make([]byte, opts.MaxSize)
	c, err := chunkers.NewChunkerBuffer("fastcdc-v1.0.0", bytes.NewReader(data), opts, buf)
	if err != nil {
		t.Fatal(err)
	}
	c.Next()

	rd := bytes.NewReader(data)
	allocs := testing.AllocsPerRun(50, func() {
		rd.Reset(data)
		c.Reset(rd)
		c.Next()
	})
	// A fresh MaxSize buffer would show up as a large allocation; reuse keeps
	// this at zero (rd is reused, so no per-iteration heap allocation at all).
	if allocs > 0 {
		t.Fatalf("Reset on a buffer-backed chunker allocated %.1f objects/op; expected buffer reuse (0)", allocs)
	}
}

func benchData16M() []byte {
	b := make([]byte, 16<<20)
	rand.New(rand.NewSource(0)).Read(b)
	return b
}

// BenchmarkNewChunker measures the default constructor (internally allocated
// 2*MaxSize buffer) over a whole stream.
func BenchmarkNewChunker(b *testing.B) {
	data := benchData16M()
	opts := &chunkers.ChunkerOpts{MinSize: 2048, MaxSize: 65536, NormalSize: 8192}
	r := bytes.NewReader(data)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Reset(data)
		c, _ := chunkers.NewChunker("fastcdc-v1.0.0", r, opts)
		for {
			if _, err := c.Next(); err != nil {
				break
			}
		}
	}
}

// BenchmarkNewChunkerBuffer measures a caller-owned, reused MaxSize buffer —
// the smallest-footprint mode for many concurrent chunkers.
func BenchmarkNewChunkerBuffer(b *testing.B) {
	data := benchData16M()
	opts := &chunkers.ChunkerOpts{MinSize: 2048, MaxSize: 65536, NormalSize: 8192}
	buf := make([]byte, opts.MaxSize)
	r := bytes.NewReader(data)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Reset(data)
		c, _ := chunkers.NewChunkerBuffer("fastcdc-v1.0.0", r, opts, buf)
		for {
			if _, err := c.Next(); err != nil {
				break
			}
		}
	}
}
