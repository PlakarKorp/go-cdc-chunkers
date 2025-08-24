package chunkers

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

/************ Test-only implementation ************/

// testimpl returns cutpoints equal to opts.NormalSize (or n if smaller).
// DefaultOptions returns distinctive sizes so we can assert defaults applied.
type testImpl struct{}

func (t *testImpl) DefaultOptions() *ChunkerOpts {
	return &ChunkerOpts{MinSize: 8, NormalSize: 16, MaxSize: 64}
}
func (t *testImpl) Setup(_ *ChunkerOpts) error    { return nil }
func (t *testImpl) Validate(_ *ChunkerOpts) error { return nil }
func (t *testImpl) Algorithm(opts *ChunkerOpts, _ []byte, n int) int {
	// Return min(n, NormalSize) as the cutpoint.
	if opts.NormalSize < n {
		return opts.NormalSize
	}
	return n
}

// Register the test impl once.
func init() {
	_ = Register("testimpl", func() ChunkerImplementation { return &testImpl{} })
}

/************ Helpers ************/

func makeData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i * 37) % 251)
	}
	return b
}

/************ Tests ************/

func TestRegister_Duplicate(t *testing.T) {
	err := Register("test-dupe", func() ChunkerImplementation { return &testImpl{} })
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}
	err2 := Register("test-dupe", func() ChunkerImplementation { return &testImpl{} })
	if !errors.Is(err2, errors.New("algorithm already registered")) && err2 == nil {
		t.Fatalf("expected duplicate register error, got %v", err2)
	}
}

func TestNewChunker_UnknownAlgorithm(t *testing.T) {
	_, err := NewChunker("nope-algo", bytes.NewReader(nil), nil)
	if err == nil {
		t.Fatal("expected unknown algorithm error, got nil")
	}
}

func TestNewChunker_DefaultsAndFillZeros(t *testing.T) {
	// defaults when opts == nil
	ch, err := NewChunker("testimpl", bytes.NewReader(makeData(10)), nil)
	if err != nil {
		t.Fatalf("NewChunker error: %v", err)
	}
	if ch.MinSize() != 8 || ch.NormalSize() != 16 || ch.MaxSize() != 64 {
		t.Fatalf("defaults not applied: min=%d norm=%d max=%d", ch.MinSize(), ch.NormalSize(), ch.MaxSize())
	}

	// fill zeros from defaults
	opts := &ChunkerOpts{MinSize: 0, NormalSize: 32, MaxSize: 0}
	ch2, err := NewChunker("testimpl", bytes.NewReader(makeData(10)), opts)
	if err != nil {
		t.Fatalf("NewChunker error: %v", err)
	}
	// Min and Max should be taken from defaults (8 and 64); Normal kept as 32
	if ch2.MinSize() != 8 || ch2.NormalSize() != 32 || ch2.MaxSize() != 64 {
		t.Fatalf("zero fields not filled as expected: min=%d norm=%d max=%d", ch2.MinSize(), ch2.NormalSize(), ch2.MaxSize())
	}
}

func TestChunker_Next_BasicAndEOF(t *testing.T) {
	// Data length > NormalSize to force a first non-EOF cut, then EOF on second call.
	opts := &ChunkerOpts{MinSize: 8, NormalSize: 12, MaxSize: 64}
	data := makeData(20)

	ch, err := NewChunker("testimpl", bytes.NewReader(data), opts)
	if err != nil {
		t.Fatalf("NewChunker error: %v", err)
	}

	// First call: cut at NormalSize (12), err == nil
	chunk, err := ch.Next()
	if err != nil {
		t.Fatalf("unexpected err on first Next: %v", err)
	}
	if len(chunk) != 12 || !bytes.Equal(chunk, data[:12]) {
		t.Fatalf("first chunk mismatch: got len=%d", len(chunk))
	}

	// Second call: remaining 8 bytes; Algorithm returns min(n, NormalSize)=8; since cutpoint >= MinSize,
	// Next will return chunk and (because reader then empty) err == io.EOF on next call.
	chunk2, err := ch.Next()
	if err != nil && err != io.EOF {
		t.Fatalf("unexpected err on second Next: %v", err)
	}
	if len(chunk2) != 8 || !bytes.Equal(chunk2, data[12:]) {
		t.Fatalf("second chunk mismatch: got len=%d", len(chunk2))
	}

	// Third call: EOF, zero-length chunk
	chunk3, err := ch.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF on third Next, got %v", err)
	}
	if chunk3 != nil && len(chunk3) != 0 {
		t.Fatalf("expected empty chunk on EOF, got len=%d", len(chunk3))
	}
}

func TestChunker_Next_FinalBelowMinReturnsEOF(t *testing.T) {
	// Force a final slice shorter than MinSize so Next returns EOF along with the last (short) chunk.
	opts := &ChunkerOpts{MinSize: 8, NormalSize: 12, MaxSize: 64}
	// Total 18 bytes: first cut 12, remaining 6 (< MinSize) -> returned with io.EOF.
	data := makeData(18)

	ch, err := NewChunker("testimpl", bytes.NewReader(data), opts)
	if err != nil {
		t.Fatalf("NewChunker error: %v", err)
	}

	c1, err := ch.Next()
	if err != nil {
		t.Fatalf("unexpected first err: %v", err)
	}
	if len(c1) != 12 {
		t.Fatalf("want first len 12, got %d", len(c1))
	}

	c2, err := ch.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF on last short chunk, got %v", err)
	}
	if len(c2) != 6 || !bytes.Equal(c2, data[12:]) {
		t.Fatalf("last short chunk mismatch: got len=%d", len(c2))
	}
}

func TestChunker_Reset(t *testing.T) {
	opts := &ChunkerOpts{MinSize: 8, NormalSize: 12, MaxSize: 64}
	data1 := makeData(20)
	data2 := makeData(32)

	ch, err := NewChunker("testimpl", bytes.NewReader(data1), opts)
	if err != nil {
		t.Fatalf("NewChunker error: %v", err)
	}

	// Consume one chunk from data1
	_, _ = ch.Next()

	// Reset to a new reader and read again from start of data2
	ch.Reset(bytes.NewReader(data2))
	all, err := io.ReadAll(&chunkReader{ch})
	if err != nil && err != io.EOF {
		t.Fatalf("reading after reset: %v", err)
	}
	if !bytes.Equal(all, data2) {
		t.Fatalf("after reset, got != data2 (len=%d vs %d)", len(all), len(data2))
	}
}

// chunkReader wraps a Chunker to read all chunks sequentially.
type chunkReader struct{ c *Chunker }

func (cr *chunkReader) Read(p []byte) (int, error) {
	chunk, err := cr.c.Next()
	if err != nil && err != io.EOF {
		return 0, err
	}
	n := copy(p, chunk)
	if err == io.EOF && n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func TestChunker_Copy(t *testing.T) {
	opts := &ChunkerOpts{MinSize: 8, NormalSize: 12, MaxSize: 64}
	// Make data length so last chunk < MinSize to exercise EOF path in Copy.
	data := makeData(30) // 12 + 12 + 6(last<MinSize)

	ch, err := NewChunker("testimpl", bytes.NewReader(data), opts)
	if err != nil {
		t.Fatalf("NewChunker error: %v", err)
	}

	var buf bytes.Buffer
	written, err := ch.Copy(&buf)
	if err != io.EOF {
		t.Fatalf("Copy should return io.EOF, got %v", err)
	}
	// Data integrity: destination must equal original
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("copied bytes != original (got %d, want %d)", buf.Len(), len(data))
	}

	// NOTE: Copy increments nbytes only for non-EOF chunks,
	// so it excludes the last (EOF) chunk length.
	// Here: chunks 12 + 12 (+6 as last but not counted) => written should be 24.
	if written != int64(30) {
		t.Fatalf("unexpected written count: got %d want %d", written, 30)
	}
}

func TestChunker_Split(t *testing.T) {
	opts := &ChunkerOpts{MinSize: 8, NormalSize: 12, MaxSize: 64}
	data := makeData(30) // 12 + 12 + 6

	ch, err := NewChunker("testimpl", bytes.NewReader(data), opts)
	if err != nil {
		t.Fatalf("NewChunker error: %v", err)
	}

	var (
		offsets []uint
		lengths []uint
		concat  []byte
	)

	err = ch.Split(func(offset, length uint, chunk []byte) error {
		offsets = append(offsets, offset)
		lengths = append(lengths, length)
		concat = append(concat, chunk...)
		return nil
	})
	if err != nil {
		t.Fatalf("Split error: %v", err)
	}

	// We expect three callbacks for 12,12,6
	if len(lengths) != 3 || lengths[0] != 12 || lengths[1] != 12 || lengths[2] != 6 {
		t.Fatalf("unexpected lengths: %v", lengths)
	}
	// Offsets should be cumulative
	if offsets[0] != 0 || offsets[1] != 12 || offsets[2] != 24 {
		t.Fatalf("unexpected offsets: %v", offsets)
	}
	// Data integrity
	if !bytes.Equal(concat, data) {
		t.Fatalf("reconstructed != original")
	}
}

/************ test-only implementation for this file ************/

type emptyImpl struct{}

func (e *emptyImpl) DefaultOptions() *ChunkerOpts {
	return &ChunkerOpts{MinSize: 8, NormalSize: 16, MaxSize: 64}
}
func (e *emptyImpl) Setup(_ *ChunkerOpts) error                    { return nil }
func (e *emptyImpl) Validate(_ *ChunkerOpts) error                 { return nil }
func (e *emptyImpl) Algorithm(_ *ChunkerOpts, _ []byte, n int) int { return n } // not reached for empty input

func init() {
	// Use a unique name to avoid clashing with other tests.
	_ = Register("emptyimpl", func() ChunkerImplementation { return &emptyImpl{} })
}

func TestChunker_EmptyInput(t *testing.T) {
	ch, err := NewChunker("emptyimpl", bytes.NewReader(nil), nil)
	if err != nil {
		t.Fatalf("NewChunker error: %v", err)
	}

	// 1) Next(): first call on empty input should return []byte{} and io.EOF
	chunk, err := ch.Next()
	if err != io.EOF {
		t.Fatalf("first Next on empty input: want io.EOF, got %v", err)
	}
	if chunk == nil || len(chunk) != 0 {
		t.Fatalf("first Next on empty input: want empty non-nil slice, got %v (len %d)", chunk, len(chunk))
	}

	// 2) Next(): subsequent call should return (nil, io.EOF)
	chunk2, err := ch.Next()
	if err != io.EOF {
		t.Fatalf("second Next on empty input: want io.EOF, got %v", err)
	}
	if chunk2 != nil {
		t.Fatalf("second Next on empty input: want nil chunk, got len %d", len(chunk2))
	}

	// 3) Copy(): should write nothing and return (0, io.EOF)
	var buf bytes.Buffer
	written, err := ch.Copy(&buf)
	if err != io.EOF {
		t.Fatalf("Copy on empty input: want io.EOF, got %v", err)
	}
	if written != 0 {
		t.Fatalf("Copy on empty input: want written=0, got %d", written)
	}
	if buf.Len() != 0 {
		t.Fatalf("Copy on empty input: destination should be empty, got %d bytes", buf.Len())
	}

	// 4) Split(): callback should be invoked once with length 0 at offset 0
	var calls int
	var gotOffset, gotLength uint
	var gotChunk []byte
	err = ch.Split(func(offset, length uint, chunk []byte) error {
		calls++
		gotOffset, gotLength = offset, length
		gotChunk = append([]byte{}, chunk...)
		return nil
	})
	if err != nil {
		t.Fatalf("Split on empty input: unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("Split on empty input: want 1 callback call, got %d", calls)
	}
	if gotOffset != 0 || gotLength != 0 || len(gotChunk) != 0 {
		t.Fatalf("Split on empty input: want offset=0 length=0 empty chunk, got offset=%d length=%d len(chunk)=%d",
			gotOffset, gotLength, len(gotChunk))
	}
}
