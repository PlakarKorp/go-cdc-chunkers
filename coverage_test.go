package chunkers

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// errSetupImpl is an implementation whose Setup always fails, to exercise the
// Setup-error paths of the constructors.
type errSetupImpl struct{}

func (errSetupImpl) DefaultOptions() *ChunkerOpts {
	return &ChunkerOpts{MinSize: 8, NormalSize: 16, MaxSize: 64}
}
func (errSetupImpl) Setup(_ *ChunkerOpts) error                    { return errors.New("setup failed") }
func (errSetupImpl) Validate(_ *ChunkerOpts) error                 { return nil }
func (errSetupImpl) Algorithm(_ *ChunkerOpts, _ []byte, n int) int { return n }

func init() {
	_ = Register("errsetup", func() ChunkerImplementation { return errSetupImpl{} })
}

// errReader returns an error (not io.EOF) partway through, to exercise the
// non-EOF read-error branches of Next/Copy/Split.
type errReader struct {
	data []byte
	pos  int
	fail int // byte offset at which to return the error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.pos >= r.fail {
		return 0, errors.New("read boom")
	}
	n := copy(p, r.data[r.pos:min(r.fail, len(r.data))])
	r.pos += n
	return n, nil
}

// failWriter fails on Write, to exercise Copy's writer-error branch.
type failWriter struct{}

func (failWriter) Write(_ []byte) (int, error) { return 0, errors.New("write boom") }

// TestNewChunker_NormalSizeDefaulted covers the NormalSize defaulting branch
// (opts non-nil with NormalSize==0 but Min/Max set).
func TestNewChunker_NormalSizeDefaulted(t *testing.T) {
	c, err := NewChunker("testimpl", bytes.NewReader(nil), &ChunkerOpts{MinSize: 8, MaxSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	if c.NormalSize() != 16 { // testimpl default
		t.Fatalf("NormalSize not defaulted: got %d", c.NormalSize())
	}
}

// TestNewChunker_SetupError covers the Setup-error return in NewChunker.
func TestNewChunker_SetupError(t *testing.T) {
	if _, err := NewChunker("errsetup", bytes.NewReader(nil), nil); err == nil {
		t.Fatal("expected Setup error from NewChunker")
	}
}

// TestNewChunkerBuffer_UnknownAlgorithm covers the newChunker-error return in
// NewChunkerBuffer (before the buffer-size check).
func TestNewChunkerBuffer_UnknownAlgorithm(t *testing.T) {
	if _, err := NewChunkerBuffer("nope-not-registered", bytes.NewReader(nil), nil, make([]byte, 1<<20)); err == nil {
		t.Fatal("expected unknown-algorithm error")
	}
}

// TestNext_ReadError covers Next's non-EOF read-error branch.
func TestNext_ReadError(t *testing.T) {
	r := &errReader{data: make([]byte, 8), fail: 4}
	c, err := NewChunker("testimpl", r, &ChunkerOpts{MinSize: 8, NormalSize: 16, MaxSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Next(); err == nil {
		t.Fatal("expected read error from Next")
	}
}

// TestCopy_ReadError covers Copy's non-EOF error propagation.
func TestCopy_ReadError(t *testing.T) {
	r := &errReader{data: make([]byte, 8), fail: 4}
	c, _ := NewChunker("testimpl", r, &ChunkerOpts{MinSize: 8, NormalSize: 16, MaxSize: 64})
	if _, err := c.Copy(&bytes.Buffer{}); err == nil {
		t.Fatal("expected read error from Copy")
	}
}

// TestCopy_WriteError covers Copy's writer-error branch.
func TestCopy_WriteError(t *testing.T) {
	c, _ := NewChunker("testimpl", bytes.NewReader(makeData(128)), &ChunkerOpts{MinSize: 8, NormalSize: 16, MaxSize: 64})
	if _, err := c.Copy(failWriter{}); err == nil {
		t.Fatal("expected write error from Copy")
	}
}

// TestSplit_ReadError covers Split's non-EOF error propagation.
func TestSplit_ReadError(t *testing.T) {
	r := &errReader{data: make([]byte, 8), fail: 4}
	c, _ := NewChunker("testimpl", r, &ChunkerOpts{MinSize: 8, NormalSize: 16, MaxSize: 64})
	err := c.Split(func(_, _ uint, _ []byte) error { return nil })
	if err == nil {
		t.Fatal("expected read error from Split")
	}
}

// TestSplit_CallbackError covers Split's callback-error branch.
func TestSplit_CallbackError(t *testing.T) {
	c, _ := NewChunker("testimpl", bytes.NewReader(makeData(128)), &ChunkerOpts{MinSize: 8, NormalSize: 16, MaxSize: 64})
	want := errors.New("callback boom")
	err := c.Split(func(_, _ uint, _ []byte) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("expected callback error, got %v", err)
	}
}

// zeroForeverReader always returns (0, nil): the pathological no-progress case
// the peek guard must break out of with io.ErrNoProgress.
type zeroForeverReader struct{}

func (zeroForeverReader) Read(_ []byte) (int, error) { return 0, nil }

// TestPeek_NoProgress covers reader.go's maxConsecutiveEmptyReads guard
// (tries <= 1 -> io.ErrNoProgress).
func TestPeek_NoProgress(t *testing.T) {
	var cr bufReader
	cr.reset(zeroForeverReader{}, make([]byte, 64))
	_, err := cr.peek(32)
	if !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("expected io.ErrNoProgress, got %v", err)
	}
}

// TestPeek_EOFFallback covers the peek branch where the source ends with a
// clean (0, io.EOF) so cr.err is io.EOF and the short-read path returns it.
func TestPeek_EOFFallback(t *testing.T) {
	var cr bufReader
	cr.reset(bytes.NewReader([]byte("12345")), make([]byte, 64))
	data, err := cr.peek(32) // ask for more than available
	if err != io.EOF {
		t.Fatalf("expected io.EOF on short read, got %v", err)
	}
	if string(data) != "12345" {
		t.Fatalf("expected the 5 available bytes, got %q", data)
	}
}
