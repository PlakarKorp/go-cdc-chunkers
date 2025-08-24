package fastcdc

import (
	"bytes"
	"errors"
	"io"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

func TestFastCDC4Stadia_DefaultOptions(t *testing.T) {
	impl := newFastCDC4Stadia().(*FastCDC4Stadia)
	opts := impl.DefaultOptions()

	if opts.MinSize != 2*1024 {
		t.Errorf("MinSize: want %d, got %d", 2*1024, opts.MinSize)
	}
	if opts.MaxSize != 64*1024 {
		t.Errorf("MaxSize: want %d, got %d", 64*1024, opts.MaxSize)
	}
	if opts.NormalSize != 8*1024 {
		t.Errorf("NormalSize: want %d, got %d", 8*1024, opts.NormalSize)
	}
}

func TestFastCDC4Stadia_Validate(t *testing.T) {
	impl := newFastCDC4Stadia().(*FastCDC4Stadia)

	// Valid baseline
	valid := &chunkers.ChunkerOpts{MinSize: 2 * 1024, MaxSize: 64 * 1024, NormalSize: 8 * 1024}
	if err := impl.Validate(valid); err != nil {
		t.Fatalf("valid options should pass, got %v", err)
	}

	// NormalSize errors
	for _, os := range []*chunkers.ChunkerOpts{
		{MinSize: 2048, MaxSize: 65536, NormalSize: 0},                  // zero
		{MinSize: 2048, MaxSize: 65536, NormalSize: 63},                 // < 64
		{MinSize: 2048, MaxSize: 65536, NormalSize: 1024*1024*1024 + 1}, // > 1GB
	} {
		if err := impl.Validate(os); !errors.Is(err, errNormalSize) {
			t.Errorf("expected errNormalSize, got %v", err)
		}
	}

	// MinSize errors
	for _, os := range []*chunkers.ChunkerOpts{
		{MinSize: 0, MaxSize: 65536, NormalSize: 8192},                            // < 64
		{MinSize: 63, MaxSize: 65536, NormalSize: 8192},                           // < 64
		{MinSize: 1024*1024*1024 + 1, MaxSize: 2 * 1024 * 1024, NormalSize: 8192}, // > 1GB
		{MinSize: 8192, MaxSize: 65536, NormalSize: 8192},                         // >= NormalSize
		{MinSize: 9000, MaxSize: 65536, NormalSize: 8192},                         // > NormalSize
	} {
		if err := impl.Validate(os); !errors.Is(err, errMinSize) {
			t.Errorf("expected errMinSize, got %v", err)
		}
	}

	// MaxSize errors
	for _, os := range []*chunkers.ChunkerOpts{
		{MinSize: 2048, MaxSize: 0, NormalSize: 8192},                  // < 64
		{MinSize: 2048, MaxSize: 63, NormalSize: 8192},                 // < 64
		{MinSize: 2048, MaxSize: 1024*1024*1024 + 1, NormalSize: 8192}, // > 1GB
		{MinSize: 2048, MaxSize: 8192, NormalSize: 8192},               // <= NormalSize
		{MinSize: 2048, MaxSize: 8000, NormalSize: 8192},               // < NormalSize
	} {
		if err := impl.Validate(os); !errors.Is(err, errMaxSize) {
			t.Errorf("expected errMaxSize, got %v", err)
		}
	}
}

func TestFastCDC4Stadia_Algorithm_PanicWhenNTooLarge(t *testing.T) {
	impl := newFastCDC4Stadia().(*FastCDC4Stadia)
	opts := impl.DefaultOptions()

	data := make([]byte, 1024)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when N > len(data), got none")
		}
	}()
	_ = impl.Algorithm(opts, data, len(data)+1)
}

func TestFastCDC4Stadia_Algorithm_BasicCuts(t *testing.T) {
	impl := newFastCDC4Stadia().(*FastCDC4Stadia)
	opts := impl.DefaultOptions()

	// Case 1: N <= MinSize -> returns N
	dataSmall := make([]byte, opts.MinSize-128)
	cutSmall := impl.Algorithm(opts, dataSmall, len(dataSmall))
	if cutSmall != len(dataSmall) {
		t.Fatalf("N<=MinSize: want %d, got %d", len(dataSmall), cutSmall)
	}

	// Case 2: N >= MaxSize -> cut in [MinSize..MaxSize]
	dataLarge := make([]byte, opts.MaxSize*2)
	cutLarge := impl.Algorithm(opts, dataLarge, len(dataLarge))
	if cutLarge < opts.MinSize || cutLarge > opts.MaxSize {
		t.Fatalf("N>=MaxSize: cut %d, want between [%d..%d]", cutLarge, opts.MinSize, opts.MaxSize)
	}

	// Case 3: MinSize < N < MaxSize -> cut in (MinSize..=N]
	nMid := (opts.MinSize + opts.MaxSize) / 2
	dataMid := make([]byte, nMid)
	cutMid := impl.Algorithm(opts, dataMid, len(dataMid))
	if cutMid <= opts.MinSize || cutMid > len(dataMid) {
		t.Fatalf("Min<N<Max: cut %d, want in (%d..%d]", cutMid, opts.MinSize, len(dataMid))
	}
}

func TestFastCDC4Stadia_EndToEndChunking(t *testing.T) {
	// Uses the registered name to test the full chunker pipeline.
	data := make([]byte, 100*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}

	ch, err := chunkers.NewChunker("fastcdc4stadia", bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("NewChunker error: %v", err)
	}

	// Basic property sanity
	if ch.MinSize() != 2*1024 || ch.MaxSize() != 64*1024 || ch.NormalSize() != 8*1024 {
		t.Fatalf("unexpected defaults: min=%d max=%d norm=%d", ch.MinSize(), ch.MaxSize(), ch.NormalSize())
	}

	var chunks [][]byte
	for {
		c, err := ch.Next()
		if err != nil {
			if err == io.EOF {
				if len(c) > 0 {
					chunks = append(chunks, append([]byte{}, c...))
				}
				break
			}
			t.Fatalf("Next error: %v", err)
		}
		chunks = append(chunks, append([]byte{}, c...))
	}

	if len(chunks) == 0 {
		t.Fatal("no chunks produced")
	}

	// Check size constraints (allow final chunk smaller than MinSize)
	for i, c := range chunks {
		if i != len(chunks)-1 && len(c) < ch.MinSize() {
			t.Fatalf("chunk %d too small: %d < %d", i, len(c), ch.MinSize())
		}
		if len(c) > ch.MaxSize() {
			t.Fatalf("chunk %d too large: %d > %d", i, len(c), ch.MaxSize())
		}
	}

	// Integrity: reassemble and compare
	var got []byte
	for _, c := range chunks {
		got = append(got, c...)
	}
	if !bytes.Equal(data, got) {
		t.Fatal("reconstructed data != original")
	}
}

func TestFastCDC4Stadia_Algorithm_HashBelowThresh(t *testing.T) {
	impl := newFastCDC4Stadia().(*FastCDC4Stadia)

	// Make thresh very large: denom = (NormalSize - MinSize + 1) = 2
	// so thresh ~= MaxUint64 / 2, which is easy to be <=.
	min := 64
	opts := &chunkers.ChunkerOpts{
		MinSize:    min,
		NormalSize: min + 1, // validates and maximizes thresh
		MaxSize:    4096,    // > NormalSize to pass Validate
	}
	if err := impl.Validate(opts); err != nil {
		t.Fatalf("test opts should validate, got %v", err)
	}

	// N must be >= MinSize so the main loop runs at least once.
	N := min + 128
	data := make([]byte, N)

	// Try a handful of deterministic patterns; we only need ONE to
	// hit the branch and return exactly i == MinSize on the first iteration.
	patterns := [][]byte{
		bytes.Repeat([]byte{0x00}, min),
		bytes.Repeat([]byte{0xFF}, min),
		bytes.Repeat([]byte{0x55}, min),
		bytes.Repeat([]byte{0xAA}, min),
		func() []byte {
			b := make([]byte, min)
			for i := range b {
				b[i] = byte(i)
			}
			return b
		}(),
		func() []byte {
			b := make([]byte, min)
			for i := range b {
				b[i] = byte(255 - i)
			}
			return b
		}(),
	}

	hit := false
	for _, p := range patterns {
		// Fill the first MinSize bytes with the pattern; the rest can be zero.
		copy(data[:min], p)
		for i := min; i < len(data); i++ {
			data[i] = 0
		}

		cut := impl.Algorithm(opts, data, len(data))
		if cut == min {
			// Returned on the first iteration (i == MinSize) via `hash <= thresh`.
			hit = true
			break
		}
	}

	if !hit {
		t.Fatalf("expected at least one pattern to trigger hash <= thresh and return MinSize (%d)", min)
	}
}
