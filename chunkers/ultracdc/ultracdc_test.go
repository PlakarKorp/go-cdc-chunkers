package ultracdc

import (
	"bytes"
	"errors"
	"io"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

func TestUltraCDC_DefaultOptions(t *testing.T) {
	impl := newUltraCDC().(*UltraCDC)
	opts := impl.DefaultOptions()
	if opts.MinSize != 2*1024 || opts.NormalSize != 10*1024 || opts.MaxSize != 64*1024 {
		t.Fatalf("unexpected defaults: min=%d norm=%d max=%d", opts.MinSize, opts.NormalSize, opts.MaxSize)
	}
}

func TestUltraCDC_Validate(t *testing.T) {
	impl := newUltraCDC().(*UltraCDC)
	valid := &chunkers.ChunkerOpts{MinSize: 2 * 1024, NormalSize: 10 * 1024, MaxSize: 64 * 1024}
	if err := impl.Validate(valid); err != nil {
		t.Fatalf("valid options should pass: %v", err)
	}

	// NormalSize errors
	for _, o := range []*chunkers.ChunkerOpts{
		{MinSize: 2048, NormalSize: 0, MaxSize: 65536},
		{MinSize: 2048, NormalSize: 63, MaxSize: 65536},
		{MinSize: 2048, NormalSize: 1024*1024*1024 + 1, MaxSize: 65536},
	} {
		if err := impl.Validate(o); !errors.Is(err, ErrNormalSize) {
			t.Errorf("expected ErrNormalSize, got %v", err)
		}
	}

	// MinSize errors
	for _, o := range []*chunkers.ChunkerOpts{
		{MinSize: 0, NormalSize: 8192, MaxSize: 65536},
		{MinSize: 63, NormalSize: 8192, MaxSize: 65536},
		{MinSize: 1024*1024*1024 + 1, NormalSize: 8192, MaxSize: 2 * 1024 * 1024},
		{MinSize: 8192, NormalSize: 8192, MaxSize: 65536}, // >= NormalSize
		{MinSize: 9000, NormalSize: 8192, MaxSize: 65536}, // > NormalSize
	} {
		if err := impl.Validate(o); !errors.Is(err, ErrMinSize) {
			t.Errorf("expected ErrMinSize, got %v", err)
		}
	}

	// MaxSize errors
	for _, o := range []*chunkers.ChunkerOpts{
		{MinSize: 2048, NormalSize: 8192, MaxSize: 0},
		{MinSize: 2048, NormalSize: 8192, MaxSize: 63},
		{MinSize: 2048, NormalSize: 8192, MaxSize: 1024*1024*1024 + 1},
		{MinSize: 2048, NormalSize: 8192, MaxSize: 8192}, // <= NormalSize
		{MinSize: 2048, NormalSize: 8192, MaxSize: 8000}, // < NormalSize
	} {
		if err := impl.Validate(o); !errors.Is(err, ErrMaxSize) {
			t.Errorf("expected ErrMaxSize, got %v", err)
		}
	}
}

func TestUltraCDC_Algorithm_PanicWhenNTooLarge(t *testing.T) {
	impl := newUltraCDC().(*UltraCDC)
	opts := impl.DefaultOptions()
	data := make([]byte, 16)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when n > len(data)")
		}
	}()
	_ = impl.Algorithm(opts, data, len(data)+1)
}

func TestUltraCDC_Algorithm_ReturnsNWhenLEMinSize(t *testing.T) {
	impl := newUltraCDC().(*UltraCDC)
	opts := &chunkers.ChunkerOpts{MinSize: 128, NormalSize: 256, MaxSize: 1024}
	data := make([]byte, 100) // n <= MinSize
	got := impl.Algorithm(opts, data, len(data))
	if got != len(data) {
		t.Fatalf("want %d, got %d", len(data), got)
	}
}

func TestUltraCDC_Algorithm_LowEntropyCutAfterThreshold(t *testing.T) {
	impl := newUltraCDC().(*UltraCDC)

	// Smaller sizes so we can hit 64 identical windows cheaply.
	min := 64
	norm := 512
	max := 4096
	opts := &chunkers.ChunkerOpts{MinSize: min, NormalSize: norm, MaxSize: max}

	// Make data such that every 8-byte window after min is identical to the previous:
	// all-zeroes is fine.
	// Need >= 64 consecutive equality hits.
	// i runs: from min+8 to <= n-8, step 8. Choose n to allow >64 iterations.
	iterations := 70
	n := min + 8*(iterations+2) // generous slack
	data := make([]byte, n)     // all zeros

	cut := impl.Algorithm(opts, data, n)
	// Expect a cut at i+8 when lowEntropyCount reaches threshold (64):
	// first comparison (i=min+8) sets count=1, so after reaching 64,
	// i should be min+8*64, cutpoint = i+8 = min+8*(64+1)
	want := min + 8*(64+1)
	if cut != want {
		t.Fatalf("low-entropy cut: want %d, got %d", want, cut)
	}
}

func TestUltraCDC_Algorithm_EarlyCutWithMaskS_BeforeNormal(t *testing.T) {
	impl := newUltraCDC().(*UltraCDC)

	// Force the inner j-loop's first check (dist & mask) == 0 before NormalSize.
	// Make outBufWin (data[min:min+8]) all 0xAA so dist=0.
	min := 64
	// Keep normal high so we use maskS (not maskL) at the first iteration.
	norm := 2048
	max := 4096
	opts := &chunkers.ChunkerOpts{MinSize: min, NormalSize: norm, MaxSize: max}

	// Construct data:
	// - data[min:min+8] == 0xAA (dist=0)
	// - next 8-byte window must be different to avoid the low-entropy path.
	// - Provide some extra bytes so the loop runs.
	n := min + 8*4
	data := make([]byte, n)
	for i := 0; i < 8; i++ {
		data[min+i] = 0xAA
	}
	// Make next window different at i=min+8
	data[min+8] = 0xAB

	cut := impl.Algorithm(opts, data, n)
	// On first loop i = min+8, dist==0, mask==maskS -> cutpoint = i + j with j==0 => i.
	want := min + 8
	if cut != want {
		t.Fatalf("maskS early cut: want %d, got %d", want, cut)
	}
}

func TestUltraCDC_Algorithm_EarlyCutWithMaskL_AfterNormal(t *testing.T) {
	impl := newUltraCDC().(*UltraCDC)

	// Ensure we cross >= NormalSize so mask switches to maskL, and still cut via dist==0.
	min := 64
	// Put normal right at the first loop iteration (i starts at min+8),
	// so i >= normal triggers maskL.
	norm := min + 8
	max := 4096
	opts := &chunkers.ChunkerOpts{MinSize: min, NormalSize: norm, MaxSize: max}

	n := min + 8*4
	data := make([]byte, n)
	for i := 0; i < 8; i++ {
		data[min+i] = 0xAA // outBufWin -> dist==0
	}
	// Ensure next window differs to avoid low-entropy counter.
	data[min+8] = 0xAB

	cut := impl.Algorithm(opts, data, n)
	// On first iteration i=min+8 which is >= normal, mask==maskL and dist==0 -> cut at i.
	want := min + 8
	if cut != want {
		t.Fatalf("maskL early cut: want %d, got %d", want, cut)
	}
}

func TestUltraCDC_EndToEndChunking(t *testing.T) {
	data := make([]byte, 100*1024)
	for i := range data {
		data[i] = byte((i * 37) % 251)
	}

	ch, err := chunkers.NewChunker("ultracdc", bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("NewChunker error: %v", err)
	}

	// Collect chunks (copy each; Next may reuse buffer)
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

	// Check size constraints (last chunk may be < MinSize)
	for i, c := range chunks {
		if i != len(chunks)-1 && len(c) < ch.MinSize() {
			t.Fatalf("chunk %d too small: %d < %d", i, len(c), ch.MinSize())
		}
		if len(c) > ch.MaxSize() {
			t.Fatalf("chunk %d too large: %d > %d", i, len(c), ch.MaxSize())
		}
	}

	// Integrity
	var got []byte
	for _, c := range chunks {
		got = append(got, c...)
	}
	if !bytes.Equal(data, got) {
		t.Fatal("reconstructed data != original")
	}
}
