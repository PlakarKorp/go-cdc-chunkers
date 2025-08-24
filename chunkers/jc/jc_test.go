package jc

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

func TestJC_DefaultOptions(t *testing.T) {
	impl := newJC().(*JC)
	opts := impl.DefaultOptions()
	if opts.MinSize != 2*1024 || opts.MaxSize != 64*1024 || opts.NormalSize != 8*1024 || opts.Key != nil {
		t.Fatalf("unexpected defaults: min=%d max=%d norm=%d key=%v", opts.MinSize, opts.MaxSize, opts.NormalSize, opts.Key)
	}
}

func TestJC_Validate(t *testing.T) {
	impl := newJC().(*JC)

	// valid baseline
	valid := &chunkers.ChunkerOpts{MinSize: 2 * 1024, MaxSize: 64 * 1024, NormalSize: 8 * 1024}
	if err := impl.Validate(valid); err != nil {
		t.Fatalf("valid opts should pass: %v", err)
	}

	// NormalSize errors
	for _, tt := range []*chunkers.ChunkerOpts{
		{MinSize: 2048, MaxSize: 65536, NormalSize: 0},
		{MinSize: 2048, MaxSize: 65536, NormalSize: 63},
		{MinSize: 2048, MaxSize: 65536, NormalSize: 1024*1024*1024 + 1},
	} {
		if err := impl.Validate(tt); !errors.Is(err, errNormalSize) {
			t.Errorf("expected errNormalSize, got %v", err)
		}
	}

	// MinSize errors
	for _, tt := range []*chunkers.ChunkerOpts{
		{MinSize: 0, MaxSize: 65536, NormalSize: 8192},
		{MinSize: 63, MaxSize: 65536, NormalSize: 8192},
		{MinSize: 1024*1024*1024 + 1, MaxSize: 2 * 1024 * 1024, NormalSize: 8192},
		{MinSize: 8192, MaxSize: 65536, NormalSize: 8192}, // >= NormalSize
		{MinSize: 9000, MaxSize: 65536, NormalSize: 8192}, // > NormalSize
	} {
		if err := impl.Validate(tt); !errors.Is(err, errMinSize) {
			t.Errorf("expected errMinSize, got %v", err)
		}
	}

	// MaxSize errors
	for _, tt := range []*chunkers.ChunkerOpts{
		{MinSize: 2048, MaxSize: 0, NormalSize: 8192},
		{MinSize: 2048, MaxSize: 63, NormalSize: 8192},
		{MinSize: 2048, MaxSize: 1024*1024*1024 + 1, NormalSize: 8192},
		{MinSize: 2048, MaxSize: 8192, NormalSize: 8192}, // <= NormalSize
		{MinSize: 2048, MaxSize: 8000, NormalSize: 8192}, // < NormalSize
	} {
		if err := impl.Validate(tt); !errors.Is(err, errMaxSize) {
			t.Errorf("expected errMaxSize, got %v", err)
		}
	}
}

func TestJC_Setup_LegacyAndCalculatedMasksAndJump(t *testing.T) {
	// Legacy: either explicit legacy impl or default (2k/64k/8k) on new impl
	leg := newLegacyJC().(*JC)
	opts := (&JC{}).DefaultOptions()
	if err := leg.Setup(opts); err != nil {
		t.Fatalf("legacy Setup error: %v", err)
	}
	if leg.maskC != 0x590003570000 || leg.maskJ != 0x590003560000 {
		t.Fatalf("legacy masks mismatch: C=0x%x J=0x%x", leg.maskC, leg.maskJ)
	}

	// Non-legacy (force calculated masks) + verify jumpLength
	// Choose power-of-two NormalSize so bits = log2(NormalSize) is integral.
	newImpl := newJC().(*JC)
	custom := &chunkers.ChunkerOpts{
		MinSize:    1024,  // < NormalSize
		MaxSize:    32768, // > NormalSize
		NormalSize: 4096,  // 2^12
	}
	if err := newImpl.Setup(custom); err != nil {
		t.Fatalf("Setup error: %v", err)
	}
	// Expected masks
	// bits = 12 => cOnes=11, jOnes=10
	wantC := generateSpacedMask(11, 64)
	wantJ := embedMask(wantC)
	if newImpl.maskC != wantC || newImpl.maskJ != wantJ {
		t.Fatalf("calculated masks mismatch: got C=0x%x J=0x%x, want C=0x%x J=0x%x",
			newImpl.maskC, newImpl.maskJ, wantC, wantJ)
	}

	// jumpLength = 2^(cOnes + jOnes) / (2^cOnes - 2^jOnes)
	// For cOnes=11, jOnes=10: 2^(21) / (2048-1024) = 2^21 / 1024 = 2048
	if newImpl.jumpLength != 2048 {
		t.Fatalf("jumpLength mismatch: got %d want %d", newImpl.jumpLength, 2048)
	}
}

func TestJC_Setup_KeyDerivesG_Deterministic(t *testing.T) {
	impl := newJC().(*JC)
	opts := impl.DefaultOptions()

	// Setup without key -> baseline G
	if err := impl.Setup(opts); err != nil {
		t.Fatalf("Setup (no key) error: %v", err)
	}
	baseG := impl.G // copy

	// With key A
	keyA := make([]byte, 32)
	for i := range keyA {
		keyA[i] = byte(i * 7)
	}
	withKey := &chunkers.ChunkerOpts{
		MinSize:    opts.MinSize,
		MaxSize:    opts.MaxSize,
		NormalSize: opts.NormalSize,
		Key:        keyA,
	}
	if err := impl.Setup(withKey); err != nil {
		t.Fatalf("Setup (key A) error: %v", err)
	}
	GA := impl.G
	if reflect.DeepEqual(GA, baseG) {
		t.Fatal("expected G with key to differ from default G")
	}

	// Re-run with the same key -> identical G (deterministic)
	if err := impl.Setup(withKey); err != nil {
		t.Fatalf("Setup (key A again) error: %v", err)
	}
	if !reflect.DeepEqual(GA, impl.G) {
		t.Fatal("expected same key to produce identical G table")
	}

	// With a different key -> different G
	keyB := make([]byte, 32)
	for i := range keyB {
		keyB[i] = byte(i * 3)
	}
	withKeyB := &chunkers.ChunkerOpts{
		MinSize:    opts.MinSize,
		MaxSize:    opts.MaxSize,
		NormalSize: opts.NormalSize,
		Key:        keyB,
	}
	if err := impl.Setup(withKeyB); err != nil {
		t.Fatalf("Setup (key B) error: %v", err)
	}
	if reflect.DeepEqual(GA, impl.G) {
		t.Fatal("expected different key to produce different G table")
	}
}

func Test_generateSpacedMask_and_embedMask(t *testing.T) {
	if got := generateSpacedMask(0, 64); got != 0 {
		t.Errorf("zero ones -> 0, got 0x%x", got)
	}
	if got := generateSpacedMask(64, 64); got != 0xFFFFFFFFFFFFFFFF {
		t.Errorf("all ones -> 0xFFFF.., got 0x%x", got)
	}
	if got := generateSpacedMask(1, 64); got != (1 << 63) {
		t.Errorf("one one -> msb, got 0x%x", got)
	}
	// embedMask
	if embedMask(0) != 0 {
		t.Errorf("embedMask(0) -> 0")
	}
	m := uint64(0b11110000)
	if got := embedMask(m); got != (m & (m - 1)) {
		t.Errorf("embedMask mismatch: got 0b%b want 0b%b", got, m&(m-1))
	}
}

func TestJC_Algorithm_BasicCuts(t *testing.T) {
	impl := newJC().(*JC)
	opts := impl.DefaultOptions()
	if err := impl.Setup(opts); err != nil {
		t.Fatalf("Setup error: %v", err)
	}

	// Case 1: n <= NormalSize -> returns n
	dataSmall := make([]byte, opts.NormalSize-128)
	if cut := impl.Algorithm(opts, dataSmall, len(dataSmall)); cut != len(dataSmall) {
		t.Fatalf("n<=NormalSize: want %d, got %d", len(dataSmall), cut)
	}

	// Case 2: n >= MaxSize -> i in [MinSize..MaxSize]
	dataLarge := make([]byte, opts.MaxSize*2)
	cutLarge := impl.Algorithm(opts, dataLarge, len(dataLarge))
	if cutLarge < opts.MinSize || cutLarge > opts.MaxSize {
		t.Fatalf("n>=MaxSize: cut %d, want in [%d..%d]", cutLarge, opts.MinSize, opts.MaxSize)
	}

	// Case 3: NormalSize < n < MaxSize -> cut in (MinSize..=n]
	nMid := (opts.NormalSize + opts.MaxSize) / 2
	dataMid := make([]byte, nMid)
	cutMid := impl.Algorithm(opts, dataMid, len(dataMid))
	if cutMid <= opts.MinSize || cutMid > len(dataMid) {
		t.Fatalf("Normal<n<Max: cut %d, want in (%d..%d]", cutMid, opts.MinSize, len(dataMid))
	}
}

func TestJC_EndToEndChunking_LegacyAndCurrent(t *testing.T) {
	for _, name := range []string{"jc", "jc-v1.0.0"} {
		data := make([]byte, 100*1024)
		for i := range data {
			data[i] = byte((i * 37) % 251)
		}

		ch, err := chunkers.NewChunker(name, bytes.NewReader(data), nil)
		if err != nil {
			t.Fatalf("[%s] NewChunker error: %v", name, err)
		}

		// Gather chunks; copy each because Next() may reuse internal buffer.
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
				t.Fatalf("[%s] Next error: %v", name, err)
			}
			chunks = append(chunks, append([]byte{}, c...))
		}

		if len(chunks) == 0 {
			t.Fatalf("[%s] no chunks produced", name)
		}

		// Size constraints (allow last < MinSize)
		for i, c := range chunks {
			if i != len(chunks)-1 && len(c) < ch.MinSize() {
				t.Fatalf("[%s] chunk %d too small: %d < %d", name, i, len(c), ch.MinSize())
			}
			if len(c) > ch.MaxSize() {
				t.Fatalf("[%s] chunk %d too large: %d > %d", name, i, len(c), ch.MaxSize())
			}
		}

		// Integrity
		var got []byte
		for _, c := range chunks {
			got = append(got, c...)
		}
		if !bytes.Equal(data, got) {
			t.Fatalf("[%s] reconstructed data != original", name)
		}
	}
}

func TestJC_PropagatesNewKeyedError(t *testing.T) {
	fastCDC := newJC()
	opts := &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		MaxSize:    64 * 1024,
		NormalSize: 8 * 1024,
		Key:        make([]byte, 31), // invalid length
	}

	// Act
	err := fastCDC.Setup(opts)

	// Assert
	if err == nil {
		t.Fatalf("expected Setup to return sentinel NewKeyed error; got %v", err)
	}
}

func TestJC_PropagatesDigestReadError(t *testing.T) {
	// Override only the Read call
	origRead := readDigest
	defer func() { readDigest = origRead }()

	readDigest = func(_ interface{ Read([]byte) (int, error) }, _ []byte) (int, error) {
		return 0, errors.New("sentinel: digest read error")
	}

	fastCDC := newJC()
	opts := &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		MaxSize:    64 * 1024,
		NormalSize: 8 * 1024,
		Key:        make([]byte, 32), // ensure we go through the keyed path
	}

	err := fastCDC.Setup(opts)
	if err == nil || err.Error() != "sentinel: digest read error" {
		t.Fatalf("expected sentinel digest read error, got %v", err)
	}
}

func TestJC_Algorithm_ReturnsAtMaskCZero(t *testing.T) {
	// Construct JC with predictable behavior: fp will be 0 at first step.
	jc := &JC{
		// G defaults to zero-values; G[0] == 0 keeps fp at 0 when data[i]==0
		maskC:      0x1, // any non-zero
		maskJ:      0x1, // any non-zero
		jumpLength: 2,   // irrelevant for this path
	}

	opts := &chunkers.ChunkerOpts{
		MinSize:    4,
		NormalSize: 5,  // ensure n > NormalSize
		MaxSize:    32, // ensure n < MaxSize
	}

	n := 10
	data := make([]byte, n) // all zeros; data[MinSize]==0

	got := jc.Algorithm(opts, data, n)
	want := opts.MinSize // should return i (MinSize) immediately
	if got != want {
		t.Fatalf("expected immediate cut at MinSize=%d, got %d", want, got)
	}
}

func TestJC_Algorithm_JumpBranch(t *testing.T) {
	// Force the jump path on the very first iteration:
	// - maskJ=0 => (fp & maskJ) == 0 is true
	// - maskC=1 and fp==1 => (fp & maskC) != 0 -> triggers jump
	jc := &JC{
		maskC:      0x1,
		maskJ:      0x0,
		jumpLength: 3,
	}
	// Make G[valueAtMinSize] == 1 to set fp to 1 on the first step.
	// Choose value 7; any non-zero index works.
	jc.G[7] = 1

	opts := &chunkers.ChunkerOpts{
		MinSize:    4,
		NormalSize: 5, // ensure we run the loop
		MaxSize:    32,
	}

	// Set n so that after one jump, i == n, causing the function to return n.
	n := opts.MinSize + jc.jumpLength // 4 + 3 = 7
	data := make([]byte, n)
	data[opts.MinSize] = 7 // ensures fp = (0<<1) + G[7] = 1 on first iteration

	got := jc.Algorithm(opts, data, n)
	want := n // after jump, loop ends and returns min(i, n) == n
	if got != want {
		t.Fatalf("expected return %d after jump, got %d", want, got)
	}
}
