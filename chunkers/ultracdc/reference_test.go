/*
 * Copyright (c) 2025 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package ultracdc

import (
	"bytes"
	"math/bits"
	"math/rand"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

// referenceAlgorithm is the original UltraCDC inner loop, kept verbatim (two
// table lookups per byte, slice-window loads), as the ground truth the
// single-lookup/resliced Algorithm must match cutpoint-for-cutpoint.
func (c *UltraCDC) referenceAlgorithm(options *chunkers.ChunkerOpts, data []byte, n int) (cutpoint int) {
	const (
		maskS                     uint64 = 0x2F
		maskL                     uint64 = 0x2C
		lowEntropyStringThreshold int    = 64
	)
	minSize := options.MinSize
	maxSize := options.MaxSize
	normalSize := options.NormalSize

	var lowEntropyCount int
	mask := maskS

	switch {
	case n <= minSize:
		return n
	case n >= maxSize:
		n = maxSize
	case n <= normalSize:
		normalSize = n
	}

	outBufWin := data[minSize : minSize+8]
	dist := 0
	for _, v := range outBufWin {
		dist += bits.OnesCount8(v ^ 0xAA)
	}

	var inBufWin []byte
	for i := minSize + 8; i <= n-8; i += 8 {
		if i >= normalSize {
			mask = maskL
		}
		inBufWin = data[i : i+8]
		if bytes.Equal(inBufWin, outBufWin) {
			lowEntropyCount++
			if lowEntropyCount >= lowEntropyStringThreshold {
				return i + 8
			}
			continue
		}
		lowEntropyCount = 0
		for j := 0; j < 8; j++ {
			if (uint64(dist) & mask) == 0 {
				return i + j
			}
			outByte := data[i+j-8]
			inByte := data[i+j]
			update := bits.OnesCount8(inByte^0xAA) - bits.OnesCount8(outByte^0xAA)
			dist += update
		}
		outBufWin = inBufWin
	}
	return n
}

func TestUltraCDCOptimizedMatchesReference(t *testing.T) {
	cfgs := []struct{ min, normal, max int }{
		{64, 128, 256},
		{2 * 1024, 10 * 1024, 64 * 1024},
		{2*1024 + 3, 10 * 1024, 64 * 1024}, // min not 8-aligned
		{4 * 1024, 16 * 1024, 64 * 1024},
		{1024, 4096, 16384},
	}
	impl := newUltraCDC().(*UltraCDC)
	opts0 := impl.DefaultOptions()
	_ = opts0

	r := rand.New(rand.NewSource(5))
	// fill produces a slice of length nn but with capacity cap, mirroring how
	// the chunker is really used: Next always hands Algorithm a window whose
	// backing buffer has capacity >= MaxSize, so the algorithm may reslice the
	// window up to MaxSize even when len(data) < MaxSize. Allocating exactly nn
	// bytes (cap == nn) would violate that contract and is not a real input.
	fillers := map[string]func(nn, cp int) []byte{
		"random": func(nn, cp int) []byte { b := make([]byte, nn, cp); r.Read(b); return b },
		"zeros":  func(nn, cp int) []byte { return make([]byte, nn, cp) },
		"seq": func(nn, cp int) []byte {
			b := make([]byte, nn, cp)
			for i := range b {
				b[i] = byte(i)
			}
			return b
		},
		// low-entropy: repeating 8-byte block exercises the LEST path.
		"block8": func(nn, cp int) []byte {
			b := make([]byte, nn, cp)
			blk := []byte{1, 2, 3, 4, 5, 6, 7, 8}
			for i := range b {
				b[i] = blk[i%8]
			}
			return b
		},
	}

	for _, cf := range cfgs {
		opts := &chunkers.ChunkerOpts{MinSize: cf.min, NormalSize: cf.normal, MaxSize: cf.max}
		for fname, fill := range fillers {
			for _, nn := range []int{0, 1, cf.min - 1, cf.min, cf.min + 1, cf.min + 8, cf.normal, cf.max - 1, cf.max, cf.max + 1, cf.max * 2} {
				if nn < 0 {
					continue
				}
				// Capacity at least MaxSize, as the real caller guarantees.
				cp := nn
				if cp < cf.max {
					cp = cf.max
				}
				data := fill(nn, cp)
				want := impl.referenceAlgorithm(opts, data, len(data))
				got := impl.Algorithm(opts, data, len(data))
				if want != got {
					t.Fatalf(`%s n=%d cfg=%+v: optimized=%d reference=%d`, fname, nn, cf, got, want)
				}
			}
		}
	}
}
