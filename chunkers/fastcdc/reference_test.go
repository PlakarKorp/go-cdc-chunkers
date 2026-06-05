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

package fastcdc

import (
	"math/rand"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

// referenceAlgorithm is the original, pre-optimisation FastCDC inner loop, kept
// verbatim (minus the unsafe pointer arithmetic, which read one byte past the
// slice on the final iteration). It is the ground truth that the optimised
// Algorithm must match cutpoint-for-cutpoint. Keeping it here lets us prove the
// hoist+unroll is output-preserving independently of the cross-package goldens.
func (c *FastCDC) referenceAlgorithm(options *chunkers.ChunkerOpts, data []byte, n int) int {
	MinSize := options.MinSize
	MaxSize := options.MaxSize
	NormalSize := options.NormalSize

	switch {
	case n <= MinSize:
		return n
	case n >= MaxSize:
		n = MaxSize
	case n <= NormalSize:
		NormalSize = n
	}

	fp := uint64(0)
	i := MinSize
	mask := c.maskS

	for ; i < n; i++ {
		if i == NormalSize {
			mask = c.maskL
		}
		fp = (fp << 1) + c.G[data[i]]
		if (fp & mask) == 0 {
			return i
		}
	}
	return i
}

// TestFastCDCOptimizedMatchesReference proves the optimised Algorithm returns
// the identical cutpoint as the reference loop across a wide range of inputs,
// sizes and the keyed/unkeyed gear tables.
func TestFastCDCOptimizedMatchesReference(t *testing.T) {
	type cfg struct {
		min, normal, max int
	}
	cfgs := []cfg{
		{64, 128, 256},
		{2 * 1024, 8 * 1024, 64 * 1024},
		{2*1024 + 1, 8 * 1024, 64 * 1024}, // min not 8-aligned
		{4 * 1024, 16 * 1024, 64 * 1024},
		{1024, 2048, 4096},
	}

	// A mix of constructors so both gear tables and the legacy/v1 mask paths
	// are exercised.
	makers := map[string]func() chunkers.ChunkerImplementation{
		"v1":     newFastCDC,
		"legacy": newLegacyFastCDC,
	}

	r := rand.New(rand.NewSource(1))
	// Several input fillings: random, all-equal, low-entropy, sequential.
	fillers := map[string]func(n int) []byte{
		"random": func(n int) []byte { b := make([]byte, n); r.Read(b); return b },
		"zeros":  func(n int) []byte { return make([]byte, n) },
		"seq": func(n int) []byte {
			b := make([]byte, n)
			for i := range b {
				b[i] = byte(i)
			}
			return b
		},
	}

	for mname, mk := range makers {
		for _, cf := range cfgs {
			opts := &chunkers.ChunkerOpts{MinSize: cf.min, NormalSize: cf.normal, MaxSize: cf.max}
			impl := mk().(*FastCDC)
			if err := impl.Setup(opts); err != nil {
				t.Fatalf(`%s setup: %s`, mname, err)
			}

			for fname, fill := range fillers {
				// Test many lengths around the boundaries (sub-min, between
				// min and max, past max).
				for _, n := range []int{0, 1, cf.min - 1, cf.min, cf.min + 1, cf.normal, cf.max - 1, cf.max, cf.max + 1, cf.max * 2} {
					if n < 0 {
						continue
					}
					data := fill(n)
					want := impl.referenceAlgorithm(opts, data, len(data))
					got := impl.Algorithm(opts, data, len(data))
					if want != got {
						t.Fatalf(`%s/%s n=%d cfg=%+v: optimized=%d reference=%d`, mname, fname, n, cf, got, want)
					}
				}
			}
		}
	}
}
