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

package jc

import (
	"math/rand"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

// referenceAlgorithm is the original JC inner loop, kept verbatim, as the
// ground truth the resliced Algorithm must match cutpoint-for-cutpoint.
func (c *JC) referenceAlgorithm(options *chunkers.ChunkerOpts, data []byte, n int) int {
	MinSize := options.MinSize
	MaxSize := options.MaxSize
	NormalSize := options.NormalSize

	switch {
	case n <= NormalSize:
		return n
	case n >= MaxSize:
		n = MaxSize
	}

	fp := uint64(0)
	i := MinSize

	for i < n {
		fp = (fp << 1) + c.G[data[i]]
		if (fp & c.maskJ) == 0 {
			if (fp & c.maskC) == 0 {
				return i
			}
			fp = 0
			i = i + c.jumpLength
		} else {
			i++
		}
	}
	return min(i, n)
}

func TestJCOptimizedMatchesReference(t *testing.T) {
	cfgs := []struct{ min, normal, max int }{
		{64, 128, 256},
		{2 * 1024, 8 * 1024, 64 * 1024},
		{2*1024 + 1, 8 * 1024, 64 * 1024},
		{4 * 1024, 16 * 1024, 64 * 1024},
		{1024, 4096, 16384},
	}
	makers := map[string]func() chunkers.ChunkerImplementation{
		"v1":     newJC,
		"legacy": newLegacyJC,
	}
	r := rand.New(rand.NewSource(3))
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
			impl := mk().(*JC)
			if err := impl.Setup(opts); err != nil {
				t.Fatalf(`%s setup: %s`, mname, err)
			}
			for fname, fill := range fillers {
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
