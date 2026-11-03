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
	"math/rand"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

// sink keeps the compiler from dropping the call being measured.
var sink int

func benchData(kind string, n int) []byte {
	data := make([]byte, n)
	switch kind {
	case "zeros":
		// already zeroed, and every window matches the one before it, so
		// this is the low-entropy path with its own early exit
	case "seq":
		for i := range data {
			data[i] = byte(i)
		}
	case "random":
		rand.New(rand.NewSource(1)).Read(data)
	case "block8":
		blk := []byte{1, 2, 3, 4, 5, 6, 7, 8}
		for i := range data {
			data[i] = blk[i%8]
		}
	}
	return data
}

// BenchmarkAlgorithm measures the scan on its own, which is where every byte
// of a backup is spent when this chunker is selected.
//
// The fillings pick different paths rather than different data: random bytes
// run the Hamming distance update over every byte, zeros and block8 take the
// repeated-window branch that skips it, and seq never repeats a window.
// The sizes walk the regimes: below MinSize, which returns without looking at
// the data, inside the maskS window, and past NormalSize.
//
// The number to compare is ns/op. A throughput would be misleading, because
// the scan stops at the first cutpoint rather than at the end of the buffer.
func BenchmarkAlgorithm(b *testing.B) {
	opts := &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		NormalSize: 10 * 1024,
		MaxSize:    64 * 1024,
	}
	impl := newUltraCDC().(*UltraCDC)
	if err := impl.Setup(opts); err != nil {
		b.Fatalf("Setup() error = %v", err)
	}

	sizes := []struct {
		name string
		n    int
	}{
		{"1KiB_below_min", 1024},
		{"4KiB_mask_s", 4 * 1024},
		{"16KiB_mask_l", 16 * 1024},
		{"64KiB_max", 64 * 1024},
	}

	for _, kind := range []string{"random", "zeros", "seq", "block8"} {
		for _, size := range sizes {
			data := benchData(kind, size.n)
			b.Run(kind+"/"+size.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sink = impl.Algorithm(opts, data, len(data))
				}
			})
		}
	}
}
