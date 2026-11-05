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

// sink keeps the compiler from dropping the call being measured.
var sink int

func benchData(kind string, n int) []byte {
	data := make([]byte, n)
	switch kind {
	case "zeros":
		// already zeroed
	case "seq":
		for i := range data {
			data[i] = byte(i)
		}
	case "random":
		rand.New(rand.NewSource(1)).Read(data)
	}
	return data
}

// BenchmarkAlgorithm measures the inner loop on its own, which is where every
// byte of every backup is spent, over the shapes it actually meets.
//
// The three fillings bracket its cost rather than describe real data: random
// bytes cut early and leave the loop after a few hundred iterations, while
// zeros and a repeating sequence never satisfy the mask and run to the end of
// the window. A regression in the loop body shows up in the second kind well
// before it shows up in a full chunking benchmark, where the reader path
// dilutes it.
//
// The sizes walk the three regimes: below MinSize, which returns without
// looking at the data, inside the maskS window, and past NormalSize so that
// both masks are exercised in one call.
//
// The number to compare is ns/op. A throughput would be misleading here,
// because the loop stops at the first cutpoint rather than at the end of the
// buffer, so the bytes it actually reads depend on the data and not on the
// size of the slice it was handed.
func BenchmarkAlgorithm(b *testing.B) {
	opts := &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		NormalSize: 8 * 1024,
		MaxSize:    64 * 1024,
	}
	impl := newFastCDC().(*FastCDC)
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

	for _, kind := range []string{"random", "zeros", "seq"} {
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

// BenchmarkAlgorithmKeyed is the same loop against a derived gear table
// rather than the shared one, since that is what a keyed repository runs.
func BenchmarkAlgorithmKeyed(b *testing.B) {
	opts := &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		NormalSize: 8 * 1024,
		MaxSize:    64 * 1024,
		Key:        []byte("0123456789abcdef0123456789abcdef"),
	}
	impl := newLegacyKFastCDC().(*FastCDC)
	if err := impl.Setup(opts); err != nil {
		b.Fatalf("Setup() error = %v", err)
	}

	data := benchData("seq", 64*1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink = impl.Algorithm(opts, data, len(data))
	}
}
