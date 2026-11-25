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

package tests

import (
	"bytes"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers/v2"
)

// FuzzChunker holds every registered algorithm to the three invariants that
// must survive any input, at any buffer boundary:
//
//  1. reconstruction: concatenating the chunks reproduces the input exactly;
//  2. size contract: every cutpoint lies in [MinSize, MaxSize], with the sole
//     exception of a final short chunk (the tail the algorithm can't grow);
//  3. buffer-boundary determinism: the cutpoint sequence is identical whether
//     the reader refills from a generous 2*MaxSize buffer or from a minimal
//     MaxSize buffer that forces a compaction on nearly every chunk.
//
// (3) is the invariant that actually catches bugs: a chunker that peeks past
// what the small buffer can hold, or that resets state across a refill, will
// cut differently between the two paths. The fuzzer drives a small fixed size
// profile so MaxSize stays cheap to allocate per call.
func FuzzChunker(f *testing.F) {
	const (
		fzMin    = 256
		fzNormal = 1024
		fzMax    = 4096
	)

	// Seed the corpus with the deterministic battery the golden test uses,
	// trimmed to a few KiB so the fuzzer mutates cheaply. Without `-fuzz`
	// these seeds still run on every `go test`, giving a regression net.
	for _, in := range makeInputs(8 * fzMax) {
		f.Add(in.data)
	}
	f.Add([]byte("plakarplakarplakar"))

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, a := range allAlgorithms {
			opts := &chunkers.ChunkerOpts{MinSize: fzMin, NormalSize: fzNormal, MaxSize: fzMax}
			if a.keyed {
				opts.Key = fixedKey
			}

			// Path A: default (generous) buffer.
			chA, err := chunkers.NewChunker(a.name, bytes.NewReader(data), opts)
			if err != nil {
				t.Fatalf("%s: NewChunker: %s", a.name, err)
			}
			lensA, allA, err := collectNext(chA)
			if err != nil {
				t.Fatalf("%s: collect (default buf): %s", a.name, err)
			}

			// Invariant 1: exact reconstruction.
			if !bytes.Equal(allA, data) {
				t.Fatalf("%s: reconstruction != input (got %d want %d)", a.name, len(allA), len(data))
			}

			// Invariant 2: size contract. A chunk shorter than MinSize is only
			// legal as the final chunk; one longer than MaxSize is never legal.
			for i, l := range lensA {
				if l > fzMax {
					t.Fatalf("%s: chunk %d length %d exceeds MaxSize %d", a.name, i, l, fzMax)
				}
				if l < fzMin && i != len(lensA)-1 {
					t.Fatalf("%s: non-final chunk %d length %d below MinSize %d", a.name, i, l, fzMin)
				}
			}

			// Invariant 3: buffer-boundary determinism. A MaxSize-sized buffer
			// is the smallest NewChunkerBuffer accepts and forces in-place
			// compaction on most chunks, exercising the refill path hardest.
			chB, err := chunkers.NewChunkerBuffer(a.name, bytes.NewReader(data), opts, make([]byte, fzMax))
			if err != nil {
				t.Fatalf("%s: NewChunkerBuffer: %s", a.name, err)
			}
			lensB, _, err := collectNext(chB)
			if err != nil {
				t.Fatalf("%s: collect (min buf): %s", a.name, err)
			}
			if len(lensA) != len(lensB) {
				t.Fatalf("%s: cut count differs across buffer sizes: default=%d min=%d", a.name, len(lensA), len(lensB))
			}
			for i := range lensA {
				if lensA[i] != lensB[i] {
					t.Fatalf("%s: cut %d differs across buffer sizes: default=%d min=%d", a.name, i, lensA[i], lensB[i])
				}
			}
		}
	})
}
