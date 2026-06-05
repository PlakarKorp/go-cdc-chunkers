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
	"io"
	"math/rand"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

// This file is shared scaffolding for the equivalence, golden and fuzz tests.
// It deliberately depends on nothing but the public API so that the very same
// helpers exercise both the legacy bufio path and the new buffer path.

// algoParams describes one registered algorithm and whether it needs a key.
type algoParams struct {
	name  string
	keyed bool
}

// allAlgorithms is the full set of registered algorithms we hold to the
// idempotency contract. kfastcdc is keyed and is fed a fixed key below.
var allAlgorithms = []algoParams{
	{name: "fastcdc"},
	{name: "fastcdc-v1.0.0"},
	{name: "kfastcdc", keyed: true},
	{name: "jc"},
	{name: "jc-v1.0.0"},
	{name: "ultracdc"},
	{name: "fastcdc4stadia"},
}

// fixedKey is a deterministic 32 byte key, so keyed runs are reproducible.
var fixedKey = func() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i*7 + 3)
	}
	return k
}()

// sizeProfile is one (min, normal, max) triple.
type sizeProfile struct {
	name   string
	min    int
	normal int
	max    int
}

// sizeProfiles spans the default profile, a large-chunk profile, and the
// multi-MiB MaxSize regime that the kloset workload actually uses at high
// concurrency. Every profile keeps min < normal < max and a power-of-two
// normal, so FastCDC's Validate accepts it.
var sizeProfiles = []sizeProfile{
	{name: "2K-8K-64K", min: 2 * 1024, normal: 8 * 1024, max: 64 * 1024},
	{name: "256K-512K-1M", min: 256 * 1024, normal: 512 * 1024, max: 1024 * 1024},
	{name: "1M-4M-16M", min: 1024 * 1024, normal: 4 * 1024 * 1024, max: 16 * 1024 * 1024},
}

// optsFor builds the ChunkerOpts for an algorithm/size combination, attaching
// the fixed key for keyed algorithms.
func optsFor(a algoParams, sp sizeProfile) *chunkers.ChunkerOpts {
	opts := &chunkers.ChunkerOpts{
		MinSize:    sp.min,
		NormalSize: sp.normal,
		MaxSize:    sp.max,
	}
	if a.keyed {
		opts.Key = fixedKey
	}
	return opts
}

// inputShape describes one synthetic input and how to materialise it.
type inputShape struct {
	name string
	data []byte
}

// makeInputs builds a deterministic battery of inputs sized relative to the
// largest MaxSize in play so that every boundary (empty, sub-min, exactly
// min/normal/max, just past max, several max) is hit. maxMax is the largest
// MaxSize across the size profiles.
func makeInputs(maxMax int) []inputShape {
	rnd := func(n int) []byte {
		b := make([]byte, n)
		// Fixed seed: goldens must be reproducible across machines.
		r := rand.New(rand.NewSource(0))
		r.Read(b)
		return b
	}
	zeros := func(n int) []byte { return make([]byte, n) }
	repeat := func(n int) []byte {
		// A short repeating pattern: highly compressible, exercises the
		// UltraCDC low-entropy path and FastCDC's failure-to-cut behaviour.
		b := make([]byte, n)
		pat := []byte("plakar")
		for i := range b {
			b[i] = pat[i%len(pat)]
		}
		return b
	}

	shapes := []inputShape{
		{name: "empty", data: []byte{}},
		{name: "one-byte", data: []byte{0x42}},
		{name: "tiny-64", data: rnd(64)},
		{name: "random-3x-maxmax", data: rnd(3 * maxMax)},
		{name: "zeros-2x-maxmax", data: zeros(2 * maxMax)},
		{name: "repeat-2x-maxmax", data: repeat(2 * maxMax)},
	}
	return shapes
}

// collectNext drives a chunker through Next and returns the cutpoint lengths
// and the concatenation of all chunks. It tolerates the io.EOF sentinel the
// same way the package's own tests do.
func collectNext(ch *chunkers.Chunker) (lengths []int, all []byte, err error) {
	for {
		chunk, e := ch.Next()
		if e != nil && e != io.EOF {
			return nil, nil, e
		}
		if len(chunk) != 0 {
			lengths = append(lengths, len(chunk))
			all = append(all, chunk...)
		}
		if e == io.EOF {
			break
		}
	}
	return lengths, all, nil
}

// newBufioChunker builds a chunker over the legacy bufio path (the behaviour
// we are holding everything else equal to).
func newBufioChunker(algorithm string, data []byte, opts *chunkers.ChunkerOpts) (*chunkers.Chunker, error) {
	return chunkers.NewChunker(algorithm, bytes.NewReader(data), opts)
}
