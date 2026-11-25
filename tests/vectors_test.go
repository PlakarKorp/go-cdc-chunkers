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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// This file emits and verifies language-neutral conformance vectors. Unlike the
// Go-only golden test (which fingerprints over the bufio path for refactor
// safety), these vectors are the cross-implementation contract: an independent
// port of fastcdc/jc/ultracdc in any language can regenerate the exact same
// inputs from the recipe, run its own chunker, and check chunk_lengths against
// ours. They are also where the known spec divergences (UltraCDC's cutpoint
// offset, JC's NormalSize tail) become a tested, documented contract rather
// than tribal knowledge — see vectors/README.md.
//
// Regenerate after an intentional behaviour change with:
//
//	go test ./tests/ -run TestVectors -update
const vectorsFile = "vectors/vectors.json"

// vectorOpts mirrors ChunkerOpts in a serialisable, language-neutral form. The
// key is hex so the file is plain ASCII; an empty string means "no key".
type vectorOpts struct {
	MinSize    uint64 `json:"min_size"`
	NormalSize uint64 `json:"normal_size"`
	MaxSize    uint64 `json:"max_size"`
	KeyHex     string `json:"key_hex,omitempty"`
}

// vector is one fully reproducible conformance case. The input is described by
// (inputRecipe, InputLen) rather than stored inline: a port reconstructs the
// bytes from the recipe, so the file stays small and stays ASCII. InputSHA256
// lets a port confirm it built the same bytes before comparing cuts.
type vector struct {
	Algorithm    string     `json:"algorithm"`
	Recipe       string     `json:"input_recipe"`
	InputLen     int        `json:"input_len"`
	InputSHA256  string     `json:"input_sha256"`
	Opts         vectorOpts `json:"opts"`
	ChunkLengths []int      `json:"chunk_lengths"`
	CutsHash     string     `json:"cuts_hash"`
	ContentHash  string     `json:"content_hash"`
}

// recipe names a reproducible byte generator. The construction is documented in
// vectors/README.md so non-Go ports can reproduce it exactly.
type recipe struct {
	name string
	len  int
}

// vectorRecipes spans the entropy regimes that distinguish the algorithms:
// incompressible random (PRNG-seeded so every language agrees), an all-zero
// run (UltraCDC's low-entropy fast path), and a short repeating pattern
// (forces the failure-to-cut tail). Lengths straddle MaxSize so first, middle
// and final-short chunks all occur.
var vectorRecipes = []recipe{
	{name: "random-seed0", len: 200 * 1024},
	{name: "zeros", len: 200 * 1024},
	{name: "repeat-plakar", len: 200 * 1024},
}

// buildRecipe materialises a recipe's bytes. The PRNG is Go's math/rand with a
// fixed seed; ports that cannot reproduce that exact stream should instead pin
// the bytes via InputSHA256 and ship their own equivalent corpus. zeros and
// repeat-plakar are trivially portable.
func buildRecipe(r recipe) []byte {
	b := make([]byte, r.len)
	switch r.name {
	case "random-seed0":
		rnd := rand.New(rand.NewSource(0))
		rnd.Read(b)
	case "zeros":
		// already zero
	case "repeat-plakar":
		pat := []byte("plakar")
		for i := range b {
			b[i] = pat[i%len(pat)]
		}
	default:
		panic("unknown recipe: " + r.name)
	}
	return b
}

// vectorSizeProfiles is a small, portable set of size triples. We keep MaxSize
// modest so the recipes above produce a useful number of chunks without huge
// inputs, and keep NormalSize a power of two so FastCDC's Validate accepts it.
var vectorSizeProfiles = []sizeProfile{
	{name: "2K-8K-64K", min: 2 * 1024, normal: 8 * 1024, max: 64 * 1024},
}

// computeVectors runs every (algorithm, recipe, size) combination through the
// public chunker and returns the vectors keyed by a stable name.
func computeVectors(t *testing.T) map[string]vector {
	t.Helper()
	out := map[string]vector{}
	for _, a := range allAlgorithms {
		for _, sp := range vectorSizeProfiles {
			for _, rc := range vectorRecipes {
				data := buildRecipe(rc)
				opts := optsFor(a, sp)

				ch, err := newBufioChunker(a.name, data, opts)
				if err != nil {
					t.Fatalf("%s/%s/%s: NewChunker: %s", a.name, sp.name, rc.name, err)
				}
				lengths, all, err := collectNext(ch)
				if err != nil {
					t.Fatalf("%s/%s/%s: collect: %s", a.name, sp.name, rc.name, err)
				}
				fp := fingerprintFrom(lengths, all)

				vo := vectorOpts{MinSize: opts.MinSize, NormalSize: opts.NormalSize, MaxSize: opts.MaxSize}
				if len(opts.Key) != 0 {
					vo.KeyHex = hex.EncodeToString(opts.Key)
				}
				inSum := sha256.Sum256(data)

				name := fmt.Sprintf("%s|%s|%s", a.name, sp.name, rc.name)
				out[name] = vector{
					Algorithm:    a.name,
					Recipe:       rc.name,
					InputLen:     len(data),
					InputSHA256:  hex.EncodeToString(inSum[:]),
					Opts:         vo,
					ChunkLengths: lengths,
					CutsHash:     fp.CutsHash,
					ContentHash:  fp.Content,
				}
			}
		}
	}
	return out
}

func TestVectors(t *testing.T) {
	got := computeVectors(t)

	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(vectorsFile), 0o755); err != nil {
			t.Fatalf("mkdir vectors: %s", err)
		}
		buf, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatalf("marshal vectors: %s", err)
		}
		if err := os.WriteFile(vectorsFile, append(buf, '\n'), 0o644); err != nil {
			t.Fatalf("write vectors: %s", err)
		}
		t.Logf("wrote %d conformance vectors to %s", len(got), vectorsFile)
		return
	}

	buf, err := os.ReadFile(vectorsFile)
	if err != nil {
		t.Fatalf("read vectors (regenerate with -update): %s", err)
	}
	var want map[string]vector
	if err := json.Unmarshal(buf, &want); err != nil {
		t.Fatalf("unmarshal vectors: %s", err)
	}

	if len(want) != len(got) {
		t.Fatalf("vector count mismatch: want %d got %d (regenerate with -update)", len(want), len(got))
	}

	names := make([]string, 0, len(got))
	for n := range got {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, n := range names {
		w, ok := want[n]
		if !ok {
			t.Errorf("%s: missing from vectors (regenerate with -update)", n)
			continue
		}
		g := got[n]
		// Compare the cheap, decisive fields first; chunk_lengths is the full
		// contract but cuts_hash collapses it to a single comparison.
		if w.CutsHash != g.CutsHash || w.ContentHash != g.ContentHash || w.InputSHA256 != g.InputSHA256 {
			t.Errorf("%s: vector drift\n  want cuts=%s content=%s\n  got  cuts=%s content=%s",
				n, w.CutsHash, w.ContentHash, g.CutsHash, g.ContentHash)
		}
	}
}
