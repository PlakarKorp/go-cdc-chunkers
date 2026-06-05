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
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

// updateGolden, when set, rewrites the golden fingerprints from the current
// code instead of asserting against them:
//
//	go test ./tests/ -run TestGolden -update
//
// The fingerprints are the idempotency oracle. They are produced once from the
// reference (bufio) implementation; every later change must reproduce them
// exactly. We do not store full per-chunk profiles (they run to megabytes for
// small-chunk profiles over large inputs); a fingerprint over the cutpoint
// sequence plus the global content digest is just as strict and stays tiny.
var updateGolden = flag.Bool("update", false, "regenerate golden fingerprints")

const goldenFile = "testdata/golden.json"

// fingerprint is the compact, exact signature of one chunking run: the number
// of chunks, a digest over the cutpoint-length sequence, and the digest over
// the reconstructed content. Two runs with the same fingerprint produced the
// same cutpoints over the same bytes.
type fingerprint struct {
	Chunks   int    `json:"chunks"`
	CutsHash string `json:"cuts_hash"`
	Content  string `json:"content"`
}

// fingerprintOf chunks data with the legacy bufio path and returns its
// fingerprint, also asserting that the chunks reconstruct the input exactly.
func fingerprintOf(t *testing.T, algo string, data []byte, opts *chunkers.ChunkerOpts) fingerprint {
	t.Helper()
	ch, err := newBufioChunker(algo, data, opts)
	if err != nil {
		t.Fatalf(`bufio chunker: %s`, err)
	}
	lengths, all, err := collectNext(ch)
	if err != nil {
		t.Fatalf(`bufio collect: %s`, err)
	}
	if !bytes.Equal(all, data) {
		t.Fatalf(`reconstruction != input (got %d want %d)`, len(all), len(data))
	}
	return fingerprintFrom(lengths, all)
}

// fingerprintFrom builds a fingerprint from a cutpoint-length sequence and the
// reconstructed content. It is the single definition of "same output" used by
// both the golden oracle and the differential tests.
func fingerprintFrom(lengths []int, content []byte) fingerprint {
	h := sha256.New()
	var buf [8]byte
	for _, l := range lengths {
		binary.LittleEndian.PutUint64(buf[:], uint64(l))
		h.Write(buf[:])
	}
	c := sha256.Sum256(content)
	return fingerprint{
		Chunks:   len(lengths),
		CutsHash: fmt.Sprintf("%x", h.Sum(nil)),
		Content:  fmt.Sprintf("%x", c[:]),
	}
}

func caseName(algo, size, input string) string {
	return fmt.Sprintf("%s|%s|%s", algo, size, input)
}

func TestGolden(t *testing.T) {
	maxMax := 0
	for _, sp := range sizeProfiles {
		if sp.max > maxMax {
			maxMax = sp.max
		}
	}
	inputs := makeInputs(maxMax)

	got := map[string]fingerprint{}
	for _, a := range allAlgorithms {
		for _, sp := range sizeProfiles {
			for _, in := range inputs {
				name := caseName(a.name, sp.name, in.name)
				got[name] = fingerprintOf(t, a.name, in.data, optsFor(a, sp))
			}
		}
	}

	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(goldenFile), 0o755); err != nil {
			t.Fatalf(`mkdir golden: %s`, err)
		}
		buf, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatalf(`marshal golden: %s`, err)
		}
		if err := os.WriteFile(goldenFile, buf, 0o644); err != nil {
			t.Fatalf(`write golden: %s`, err)
		}
		t.Logf("wrote %d golden fingerprints to %s", len(got), goldenFile)
		return
	}

	want := loadGolden(t)
	if len(want) != len(got) {
		t.Fatalf(`golden case count mismatch: want %d got %d (regenerate with -update)`, len(want), len(got))
	}

	// Sort case names so failures are reported deterministically.
	names := make([]string, 0, len(got))
	for n := range got {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, n := range names {
		w, ok := want[n]
		if !ok {
			t.Errorf(`%s: missing from golden (regenerate with -update)`, n)
			continue
		}
		g := got[n]
		if w != g {
			t.Errorf(`%s: fingerprint drift\n  want %+v\n  got  %+v`, n, w, g)
		}
	}
}

func loadGolden(t *testing.T) map[string]fingerprint {
	t.Helper()
	buf, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatalf(`read golden (regenerate with -update): %s`, err)
	}
	var m map[string]fingerprint
	if err := json.Unmarshal(buf, &m); err != nil {
		t.Fatalf(`unmarshal golden: %s`, err)
	}
	return m
}
