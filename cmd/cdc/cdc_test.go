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

package main

import (
	"bytes"
	"math/rand"
	"testing"
)

// buildCorpus makes a base file plus two files that share a large common
// region, so the dedup ratio is meaningfully below 1.
func buildCorpus(t *testing.T) [][]byte {
	t.Helper()
	r := rand.New(rand.NewSource(42))
	common := make([]byte, 512*1024)
	r.Read(common)
	a := append(append([]byte(nil), common...), bytesOf(r, 256*1024)...)
	b := append(append([]byte(nil), common...), bytesOf(r, 256*1024)...)
	return [][]byte{a, b}
}

func bytesOf(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	r.Read(b)
	return b
}

func TestMeasureDedup(t *testing.T) {
	o := &opts{min: 2 * 1024, avg: 8 * 1024, max: 64 * 1024}
	files := buildCorpus(t)
	res, err := measure("fastcdc-v1.0.0", files, o)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	if res.dedupRatio() >= 1.0 || res.dedupRatio() <= 0 {
		t.Fatalf("dedup ratio out of expected range: %f", res.dedupRatio())
	}
	// The 512KiB common prefix should dedup, so unique < total.
	if res.uniqueBytes >= res.totalBytes {
		t.Fatalf("expected dedup (unique %d < total %d)", res.uniqueBytes, res.totalBytes)
	}
}

func TestResyncSharedMonotonic(t *testing.T) {
	o := &opts{min: 2 * 1024, avg: 8 * 1024, max: 64 * 1024}
	r := rand.New(rand.NewSource(99))
	orig := make([]byte, 2*1024*1024)
	r.Read(orig)

	// A single small insertion should leave most of a good chunker's output
	// shared. We assert v1 retains the bulk of its chunks — this is both a
	// smoke test of resyncShared and a guard that v1's resync stays healthy.
	edited := applyInsertions(orig, 1, 1, 1)
	shared, no, ne, err := resyncShared("fastcdc-v1.0.0", orig, edited, o)
	if err != nil {
		t.Fatalf("resyncShared: %v", err)
	}
	if no == 0 || ne == 0 {
		t.Fatalf("expected chunks on both sides, got o=%d e=%d", no, ne)
	}
	if shared < 0.80 {
		t.Fatalf("v1 resync unexpectedly low: %.2f (a single byte edit should localise)", shared)
	}
}

func TestApplyInsertionsGrows(t *testing.T) {
	data := bytes.Repeat([]byte{1}, 1000)
	out := applyInsertions(data, 5, 3, 1)
	if len(out) != len(data)+15 {
		t.Fatalf("expected %d bytes, got %d", len(data)+15, len(out))
	}
}
