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
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"math/rand"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

// resync measures the property that actually makes content-defined chunking
// worth using: after a small edit, a good chunker re-synchronises so only the
// chunks near the edit change, and the rest of the file dedups against the
// original. We chunk the original and an edited copy, then measure how much of
// the edited file is covered by chunks the original already had.
//
// This is the test that catches a weak fingerprint combiner even when the
// average chunk size looks right: if cutpoints don't re-align after a shift,
// the shared fraction collapses.
func runResync(args []string) error {
	fs := flag.NewFlagSet("resync", flag.ExitOnError)
	a := fs.String("a", "fastcdc-v1.0.0", "baseline algorithm")
	b := fs.String("b", "fastcdc-v2.0.0", "candidate algorithm")
	edits := fs.Int("edits", 16, "number of random edits to apply")
	editSize := fs.Int("edit-size", 1, "bytes inserted at each edit (insertion shifts the tail)")
	seed := fs.Int64("seed", 1, "PRNG seed for edit positions")
	var o opts
	o.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	paths := fs.Args()
	if len(paths) != 1 {
		return fmt.Errorf("resync takes exactly one file")
	}
	files, err := readFiles(paths)
	if err != nil {
		return err
	}
	orig := files[0]
	edited := applyInsertions(orig, *edits, *editSize, *seed)

	fmt.Printf("file: %s, %d insertion(s) of %d byte(s) => edited %s\n\n",
		humanBytes(int64(len(orig))), *edits, *editSize, humanBytes(int64(len(edited))))

	fmt.Printf("%-16s %12s %12s %12s\n", "algorithm", "shared%", "chunks(o)", "chunks(e)")
	for _, algo := range []string{*a, *b} {
		shared, no, ne, err := resyncShared(algo, orig, edited, &o)
		if err != nil {
			return fmt.Errorf("%s: %w", algo, err)
		}
		fmt.Printf("%-16s %11.2f%% %12d %12d\n", algo, 100*shared, no, ne)
	}

	fmt.Printf("\nshared%% = fraction of the edited file's bytes carried by chunks\n")
	fmt.Printf("identical to ones in the original (higher is better resync/dedup).\n")
	return nil
}

// resyncShared chunks both versions and returns the fraction of the edited
// file's bytes that live in chunks whose digest also appears in the original.
func resyncShared(algo string, orig, edited []byte, o *opts) (shared float64, nOrig, nEdited int, err error) {
	origSet, _, err := chunkDigests(algo, orig, o)
	if err != nil {
		return 0, 0, 0, err
	}
	editedSet, editedBytes, err := chunkDigests(algo, edited, o)
	if err != nil {
		return 0, 0, 0, err
	}

	var sharedBytes int64
	for d, ln := range editedSet {
		if _, ok := origSet[d]; ok {
			sharedBytes += int64(ln) // ln is the (constant) length for this digest
		}
	}
	if editedBytes == 0 {
		return 0, len(origSet), len(editedSet), nil
	}
	return float64(sharedBytes) / float64(editedBytes), len(origSet), len(editedSet), nil
}

// chunkDigests returns the set of distinct chunk digests (mapped to their
// length) and the total bytes chunked. Because content-defined chunks of the
// same digest always have the same length, storing one length per digest is
// exact.
func chunkDigests(algo string, data []byte, o *opts) (map[[32]byte]int, int64, error) {
	ch, err := chunkers.NewChunker(algo, bytes.NewReader(data), o.chunkerOpts())
	if err != nil {
		return nil, 0, err
	}
	set := make(map[[32]byte]int)
	var total int64
	for {
		chunk, err := ch.Next()
		if err != nil && err != io.EOF {
			return nil, 0, err
		}
		if len(chunk) != 0 {
			total += int64(len(chunk))
			set[sha256.Sum256(chunk)] = len(chunk)
		}
		if err == io.EOF {
			break
		}
	}
	return set, total, nil
}

// applyInsertions returns a copy of data with n random single-position
// insertions of editSize bytes. Insertions (rather than overwrites) shift the
// tail, which is the hard case for chunk re-synchronisation.
func applyInsertions(data []byte, n, editSize int, seed int64) []byte {
	r := rand.New(rand.NewSource(seed))
	out := append([]byte(nil), data...)
	for i := 0; i < n; i++ {
		pos := 0
		if len(out) > 0 {
			pos = r.Intn(len(out))
		}
		ins := make([]byte, editSize)
		r.Read(ins)
		out = append(out[:pos], append(ins, out[pos:]...)...)
	}
	return out
}
