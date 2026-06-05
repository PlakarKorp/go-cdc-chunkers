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
	"flag"
	"fmt"
)

func runCompare(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	a := fs.String("a", "fastcdc-v1.0.0", "baseline algorithm")
	b := fs.String("b", "fastcdc-v2.0.0", "candidate algorithm")
	tol := fs.Float64("tol", 0.02, "dedup-ratio regression tolerance (fraction) for exit status")
	var o opts
	o.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	files, err := readFiles(fs.Args())
	if err != nil {
		return err
	}

	ra, err := measure(*a, files, &o)
	if err != nil {
		return fmt.Errorf("%s: %w", *a, err)
	}
	rb, err := measure(*b, files, &o)
	if err != nil {
		return fmt.Errorf("%s: %w", *b, err)
	}

	printComparison(ra, rb)

	// Exit non-zero if the candidate's dedup ratio is worse than the baseline
	// by more than the tolerance — so this is usable as a CI regression gate.
	// (A higher ratio means less dedup, i.e. a regression.)
	if rb.dedupRatio() > ra.dedupRatio()*(1+*tol) {
		return fmt.Errorf("dedup regression: %s ratio %.4f exceeds %s %.4f by more than %.0f%%",
			*b, rb.dedupRatio(), *a, ra.dedupRatio(), *tol*100)
	}
	return nil
}

func printComparison(a, b *result) {
	amn, ap50, aavg, ap95, amx, astd := a.distribution()
	bmn, bp50, bavg, bp95, bmx, bstd := b.distribution()

	delta := func(base, cand float64) string {
		if base == 0 {
			return "  n/a"
		}
		return fmt.Sprintf("%+.1f%%", 100*(cand-base)/base)
	}

	fmt.Printf("corpus: %s\n\n", humanBytes(a.totalBytes))
	fmt.Printf("%-16s %18s %18s %10s\n", "metric", a.algorithm, b.algorithm, "Δ(b vs a)")
	fmt.Printf("%-16s %18.4f %18.4f %10s\n", "dedup ratio", a.dedupRatio(), b.dedupRatio(), delta(a.dedupRatio(), b.dedupRatio()))
	fmt.Printf("%-16s %18d %18d %10s\n", "chunks", a.chunks, b.chunks, delta(float64(a.chunks), float64(b.chunks)))
	fmt.Printf("%-16s %18d %18d %10s\n", "avg size", aavg, bavg, delta(float64(aavg), float64(bavg)))
	fmt.Printf("%-16s %18.0f %18.0f %10s\n", "size stddev", astd, bstd, delta(astd, bstd))
	fmt.Printf("%-16s %18.1f %18.1f %10s\n", "throughput MB/s", a.throughputMBs(), b.throughputMBs(), delta(a.throughputMBs(), b.throughputMBs()))
	fmt.Printf("\n  %s size: min=%d p50=%d p95=%d max=%d\n", a.algorithm, amn, ap50, ap95, amx)
	fmt.Printf("  %s size: min=%d p50=%d p95=%d max=%d\n", b.algorithm, bmn, bp50, bp95, bmx)
}
