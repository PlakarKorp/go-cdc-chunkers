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

func runAnalyze(args []string) error {
	fs := flag.NewFlagSet("analyze", flag.ExitOnError)
	chunker := fs.String("chunker", "fastcdc-v1.0.0", "chunking algorithm")
	var o opts
	o.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	files, err := readFiles(fs.Args())
	if err != nil {
		return err
	}

	res, err := measure(*chunker, files, &o)
	if err != nil {
		return err
	}
	printResult(res)
	return nil
}

func printResult(r *result) {
	mn, p50, avg, p95, mx, stddev := r.distribution()
	fmt.Printf("algorithm:   %s\n", r.algorithm)
	fmt.Printf("input:       %s in %d chunk(s)\n", humanBytes(r.totalBytes), r.chunks)
	fmt.Printf("dedup ratio: %.4f (%s unique of %s; %.2f%% saved)\n",
		r.dedupRatio(), humanBytes(r.uniqueBytes), humanBytes(r.totalBytes),
		100*(1-r.dedupRatio()))
	fmt.Printf("chunk size:  min=%d p50=%d avg=%d p95=%d max=%d stddev=%.0f\n",
		mn, p50, avg, p95, mx, stddev)
	fmt.Printf("throughput:  %.1f MB/s\n", r.throughputMBs())
}
