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
	"math"
	"os"
	"sort"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc4stadia"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
)

// opts groups the size options every subcommand shares.
type opts struct {
	min, avg, max int
}

func (o *opts) register(fs *flag.FlagSet) {
	fs.IntVar(&o.min, "min", 2*1024, "minimum chunk size in bytes")
	fs.IntVar(&o.avg, "avg", 8*1024, "average/normal chunk size in bytes")
	fs.IntVar(&o.max, "max", 64*1024, "maximum chunk size in bytes")
}

func (o *opts) chunkerOpts() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{MinSize: o.min, NormalSize: o.avg, MaxSize: o.max}
}

// chunkStat is the per-chunk record we keep: its length and content digest.
// The digest is what lets us count unique chunks for the dedup ratio.
type chunkStat struct {
	length int
	digest [32]byte
}

// result is the full measurement of running one algorithm over one corpus.
type result struct {
	algorithm string

	totalBytes  int64
	chunks      int
	uniqueBytes int64 // sum of lengths of distinct chunks (by digest)
	uniqueChunk int

	lengths  []int // every chunk length, for the distribution
	duration time.Duration
}

// dedupRatio is uniqueBytes/totalBytes: the fraction of the corpus that must
// actually be stored after deduplication. Lower is better.
func (r *result) dedupRatio() float64 {
	if r.totalBytes == 0 {
		return 0
	}
	return float64(r.uniqueBytes) / float64(r.totalBytes)
}

func (r *result) throughputMBs() float64 {
	if r.duration == 0 {
		return 0
	}
	return float64(r.totalBytes) / 1e6 / r.duration.Seconds()
}

// distribution returns min/p50/avg/p95/max and the standard deviation of the
// chunk lengths. Chunk-size variance matters: a tight distribution around the
// target is a sign of a well-behaved chunker.
func (r *result) distribution() (mn, p50, avg, p95, mx int, stddev float64) {
	if len(r.lengths) == 0 {
		return
	}
	sorted := append([]int(nil), r.lengths...)
	sort.Ints(sorted)
	mn = sorted[0]
	mx = sorted[len(sorted)-1]
	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]

	var sum int64
	for _, l := range sorted {
		sum += int64(l)
	}
	mean := float64(sum) / float64(len(sorted))
	avg = int(mean)

	var sq float64
	for _, l := range sorted {
		d := float64(l) - mean
		sq += d * d
	}
	stddev = math.Sqrt(sq / float64(len(sorted)))
	return
}

// measure chunks every file with the given algorithm, accumulating per-chunk
// digests into seen so the dedup ratio is computed across the whole corpus
// (cross-file dedup, not just within a single file). It returns the aggregate
// result.
func measure(algorithm string, files [][]byte, o *opts) (*result, error) {
	res := &result{algorithm: algorithm}
	seen := make(map[[32]byte]struct{})

	for _, data := range files {
		ch, err := chunkers.NewChunker(algorithm, bytes.NewReader(data), o.chunkerOpts())
		if err != nil {
			return nil, err
		}
		start := time.Now()
		for {
			chunk, err := ch.Next()
			if err != nil && err != io.EOF {
				return nil, err
			}
			if len(chunk) != 0 {
				res.chunks++
				res.totalBytes += int64(len(chunk))
				res.lengths = append(res.lengths, len(chunk))
				d := sha256.Sum256(chunk)
				if _, ok := seen[d]; !ok {
					seen[d] = struct{}{}
					res.uniqueChunk++
					res.uniqueBytes += int64(len(chunk))
				}
			}
			if err == io.EOF {
				break
			}
		}
		res.duration += time.Since(start)
	}
	return res, nil
}

// readFiles loads every path into memory.
func readFiles(paths []string) ([][]byte, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("need at least one input file")
	}
	files := make([][]byte, 0, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		files = append(files, data)
	}
	return files, nil
}

// humanBytes formats a byte count compactly.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
