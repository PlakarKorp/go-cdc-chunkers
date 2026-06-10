/*
 * Copyright (c) 2026 Gilles Chehade <gilles@poolp.org>
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

// Command cdcbench measures the time, CPU and memory cost of chunking a
// dataset with many concurrent chunkers. It supports two output styles:
//
//   - statistics: a human-readable summary table (text), or machine-readable
//     JSON/CSV for the end-of-run numbers and the captured time series.
//   - plotting:   PNG graphs of memory and CPU usage over time / over files.
//
// Both come from the same run; -plot renders graphs in addition to printing
// stats, and the `plot` subcommand re-renders graphs from a saved JSON series.
//
// Usage:
//
//	cdcbench run  -root DIR [-concurrency N] [-algo fastcdc] [-pooled] \
//	              [-format text|json|csv] [-plot OUTDIR]
//	cdcbench plot -in run.json -out OUTDIR
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"

	"flag"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "plot":
		cmdPlot(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `cdcbench: chunking time/CPU/memory benchmark

  cdcbench run  -root DIR [-concurrency N] [-algo NAME] [-pooled] \
                [-min B] [-avg B] [-max B] [-sample MS] \
                [-format text|json|csv] [-plot OUTDIR]
  cdcbench plot -in run.json -out OUTDIR`)
	os.Exit(2)
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	root := fs.String("root", "", "dataset root to walk (required)")
	conc := fs.Int("concurrency", 100, "number of concurrent worker goroutines")
	algo := fs.String("algo", "fastcdc", "chunker algorithm")
	pooled := fs.Bool("pooled", false, "use NewChunkerBuffer with a pooled per-worker buffer")
	minSize := fs.Int("min", 2*1024, "minimum chunk size in bytes")
	avgSize := fs.Int("avg", 8*1024, "average/normal chunk size in bytes")
	maxSize := fs.Int("max", 64*1024, "maximum chunk size in bytes")
	sampleMS := fs.Int("sample", 100, "time-series sampling interval in milliseconds")
	format := fs.String("format", "text", "stats output: text | json | csv")
	plotDir := fs.String("plot", "", "also render memory/CPU graphs into this directory")
	fs.Parse(args)

	if *root == "" {
		fmt.Fprintln(os.Stderr, "run: -root is required")
		os.Exit(2)
	}

	res, err := Run(BenchConfig{
		Root:        *root,
		Algorithm:   *algo,
		Concurrency: *conc,
		Pooled:      *pooled,
		Opts:        &chunkers.ChunkerOpts{MinSize: *minSize, NormalSize: *avgSize, MaxSize: *maxSize},
		SampleEvery: time.Duration(*sampleMS) * time.Millisecond,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "run:", err)
		os.Exit(1)
	}

	switch *format {
	case "text":
		printText(res)
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(res)
	case "csv":
		printCSV(res)
	default:
		fmt.Fprintln(os.Stderr, "unknown -format:", *format)
		os.Exit(2)
	}

	if *plotDir != "" {
		if err := renderGraphs(*plotDir, []Result{res}, nil); err != nil {
			fmt.Fprintln(os.Stderr, "plot:", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "graphs written to %s\n", *plotDir)
	}
}

func cmdPlot(args []string) {
	fs := flag.NewFlagSet("plot", flag.ExitOnError)
	in := fs.String("in", "", "JSON file(s) produced by `run -format json`, comma-separated (required)")
	out := fs.String("out", ".", "output directory for the PNGs")
	labels := fs.String("labels", "", "comma-separated legend labels, one per -in file (optional)")
	fs.Parse(args)
	if *in == "" {
		fmt.Fprintln(os.Stderr, "plot: -in is required")
		os.Exit(2)
	}

	results, err := loadResults(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, "plot:", err)
		os.Exit(1)
	}
	var labelList []string
	if *labels != "" {
		for _, l := range strings.Split(*labels, ",") {
			labelList = append(labelList, strings.TrimSpace(l))
		}
	}
	if err := renderGraphs(*out, results, labelList); err != nil {
		fmt.Fprintln(os.Stderr, "plot:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "graphs written to %s\n", *out)
}

// printText prints the human-readable summary, matching the style used in the
// project's memory benchmarks.
func printText(r Result) {
	fmt.Printf("mode=%-7s conc=%-4d algo=%-8s files=%d chunks=%d\n",
		r.Mode, r.Concurrency, r.Algorithm, r.Files, r.Chunks)
	fmt.Printf("  time=%.2fs  throughput=%.0f MB/s  data=%.1f MB  cpu=%.1fs\n",
		r.ElapsedSec, r.ThroughputMBs, float64(r.Bytes)/(1<<20), r.CPUSec)
	fmt.Printf("  peakRSS=%.1f MB  heapSys=%.1f MB  totalAlloc=%.1f GB  numGC=%d  samples=%d\n",
		r.PeakRSSMB, r.HeapSysMB, r.TotalAllocGB, r.NumGC, len(r.Samples))
}

// printCSV writes the time series as CSV to stdout (one row per sample).
func printCSV(r Result) {
	fmt.Println("elapsed_sec,files,bytes,chunks,rss_mb,heap_inuse_mb,heap_sys_mb,cpu_sec,num_gc")
	for _, s := range r.Samples {
		fmt.Printf("%.3f,%d,%d,%d,%.2f,%.2f,%.2f,%.3f,%d\n",
			s.ElapsedSec, s.Files, s.Bytes, s.Chunks, s.RSSMB, s.HeapInuseMB, s.HeapSysMB, s.CPUSec, s.NumGC)
	}
}
