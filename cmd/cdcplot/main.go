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

// Command cdcplot renders graphs about content-defined chunkers, one set per
// implementation: chunk-size distribution, chunk-size CDF, resync impact after
// edits, and dedup ratio vs average size. Each chart shows a single algorithm,
// and the PNGs for an algorithm are written under out/<algo>/.
//
// It lives in its own module so its plotting dependency (gonum.org/v1/plot) is
// never pulled into the chunkers library or the dependency-free cdc CLI. Build
// and run it from this directory:
//
//	cd cmd/cdcplot
//	go run . -kind all -out /tmp/graphs -chunkers fastcdc-v1.0.0,jc,ultracdc FILE...
package main

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"image/color"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc4stadia"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

func main() {
	kind := flag.String("kind", "all", "graph: distribution | resync | dedup-sweep | bars | all")
	out := flag.String("out", ".", "output directory for the PNG(s)")
	list := flag.String("chunkers", "fastcdc-v1.0.0,jc,ultracdc", "comma-separated algorithms")
	minSize := flag.Int("min", 2*1024, "minimum chunk size in bytes")
	avgSize := flag.Int("avg", 8*1024, "average/normal chunk size in bytes")
	maxSize := flag.Int("max", 64*1024, "maximum chunk size in bytes")
	editSize := flag.Int("edit-size", 1, "resync: bytes per insertion")
	flag.Parse()

	files, err := readFiles(flag.Args())
	if err != nil {
		fmt.Fprintf(os.Stderr, "cdcplot: %v\n", err)
		os.Exit(1)
	}
	algos := splitList(*list)
	o := &chunkers.ChunkerOpts{MinSize: *minSize, NormalSize: *avgSize, MaxSize: *maxSize}

	kinds := []string{*kind}
	if *kind == "all" {
		kinds = []string{"distribution", "resync", "dedup-sweep", "count"}
	}

	// Every graph is produced PER implementation: one algorithm per chart,
	// written under out/<algo>/, so each implementation's behaviour stands on
	// its own rather than being overlaid with the others.
	for _, algo := range algos {
		dir := filepath.Join(*out, sanitize(algo))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "cdcplot: %v\n", err)
			os.Exit(1)
		}
		for _, k := range kinds {
			if err := plotOne(k, algo, files, o, *editSize, dir); err != nil {
				fmt.Fprintf(os.Stderr, "cdcplot: %s: %v\n", algo, err)
				os.Exit(1)
			}
		}
	}
}

func plotOne(kind, algo string, files [][]byte, o *chunkers.ChunkerOpts, editSize int, dir string) error {
	switch kind {
	case "distribution":
		return plotDistribution(algo, files, o, dir)
	case "resync":
		return plotResync(algo, files[0], o, editSize, dir)
	case "dedup-sweep":
		return plotDedupSweep(algo, files, dir)
	case "count":
		return plotCount(algo, files, o, dir)
	default:
		return fmt.Errorf("unknown -kind %q", kind)
	}
}

// sanitize makes an algorithm name safe as a directory component.
func sanitize(s string) string {
	return strings.NewReplacer("/", "_", " ", "_").Replace(s)
}

/************ measurement (self-contained, library-only) ************/

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

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// chunkLengths returns every chunk length and the dedup ratio
// (unique-bytes/total) over the corpus.
func chunkLengths(algo string, files [][]byte, o *chunkers.ChunkerOpts) ([]int, float64, error) {
	var lengths []int
	var total, unique int64
	seen := make(map[[32]byte]struct{})
	for _, data := range files {
		ch, err := chunkers.NewChunker(algo, bytes.NewReader(data), o)
		if err != nil {
			return nil, 0, err
		}
		for {
			chunk, err := ch.Next()
			if err != nil && err != io.EOF {
				return nil, 0, err
			}
			if len(chunk) != 0 {
				lengths = append(lengths, len(chunk))
				total += int64(len(chunk))
				d := sha256.Sum256(chunk)
				if _, ok := seen[d]; !ok {
					seen[d] = struct{}{}
					unique += int64(len(chunk))
				}
			}
			if err == io.EOF {
				break
			}
		}
	}
	ratio := 0.0
	if total > 0 {
		ratio = float64(unique) / float64(total)
	}
	return lengths, ratio, nil
}

// resyncShared chunks orig and edited and returns the fraction of the edited
// file's bytes carried by chunks whose digest also appears in the original.
func resyncShared(algo string, orig, edited []byte, o *chunkers.ChunkerOpts) (float64, error) {
	origSet, _, err := digests(algo, orig, o)
	if err != nil {
		return 0, err
	}
	editedSet, editedBytes, err := digests(algo, edited, o)
	if err != nil {
		return 0, err
	}
	var shared int64
	for d, ln := range editedSet {
		if _, ok := origSet[d]; ok {
			shared += int64(ln)
		}
	}
	if editedBytes == 0 {
		return 0, nil
	}
	return float64(shared) / float64(editedBytes), nil
}

func digests(algo string, data []byte, o *chunkers.ChunkerOpts) (map[[32]byte]int, int64, error) {
	ch, err := chunkers.NewChunker(algo, bytes.NewReader(data), o)
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

func applyInsertions(data []byte, n, editSize int) []byte {
	r := rand.New(rand.NewSource(1))
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

/************ plotting ************/

func palette(i int) color.Color {
	cols := []color.RGBA{
		{R: 31, G: 119, B: 180, A: 255},
		{R: 214, G: 39, B: 40, A: 255},
		{R: 44, G: 160, B: 44, A: 255},
		{R: 255, G: 127, B: 14, A: 255},
		{R: 148, G: 103, B: 189, A: 255},
		{R: 23, G: 190, B: 207, A: 255},
	}
	return cols[i%len(cols)]
}

func save(p *plot.Plot, dir, name string) error {
	path := filepath.Join(dir, name)
	if err := p.Save(8*vg.Inch, 5*vg.Inch, path); err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", path)
	return nil
}

func plotDistribution(algo string, files [][]byte, o *chunkers.ChunkerOpts, dir string) error {
	lengths, _, err := chunkLengths(algo, files, o)
	if err != nil {
		return err
	}
	p := plot.New()
	p.Title.Text = fmt.Sprintf("%s — chunk-size distribution (min=%d avg=%d max=%d)",
		algo, o.MinSize, o.NormalSize, o.MaxSize)
	p.X.Label.Text = "chunk size (bytes)"
	p.Y.Label.Text = "count"

	vals := make(plotter.Values, len(lengths))
	for j, l := range lengths {
		vals[j] = float64(l)
	}
	h, err := plotter.NewHist(vals, 60)
	if err != nil {
		return err
	}
	h.FillColor = palette(0)
	h.LineStyle.Width = 0
	p.Add(h)
	return save(p, dir, "chunk-distribution.png")
}

func plotResync(algo string, orig []byte, o *chunkers.ChunkerOpts, editSize int, dir string) error {
	p := plot.New()
	p.Title.Text = fmt.Sprintf("%s — resync quality vs edits (%d-byte insertions)", algo, editSize)
	p.X.Label.Text = "number of insertions"
	p.Y.Label.Text = "shared chunks (% of edited file)"

	steps := []int{0, 1, 2, 4, 8, 16, 32, 64}
	pts := plotter.XYs{}
	for _, e := range steps {
		edited := applyInsertions(orig, e, editSize)
		shared, err := resyncShared(algo, orig, edited, o)
		if err != nil {
			return err
		}
		pts = append(pts, plotter.XY{X: float64(e), Y: 100 * shared})
	}
	line, points, err := plotter.NewLinePoints(pts)
	if err != nil {
		return err
	}
	line.Color = palette(0)
	points.Color = palette(0)
	p.Add(line, points)
	return save(p, dir, "resync-impact.png")
}

func plotDedupSweep(algo string, files [][]byte, dir string) error {
	p := plot.New()
	p.Title.Text = fmt.Sprintf("%s — dedup ratio vs avg chunk size", algo)
	p.X.Label.Text = "avg size (bytes)"
	p.Y.Label.Text = "dedup ratio (lower = better)"

	avgs := []int{4096, 8192, 16384, 32768, 65536}
	pts := plotter.XYs{}
	for _, avg := range avgs {
		o := &chunkers.ChunkerOpts{MinSize: avg / 4, NormalSize: avg, MaxSize: avg * 8}
		_, ratio, err := chunkLengths(algo, files, o)
		if err != nil {
			return err
		}
		pts = append(pts, plotter.XY{X: float64(avg), Y: ratio})
	}
	line, points, err := plotter.NewLinePoints(pts)
	if err != nil {
		return err
	}
	line.Color = palette(0)
	points.Color = palette(0)
	p.Add(line, points)
	p.X.Scale = plot.LogScale{}
	p.X.Tick.Marker = plot.LogTicks{}
	return save(p, dir, "dedup-sweep.png")
}

func plotCount(algo string, files [][]byte, o *chunkers.ChunkerOpts, dir string) error {
	lengths, _, err := chunkLengths(algo, files, o)
	if err != nil {
		return err
	}
	// Cumulative chunk-size CDF: a compact single-algorithm view of how chunk
	// sizes accumulate, complementing the histogram.
	sorted := append([]int(nil), lengths...)
	sort.Ints(sorted)
	pts := make(plotter.XYs, len(sorted))
	for i, l := range sorted {
		pts[i] = plotter.XY{X: float64(l), Y: 100 * float64(i+1) / float64(len(sorted))}
	}
	p := plot.New()
	p.Title.Text = fmt.Sprintf("%s — chunk-size CDF (%d chunks)", algo, len(sorted))
	p.X.Label.Text = "chunk size (bytes)"
	p.Y.Label.Text = "cumulative %"
	line, err := plotter.NewLine(pts)
	if err != nil {
		return err
	}
	line.Color = palette(0)
	p.Add(line)
	return save(p, dir, "chunk-size-cdf.png")
}
