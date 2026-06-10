package main

import (
	"encoding/json"
	"fmt"
	"image/color"
	"os"
	"path/filepath"
	"strings"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

// loadResults reads one or more JSON results (comma-separated paths). Each gets
// a label derived from its mode/concurrency so overlaid series are legible.
func loadResults(spec string) ([]Result, error) {
	var out []Result
	for _, p := range strings.Split(spec, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var r Result
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		out = append(out, r)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no results loaded")
	}
	return out, nil
}

// labelsFor returns a display label per result. Explicit labels (from -labels)
// win; otherwise one is derived from algorithm/mode/concurrency.
func labelsFor(results []Result, explicit []string) []string {
	out := make([]string, len(results))
	for i, r := range results {
		if i < len(explicit) && explicit[i] != "" {
			out[i] = explicit[i]
		} else {
			out[i] = fmt.Sprintf("%s/%s c=%d", r.Algorithm, r.Mode, r.Concurrency)
		}
	}
	return out
}

var palette = []color.Color{
	color.RGBA{R: 0x1f, G: 0x77, B: 0xb4, A: 255},
	color.RGBA{R: 0xff, G: 0x7f, B: 0x0e, A: 255},
	color.RGBA{R: 0x2c, G: 0xa0, B: 0x2c, A: 255},
	color.RGBA{R: 0xd6, G: 0x27, B: 0x28, A: 255},
	color.RGBA{R: 0x94, G: 0x67, B: 0xbd, A: 255},
	color.RGBA{R: 0x8c, G: 0x56, B: 0x4b, A: 255},
}

// series extracts X/Y points from each result's samples using the given
// accessors, returning a plotter line+points per result.
type accessor func(Sample) float64

func addSeries(p *plot.Plot, results []Result, labels []string, x, y accessor) error {
	for i, r := range results {
		pts := make(plotter.XYs, len(r.Samples))
		for j, s := range r.Samples {
			pts[j].X = x(s)
			pts[j].Y = y(s)
		}
		line, err := plotter.NewLine(pts)
		if err != nil {
			return err
		}
		line.Color = palette[i%len(palette)]
		line.Width = vg.Points(1.5)
		p.Add(line)
		p.Legend.Add(labels[i], line)
	}
	p.Legend.Top = true
	return nil
}

// renderGraphs writes the memory-over-time, CPU-over-time, memory-over-files
// and throughput-over-time PNGs comparing all supplied results. labels, if
// non-empty, names each series in the legend (one per result).
func renderGraphs(dir string, results []Result, labels []string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	legend := labelsFor(results, labels)

	elapsed := func(s Sample) float64 { return s.ElapsedSec }
	filesDone := func(s Sample) float64 { return float64(s.Files) }

	graphs := []struct {
		file   string
		title  string
		xLabel string
		yLabel string
		x, y   accessor
	}{
		{
			"memory-over-time.png", "Memory usage over time",
			"elapsed (s)", "RSS (MB)",
			elapsed, func(s Sample) float64 { return s.RSSMB },
		},
		{
			"heap-over-time.png", "Live heap over time",
			"elapsed (s)", "heap in-use (MB)",
			elapsed, func(s Sample) float64 { return s.HeapInuseMB },
		},
		{
			"cpu-over-time.png", "Cumulative CPU time over wall-clock time",
			"elapsed (s)", "CPU (s)",
			elapsed, func(s Sample) float64 { return s.CPUSec },
		},
		{
			"memory-over-files.png", "Memory usage as files are processed",
			"files processed", "RSS (MB)",
			filesDone, func(s Sample) float64 { return s.RSSMB },
		},
		{
			"throughput-over-time.png", "Data processed over time",
			"elapsed (s)", "data (MB)",
			elapsed, func(s Sample) float64 { return float64(s.Bytes) / (1 << 20) },
		},
	}

	for _, g := range graphs {
		p := plot.New()
		p.Title.Text = g.title
		p.X.Label.Text = g.xLabel
		p.Y.Label.Text = g.yLabel
		if err := addSeries(p, results, legend, g.x, g.y); err != nil {
			return err
		}
		if err := p.Save(7*vg.Inch, 4*vg.Inch, filepath.Join(dir, g.file)); err != nil {
			return fmt.Errorf("%s: %w", g.file, err)
		}
	}
	return nil
}
