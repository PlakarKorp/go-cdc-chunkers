package main

import (
	"bufio"
	"fmt"
	"image/color"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

type Benchmark struct {
	Name       string
	NsPerOp    float64
	Throughput float64
	Chunks     float64
}

func parseLine(line string) *Benchmark {
	re := regexp.MustCompile(`^Benchmark_(\S+)\s+\d+\s+(\d+)\s+ns/op\s+([\d.]+)\s+MB/s\s+(\d+)\s+chunks`)
	matches := re.FindStringSubmatch(line)
	if len(matches) != 5 {
		return nil
	}

	nsPerOp, _ := strconv.ParseFloat(matches[2], 64)
	throughput, _ := strconv.ParseFloat(matches[3], 64)
	chunks, _ := strconv.ParseFloat(matches[4], 64)

	return &Benchmark{
		Name:       matches[1],
		NsPerOp:    nsPerOp,
		Throughput: throughput,
		Chunks:     chunks,
	}
}

func plotBar(data []Benchmark, valueSelector func(Benchmark) float64, title, ylabel, filename string) {
	sort.Slice(data, func(i, j int) bool {
		return valueSelector(data[i]) < valueSelector(data[j])
	})

	values := make(plotter.Values, len(data))
	labels := make([]string, len(data))
	for i, b := range data {
		values[i] = valueSelector(b)
		labels[i] = strings.TrimSuffix(strings.TrimPrefix(b.Name, "Benchmark_"), "-14")
	}

	p := plot.New()
	p.Title.Text = title
	p.Y.Label.Text = ylabel

	bar, err := plotter.NewBarChart(values, vg.Points(16))
	if err != nil {
		log.Fatalf("failed to create bar chart: %v", err)
	}
	bar.LineStyle.Width = vg.Length(0)
	bar.Color = color.RGBA{R: 0, G: 128, B: 255, A: 255}
	bar.Offset = vg.Points(0)

	p.Add(bar)
	p.NominalX(labels...)

	if err := p.Save(16*vg.Inch, 6*vg.Inch, filename); err != nil {
		log.Fatalf("failed to save %s: %v", filename, err)
	}
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	var benchmarks []Benchmark

	for scanner.Scan() {
		line := scanner.Text()
		if b := parseLine(line); b != nil {
			benchmarks = append(benchmarks, *b)
		}
	}

	if len(benchmarks) == 0 {
		fmt.Println("No valid benchmark lines found.")
		return
	}

	plotBar(benchmarks, func(b Benchmark) float64 { return b.NsPerOp }, "Benchmark ns/op", "ns/op", "ns_per_op.png")
	plotBar(benchmarks, func(b Benchmark) float64 { return b.Throughput }, "Benchmark Throughput", "MB/s", "throughput.png")
	plotBar(benchmarks, func(b Benchmark) float64 { return b.Chunks }, "Benchmark Chunk Count", "Chunks", "chunks.png")

	fmt.Println("Generated: ns_per_op.png, throughput.png, chunks.png")
}
