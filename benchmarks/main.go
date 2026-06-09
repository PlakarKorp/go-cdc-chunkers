package main

import (
	"bytes"
	"flag"
	"fmt"
	"image/color"
	"io"
	"log"
	"math/rand"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc4stadia"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

func randomColor() color.Color {
	return color.RGBA{
		R: uint8(rand.Intn(256)),
		G: uint8(rand.Intn(256)),
		B: uint8(rand.Intn(256)),
		A: 255, // Fully opaque
	}
}

func toXYs(data []int) plotter.XYs {
	pts := make(plotter.XYs, 0)
	for i, val := range data {
		if val == 0 {
			continue
		}
		pts = append(pts, plotter.XY{
			X: float64(i),
			Y: float64(val),
		})
	}
	return pts
}

func main() {
	var minSize, maxSize, avgSize int
	var size int64
	var output string
	flag.Int64Var(&size, "size", 1024<<20, "data size in bytes")
	flag.IntVar(&minSize, "min", 2*1024, "Minimum chunk size in bytes")
	flag.IntVar(&avgSize, "avg", 8*1024, "Average chunk size in bytes")
	flag.IntVar(&maxSize, "max", 64*1024, "Maximum chunk size in bytes")
	flag.StringVar(&output, "output", "", "Output file for the plot")
	flag.Parse()

	var input, _ = io.ReadAll(io.LimitReader(rand.New(rand.NewSource(0)), size))

	p := plot.New()
	p.Title.Text = fmt.Sprintf("min=%d, avg=%d, max=%d", minSize, avgSize, maxSize)
	p.X.Label.Text = "Index"
	p.Y.Label.Text = "Size"
	//p.Y.Scale = plot.LogScale{}
	//p.Y.Tick.Marker = plot.LogTicks{}

	maxPoints := 0
	for _, algorithm := range flag.Args() {
		color := randomColor()
		if points, err := generateChunks(input, algorithm, minSize, avgSize, maxSize); err != nil {
			log.Fatalf("error generating chunks for %s: %v", algorithm, err)
		} else {
			if len(points) > maxPoints {
				maxPoints = len(points)
			}
			point, err := plotter.NewScatter(toXYs(points))
			if err != nil {
				log.Fatalf("failed to create line1: %v", err)
			}
			point.Color = color
			point.Radius = vg.Points(0.1)

			p.Legend.Add(algorithm, point)
			p.Add(point)
		}
	}

	minLine := make([]int, maxPoints)
	for i := range maxPoints {
		minLine[i] = minSize
	}
	lineColor := color.RGBA{
		R: uint8(255),
		G: uint8(0),
		B: uint8(0),
		A: 255, // Fully opaque}
	}
	point, err := plotter.NewScatter(toXYs(minLine))
	if err != nil {
		log.Fatalf("failed to create minline: %v", err)
	}
	point.Color = lineColor
	point.Radius = vg.Points(0.1)
	p.Add(point)

	maxLine := make([]int, maxPoints)
	for i := range maxPoints {
		maxLine[i] = maxSize
	}
	lineColor = color.RGBA{
		R: uint8(255),
		G: uint8(0),
		B: uint8(0),
		A: 255, // Fully opaque}
	}
	point, err = plotter.NewScatter(toXYs(maxLine))
	if err != nil {
		log.Fatalf("failed to create maxline: %v", err)
	}
	point.Color = lineColor
	point.Radius = vg.Points(0.1)
	p.Add(point)

	normalLine := make([]int, maxPoints)
	for i := range maxPoints {
		normalLine[i] = avgSize
	}
	lineColor = color.RGBA{
		R: uint8(0),
		G: uint8(255),
		B: uint8(0),
		A: 255, // Fully opaque}
	}
	point, err = plotter.NewScatter(toXYs(normalLine))
	if err != nil {
		log.Fatalf("failed to create avgline: %v", err)
	}
	point.Color = lineColor
	point.Radius = vg.Points(0.05)
	p.Add(point)

	// Add legend

	p.Legend.Top = true
	p.Legend.XOffs = vg.Points(-20)
	p.Legend.YOffs = vg.Points(-10)
	p.Legend.Padding = vg.Points(2)

	if output == "" {
		output = fmt.Sprintf("cdc-min%d-avg%d-max%d.png", minSize, avgSize, maxSize)
	}

	// Save plot
	if err := p.Save(6*vg.Inch, 4*vg.Inch, output); err != nil {
		log.Fatalf("failed to save plot: %v", err)
	}

}

func generateChunks(input []byte, algorithm string, min, avg, max int) ([]int, error) {
	ret := make([]int, 0)

	opts := &chunkers.ChunkerOpts{
		MinSize:    min,
		MaxSize:    max,
		NormalSize: avg,
		Key:        nil,
	}

	chunker, err := chunkers.NewChunker(algorithm, bytes.NewReader(input), opts)
	if err != nil {
		return nil, err
	}
	for err := error(nil); err == nil; {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			return nil, err
		}
		if len(chunk) < int(chunker.MinSize()) && err != io.EOF {
			return nil, err
		}
		if len(chunk) > int(chunker.MaxSize()) {
			return nil, err
		}
		ret = append(ret, len(chunk))
		if err == io.EOF {
			break
		}
	}
	return ret, nil
}
