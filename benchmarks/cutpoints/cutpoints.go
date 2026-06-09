package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc4stadia"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
)

func main() {
	var minSize, maxSize, avgSize int
	var chunker string

	flag.IntVar(&minSize, "min", 4*1024, "Minimum chunk size in bytes")
	flag.IntVar(&avgSize, "avg", 16*1024, "Average chunk size in bytes")
	flag.IntVar(&maxSize, "max", 64*1024, "Maximum chunk size in bytes")
	flag.StringVar(&chunker, "chunker", "fastcdc", "Chunking algorithm to use (e.g., fastcdc, fastcdc4stadia, jc, ultracdc)")
	flag.Parse()

	if cutpoints, err := generateChunks(os.Stdin, chunker, minSize, avgSize, maxSize); err != nil {
		log.Fatalf("error generating chunks for %s: %v", chunker, err)
	} else {
		for _, cutpoint := range cutpoints {
			fmt.Println(cutpoint)
		}
	}
}

func generateChunks(rd io.Reader, algorithm string, min, avg, max int) ([]int, error) {
	ret := make([]int, 0)

	opts := &chunkers.ChunkerOpts{
		MinSize:    min,
		MaxSize:    max,
		NormalSize: avg,
		Key:        nil,
	}

	chunker, err := chunkers.NewChunker(algorithm, rd, opts)
	if err != nil {
		return nil, err
	}

	firstRun := true
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
		if len(chunk) != 0 || firstRun {
			ret = append(ret, len(chunk))
		}
		if err == io.EOF {
			break
		}
		firstRun = false
	}
	return ret, nil
}
