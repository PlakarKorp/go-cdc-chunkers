package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers/v2"
	_ "github.com/PlakarKorp/go-cdc-chunkers/v2/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/v2/chunkers/ultracdc"
)

const (
	datalen = 128 << 22
)

var rb, _ = io.ReadAll(io.LimitReader(rand.New(rand.NewSource(0)), datalen))

// build then: go tool pprof -http=localhost:6060 cpu.prof
func main() {
	var method string
	flag.StringVar(&method, "method", "fastcdc", "chunking method")
	flag.Parse()

	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	r := bytes.NewReader(rb)
	chunker, err := chunkers.NewChunker(method, r, nil)
	if err != nil {
		log.Fatalf(`chunker error: %s`, err)
	}
	for err := error(nil); err == nil; {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			log.Fatalf(`chunker error: %s`, err)
		}
		if len(chunk) < int(chunker.MinSize()) && err != io.EOF {
			log.Fatalf(`chunker return a chunk below MinSize before last chunk: %s`, err)
		}
		if len(chunk) > int(chunker.MaxSize()) {
			log.Fatalf(`chunker return a chunk above MaxSize`)
		}

		fmt.Println("chunk size:", len(chunk))

		if err == io.EOF {
			break
		}
	}

}
