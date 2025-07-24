package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc4stadia"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
	"github.com/PlakarKorp/go-cdc-chunkers/internal/testutil"
	_ "github.com/PlakarKorp/go-cdc-chunkers/internal/testutil" // Import for side effects, to register test data
)

func main() {
	var outputDir string
	flag.StringVar(&outputDir, "dir", ".", "Directory to save the generated profiles")
	flag.Parse()

	chunkersOpts := []chunkers.ChunkerOpts{
		{
			MinSize:    2 * 1024,
			NormalSize: 8 * 1024,
			MaxSize:    32 * 1024,
		},
		{
			MinSize:    4 * 1024,
			NormalSize: 16 * 1024,
			MaxSize:    64 * 1024,
		},
		{
			MinSize:    8 * 1024,
			NormalSize: 32 * 1024,
			MaxSize:    128 * 1024,
		},
		{
			MinSize:    12 * 1024,
			NormalSize: 48 * 1024,
			MaxSize:    192 * 1024,
		},
		{
			MinSize:    16 * 1024,
			NormalSize: 64 * 1024,
			MaxSize:    256 * 1024,
		},
	}
	_ = chunkersOpts // Prevent unused variable warning

	chunkers := []string{
		"fastcdc",
		"fastcdc4stadia",
		"fastcdc-v1.0.0",
		"jc",
		"jc-v1.0.0",
		"ultracdc",
	}
	_ = chunkers // Prevent unused variable warning

	if flag.NArg() == 0 {
		log.Fatal("need file(s) to compute profile from")
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("error creating output directory %s: %v", outputDir, err)
	}

	for _, file := range flag.Args() {
		data, err := os.ReadFile(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error reading file %s: %v, skipping...\n", file, err)
			continue
		}
		_ = data

		for _, chunker := range chunkers {
			for _, chunkerOpts := range chunkersOpts {
				minStr := fmt.Sprintf("%dK", chunkerOpts.MinSize/1024)
				avgStr := fmt.Sprintf("%dK", chunkerOpts.NormalSize/1024)
				maxStr := fmt.Sprintf("%dK", chunkerOpts.MaxSize/1024)

				profileName := fmt.Sprintf("%s_%s-%s-%s:%s.json",
					chunker, minStr, avgStr, maxStr, filepath.Base(file))

				fp, err := os.Create(filepath.Join(outputDir, profileName))
				if err != nil {
					fmt.Fprintf(os.Stderr, "error creating profile file %s: %v, skipping...\n", profileName, err)
					continue
				}
				if profile, err := testutil.GenerateProfile(bytes.NewReader(data), chunker, &chunkerOpts); err != nil {
					fmt.Fprintf(os.Stderr, "error generating chunks for %s: %v, skipping...", chunker, err)
				} else {
					json.NewEncoder(fp).Encode(profile)
				}
				fp.Close()
			}
		}
	}

	//	for _, chunker := range chunkers {
	//		for _, chunkerOpts := range chunkersOpts {
	//			fmt.Printf("Generating profile for %s with options: %+v\n", chunker, chunkerOpts)
	//		}
	//	}
	/*

		if profile, err := testutil.GenerateProfile(os.Stdin, chunker, chunkerOpts); err != nil {
			log.Fatalf("error generating chunks for %s: %v", chunker, err)
		} else {
			json.NewEncoder(os.Stdout).Encode(profile)
		}
	*/

}

/*
func main() {
	var minSize, maxSize, avgSize int
	var chunker string
	var profile string

	flag.IntVar(&minSize, "min", 4*1024, "Minimum chunk size in bytes")
	flag.IntVar(&avgSize, "avg", 16*1024, "Average chunk size in bytes")
	flag.IntVar(&maxSize, "max", 64*1024, "Maximum chunk size in bytes")
	flag.StringVar(&chunker, "chunker", "fastcdc", "Chunking algorithm to use (e.g., fastcdc, fastcdc4stadia, jc, ultracdc)")
	flag.StringVar(&profile, "profile", "", "Path to an existing profile file (optional)")
	flag.Parse()

	chunkerOpts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		MaxSize:    maxSize,
		NormalSize: avgSize,
	}

	if profile == "" {
		if profile, err := testutil.GenerateProfile(os.Stdin, chunker, chunkerOpts); err != nil {
			log.Fatalf("error generating chunks for %s: %v", chunker, err)
		} else {
			json.NewEncoder(os.Stdout).Encode(profile)
		}
	} else {
		file, err := os.Open(profile)
		if err != nil {
			log.Fatalf("error opening profile file %s: %v", profile, err)
		}
		defer file.Close()

		var cdcProfile testutil.CDCProfile
		if err := json.NewDecoder(file).Decode(&cdcProfile); err != nil {
			log.Fatalf("error decoding profile file %s: %v", profile, err)
		}

		err = testutil.CompareProfile(os.Stdin, chunker, chunkerOpts, &cdcProfile)
		if err != nil {
			log.Fatalf("error comparing profile: %v", err)
		} else {
			fmt.Printf("Profile %s matches the generated chunks\n", profile)
		}
	}

}
*/
