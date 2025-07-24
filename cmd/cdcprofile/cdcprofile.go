package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc4stadia"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
	"github.com/PlakarKorp/go-cdc-chunkers/internal/testutil"
	_ "github.com/PlakarKorp/go-cdc-chunkers/internal/testutil" // Import for side effects, to register test data
)

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

		newProfile, err := testutil.MatchProfile(os.Stdin, chunker, chunkerOpts, &cdcProfile)
		if err != nil {
			log.Fatalf("error comparing profile: %v", err)
		} else {
			delta := newProfile.Duration - cdcProfile.Duration

			var comparison string
			if delta > 0 {
				comparison = "slower"
			} else {
				delta = -delta
				comparison = "faster"
			}
			fmt.Printf("generated chunks matched profile %s %s\n", delta, comparison)
		}
	}

}
