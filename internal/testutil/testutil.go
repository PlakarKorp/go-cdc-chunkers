package testutil

import (
	"crypto/sha256"
	"fmt"
	"io"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

type CDCProfile struct {
	Algorithm  string   `json:"algorithm"`
	Keyed      bool     `json:"keyed"`
	MinSize    int      `json:"min_size"`
	NormalSize int      `json:"normal_size"`
	MaxSize    int      `json:"max_size"`
	Cutpoints  []int    `json:"cutpoints"`
	Digests    []string `json:"digests"`
	Digest     string   `json:"digest"`
}

func GenerateProfile(rd io.Reader, algorithm string, opts *chunkers.ChunkerOpts) (*CDCProfile, error) {
	profile := &CDCProfile{
		Algorithm:  algorithm,
		Keyed:      opts.Key != nil,
		MinSize:    opts.MinSize,
		NormalSize: opts.NormalSize,
		MaxSize:    opts.MaxSize,
		Cutpoints:  make([]int, 0),
		Digests:    make([]string, 0),
		Digest:     "",
	}

	chunker, err := chunkers.NewChunker(algorithm, rd, opts)
	if err != nil {
		return nil, err
	}

	globalHasher := sha256.New()
	chunkHasher := sha256.New()

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

		globalHasher.Write(chunk)
		chunkHasher.Reset()
		chunkHasher.Write(chunk)

		if len(chunk) != 0 || firstRun {
			profile.Cutpoints = append(profile.Cutpoints, len(chunk))
			profile.Digests = append(profile.Digests, fmt.Sprintf("%x", chunkHasher.Sum(nil)))
		}
		if err == io.EOF {
			break
		}
		firstRun = false
	}

	profile.Digest = fmt.Sprintf("%x", globalHasher.Sum(nil))

	return profile, nil
}

func CompareProfile(rd io.Reader, algorithm string, opts *chunkers.ChunkerOpts, profile *CDCProfile) error {
	if len(profile.Cutpoints) != len(profile.Digests) {
		return fmt.Errorf("cutpoints and digests length mismatch: %d cutpoints, %d digests", len(profile.Cutpoints), len(profile.Digests))
	}

	chunker, err := chunkers.NewChunker(algorithm, rd, opts)
	if err != nil {
		return err
	}

	globalHasher := sha256.New()
	chunkHasher := sha256.New()

	i := 0
	for err := error(nil); err == nil; i++ {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			return err
		}
		if len(chunk) < int(chunker.MinSize()) && err != io.EOF {
			return fmt.Errorf("chunk size too small: %d < %d", len(chunk), chunker.MinSize())
		}
		if len(chunk) > int(chunker.MaxSize()) {
			return fmt.Errorf("chunk size too large: %d > %d", len(chunk), chunker.MaxSize())
		}

		globalHasher.Write(chunk)
		chunkHasher.Reset()
		chunkHasher.Write(chunk)

		if i >= len(profile.Cutpoints) {
			return fmt.Errorf("cutpoints length mismatch: expected at least %d, got %d", i+1, len(profile.Cutpoints))
		}
		if profile.Cutpoints[i] != len(chunk) && !(len(chunk) == 0 && i == 0) {
			return fmt.Errorf("cutpoint mismatch at index %d: expected %d, got %d", i, profile.Cutpoints[i], len(chunk))
		}
		if profile.Digests[i] != fmt.Sprintf("%x", chunkHasher.Sum(nil)) && !(len(chunk) == 0 && i == 0) {
			return fmt.Errorf("profile digest mismatch at index %d: expected %s, got %s", i, profile.Digests[i], fmt.Sprintf("%x", chunkHasher.Sum(nil)))
		}

		if err == io.EOF {
			break
		}
	}

	if profile.Digest != fmt.Sprintf("%x", globalHasher.Sum(nil)) {
		return fmt.Errorf("profile digest mismatch: expected %s, got %s", profile.Digest, fmt.Sprintf("%x", globalHasher.Sum(nil)))
	}

	return nil
}
