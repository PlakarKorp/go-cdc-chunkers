package testutil

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

type CDCProfile struct {
	Algorithm  string        `json:"algorithm"`
	Keyed      bool          `json:"keyed"`
	MinSize    int           `json:"min_size"`
	NormalSize int           `json:"normal_size"`
	MaxSize    int           `json:"max_size"`
	Cutpoints  []int         `json:"cutpoints"`
	Digests    [][]byte      `json:"digests"`
	Digest     []byte        `json:"digest"`
	Duration   time.Duration `json:"duration"`
}

func GenerateProfile(rd io.Reader, algorithm string, opts *chunkers.ChunkerOpts) (*CDCProfile, error) {
	profile := &CDCProfile{
		Algorithm:  algorithm,
		Keyed:      opts.Key != nil,
		MinSize:    opts.MinSize,
		NormalSize: opts.NormalSize,
		MaxSize:    opts.MaxSize,
		Cutpoints:  make([]int, 0, 1024),
		Digests:    make([][]byte, 0, 1024),
	}

	t0 := time.Now()
	chunker, err := chunkers.NewChunker(algorithm, rd, opts)
	if err != nil {
		return nil, err
	}

	globalHasher := sha256.New()
	chunkHasher := sha256.New()

	i := 0
	for err := error(nil); err == nil; i++ {
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

		chunkHasher.Reset()
		chunkHasher.Write(chunk)

		globalHasher.Write(chunk)

		if len(chunk) != 0 || i == 0 {
			profile.Cutpoints = append(profile.Cutpoints, len(chunk))
			profile.Digests = append(profile.Digests, chunkHasher.Sum(nil))
		}
		if err == io.EOF {
			break
		}
	}

	profile.Digest = globalHasher.Sum(nil)
	profile.Duration = time.Since(t0)

	return profile, nil
}

func MatchProfile(rd io.Reader, algorithm string, opts *chunkers.ChunkerOpts, profile *CDCProfile) (*CDCProfile, error) {
	if len(profile.Cutpoints) != len(profile.Digests) {
		return nil, fmt.Errorf("cutpoints and digests length mismatch: %d cutpoints, %d digests", len(profile.Cutpoints), len(profile.Digests))
	}

	newProfile, err := GenerateProfile(rd, algorithm, opts)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(profile.Cutpoints); i++ {
		if i >= len(newProfile.Cutpoints) {
			return nil, fmt.Errorf("cutpoint index %d out of bounds: expected at least %d cutpoints, got %d", i, len(profile.Cutpoints), len(newProfile.Cutpoints))
		}
		if profile.Cutpoints[i] != newProfile.Cutpoints[i] {
			return nil, fmt.Errorf("cutpoint mismatch at index %d: expected %d, got %d", i, profile.Cutpoints[i], newProfile.Cutpoints[i])
		}
		if !bytes.Equal(profile.Digests[i], newProfile.Digests[i]) {
			return nil, fmt.Errorf("digest mismatch at index %d: expected %x, got %x", i, profile.Digests[i], newProfile.Digests[i])
		}
	}
	if !bytes.Equal(profile.Digest, newProfile.Digest) {
		return nil, fmt.Errorf("profile digest mismatch: expected %x, got %x", profile.Digest, newProfile.Digest)
	}
	return newProfile, nil
}
