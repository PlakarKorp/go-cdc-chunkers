package testutil

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

type Chunk struct {
	Offset int
	Length int
	Digest []byte
}

type CDCProfile struct {
	Algorithm  string        `json:"algorithm"`
	Keyed      bool          `json:"keyed"`
	MinSize    int           `json:"min_size"`
	NormalSize int           `json:"normal_size"`
	MaxSize    int           `json:"max_size"`
	Chunks     []Chunk       `json:"chunks"`
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
		Chunks:     make([]Chunk, 0),
	}

	t0 := time.Now()
	chunker, err := chunkers.NewChunker(algorithm, rd, opts)
	if err != nil {
		return nil, err
	}

	globalHasher := sha256.New()
	chunkHasher := sha256.New()

	i := 0
	pos := 0
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
			profile.Chunks = append(profile.Chunks, Chunk{
				Offset: pos,
				Length: len(chunk),
				Digest: chunkHasher.Sum(nil),
			})
		}
		pos += len(chunk)
		if err == io.EOF {
			break
		}
	}

	profile.Digest = globalHasher.Sum(nil)
	profile.Duration = time.Since(t0)

	return profile, nil
}

func MatchProfile(rd io.Reader, algorithm string, opts *chunkers.ChunkerOpts, profile *CDCProfile) (*CDCProfile, error) {
	newProfile, err := GenerateProfile(rd, algorithm, opts)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(profile.Chunks); i++ {
		if i >= len(newProfile.Chunks) {
			return nil, fmt.Errorf("cutpoint index %d out of bounds: expected at least %d cutpoints, got %d", i, len(profile.Chunks), len(newProfile.Chunks))
		}
		if profile.Chunks[i].Offset != newProfile.Chunks[i].Offset {
			return nil, fmt.Errorf("cutpoint mismatch at index %d: expected %d, got %d", i, profile.Chunks[i].Offset, newProfile.Chunks[i].Offset)
		}
		if profile.Chunks[i].Length != newProfile.Chunks[i].Length {
			return nil, fmt.Errorf("length mismatch at index %d: expected %d, got %d", i, profile.Chunks[i].Length, newProfile.Chunks[i].Length)
		}
		if !bytes.Equal(profile.Chunks[i].Digest, newProfile.Chunks[i].Digest) {
			return nil, fmt.Errorf("digest mismatch at index %d: expected %x, got %x", i, profile.Chunks[i].Digest, newProfile.Chunks[i].Digest)
		}
	}
	if !bytes.Equal(profile.Digest, newProfile.Digest) {
		return nil, fmt.Errorf("profile digest mismatch: expected %x, got %x", profile.Digest, newProfile.Digest)
	}
	return newProfile, nil
}
