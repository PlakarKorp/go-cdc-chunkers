/*
 * Copyright (c) 2024 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package jc

import (
	"encoding/binary"
	"errors"
	"math"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	"github.com/zeebo/blake3"
)

func init() {
	chunkers.Register("jc", newLegacyJC)
	chunkers.Register("jc-v1.0.0", newJC)
}

var readDigest = func(r interface{ Read([]byte) (int, error) }, p []byte) (int, error) {
	return r.Read(p)
}

var errNormalSize = errors.New("NormalSize is required and must be 64B <= NormalSize <= 1GB")
var errMinSize = errors.New("MinSize is required and must be 64B <= MinSize <= 1GB && MinSize < NormalSize")
var errMaxSize = errors.New("MaxSize is required and must be 64B <= MaxSize <= 1GB && MaxSize > NormalSize")

func generateSpacedMask(oneCount int, totalBits int) uint64 {
	if oneCount >= totalBits {
		return 0xFFFFFFFFFFFFFFFF
	}
	if oneCount <= 0 {
		return 0
	}

	step := totalBits / oneCount
	var mask uint64 = 0
	for i := 0; i < oneCount; i++ {
		pos := totalBits - 1 - i*step
		if pos >= 0 {
			mask |= 1 << pos
		}
	}
	return mask
}

func embedMask(maskC uint64) uint64 {
	if maskC == 0 {
		return 0
	}
	// Unset the least significant 1-bit in maskC
	return maskC & (maskC - 1)
}

type JC struct {
	G          [256]uint64
	maskC      uint64
	maskJ      uint64
	jumpLength int

	legacy bool
}

func newLegacyJC() chunkers.ChunkerImplementation {
	return &JC{
		legacy: true,
	}
}

func newJC() chunkers.ChunkerImplementation {
	return &JC{}
}

func (c *JC) Setup(options *chunkers.ChunkerOpts) error {
	bits := uint64(math.Log2(float64(options.NormalSize)))

	cOnes := bits - 1
	jOnes := cOnes - 1
	numerator := 1 << (cOnes + jOnes)
	denominator := (1 << cOnes) - (1 << jOnes)
	c.jumpLength = numerator / denominator

	if c.legacy || (options.MinSize == 2*1024 && options.MaxSize == 64*1024 && options.NormalSize == 8*1024) {
		c.maskC = 0x590003570000
		c.maskJ = 0x590003560000
	} else {
		c.maskC = generateSpacedMask(int(cOnes), 64)
		c.maskJ = embedMask(c.maskC)
	}

	if options.Key == nil {
		c.G = G
	} else {

		hasher, err := blake3.NewKeyed(options.Key)
		if err != nil {
			return err
		}

		bytes := make([]byte, 8)
		for i := range 256 {
			binary.LittleEndian.PutUint64(bytes, G[i])
			hasher.Write(bytes)
		}

		dgst := hasher.Digest()
		digestBytes := make([]byte, 8*256)
		_, err = readDigest(dgst, digestBytes)
		if err != nil {
			return err
		}
		for i := range 256 {
			offset := i * 8
			c.G[i] = binary.LittleEndian.Uint64(digestBytes[offset : offset+8])
		}
	}

	return nil
}

func (c *JC) DefaultOptions() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		MaxSize:    64 * 1024,
		NormalSize: 8 * 1024,
		Key:        nil,
	}
}

func (c *JC) Validate(options *chunkers.ChunkerOpts) error {
	if options.NormalSize == 0 || options.NormalSize < 64 || options.NormalSize > 1024*1024*1024 {
		return errNormalSize
	}
	if options.MinSize < 64 || options.MinSize > 1024*1024*1024 || options.MinSize >= options.NormalSize {
		return errMinSize
	}
	if options.MaxSize < 64 || options.MaxSize > 1024*1024*1024 || options.MaxSize <= options.NormalSize {
		return errMaxSize
	}
	return nil
}

func (c *JC) Algorithm(options *chunkers.ChunkerOpts, data []byte, n int) int {
	MinSize := options.MinSize
	MaxSize := options.MaxSize
	NormalSize := options.NormalSize

	switch {
	case n <= NormalSize:
		return n
	case n >= MaxSize:
		n = MaxSize
	}

	fp := uint64(0)
	i := MinSize

	for i < n {
		fp = (fp << 1) + c.G[data[i]]
		if (fp & c.maskJ) == 0 {
			if (fp & c.maskC) == 0 {
				return i
			}
			fp = 0
			i = i + c.jumpLength
		} else {
			i++
		}
	}
	return min(i, n)
}
