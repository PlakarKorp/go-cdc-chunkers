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
	"errors"
	"math"
	"unsafe"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

func init() {
	chunkers.Register("jc", newJC)
}

var errNormalSize = errors.New("NormalSize is required and must be 64B <= NormalSize <= 1GB")
var errMinSize = errors.New("MinSize is required and must be 64B <= MinSize <= 1GB && MinSize < NormalSize")
var errMaxSize = errors.New("MaxSize is required and must be 64B <= MaxSize <= 1GB && MaxSize > NormalSize")

type JC struct {
	computeJumpLength bool
	jumpLength        int
}

func newJC() chunkers.ChunkerImplementation {
	return &JC{}
}

func (c *JC) Setup(options *chunkers.ChunkerOpts) error {
	return nil
}

func (c *JC) DefaultOptions() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		MaxSize:    64 * 1024,
		NormalSize: 8 * 1024,
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

	const (
		MaskC = uint64(0x590003570000)
		MaskJ = uint64(0x590003560000)
	)

	switch {
	case n <= MinSize:
		return n
	case n >= MaxSize:
		n = MaxSize
	case n <= NormalSize:
		NormalSize = n
	}

	fp := uint64(0)
	i := MinSize

	if c.computeJumpLength {
		cOnes := int(math.Log2(float64(NormalSize))) - 1
		jOnes := cOnes - 1
		c.jumpLength = ((1 << jOnes) * cOnes) / ((1 << cOnes) - (1 << jOnes))
	}

	var p unsafe.Pointer
	for ; i < n; i++ {
		p = unsafe.Pointer(&data[i])
		fp = (fp << 1) + G[*(*byte)(p)]
		if (fp & MaskJ) == 0 {
			if (fp & MaskC) == 0 {
				return i
			}
			fp = 0
			i = i + c.jumpLength
		}
		i++
	}
	if i > n {
		i = n
	}
	return i
}
