/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
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

package fastcdc_v1_0_0

import (
	"errors"
	"unsafe"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

func init() {
	chunkers.Register("fastcdc-v1.0.0", newFastCDC)
}

var ErrNormalSize = errors.New("NormalSize is required and must be 64B <= NormalSize <= 1GB")
var ErrMinSize = errors.New("MinSize is required and must be 64B <= MinSize <= 1GB && MinSize < NormalSize")
var ErrMaxSize = errors.New("MaxSize is required and must be 64B <= MaxSize <= 1GB && MaxSize > NormalSize")

func calculateMasks(normalSize, normalLevel int) (maskS, maskL uint64) {
	bits := log2(uint64(normalSize))

	var sBits, lBits uint64
	if normalLevel >= 0 {
		sBits = bits + uint64(normalLevel)
		lBits = bits - uint64(normalLevel)
	} else {
		sBits = bits - uint64(-normalLevel)
		lBits = bits + uint64(-normalLevel)
	}

	maskS = maskForBits(sBits)
	maskL = maskForBits(lBits)
	return
}

func maskForBits(bits uint64) uint64 {
	if bits >= 64 {
		return 0xFFFFFFFFFFFFFFFF
	}
	return (1 << bits) - 1
}

func log2(x uint64) uint64 {
	var n uint64
	for x >>= 1; x != 0; x >>= 1 {
		n++
	}
	return n
}

type FastCDC struct {
	maskS       uint64
	maskL       uint64
	normalLevel int
}

func newFastCDC() chunkers.ChunkerImplementation {
	return &FastCDC{
		normalLevel: 2,
	}
}

func (c *FastCDC) DefaultOptions() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		MaxSize:    64 * 1024,
		NormalSize: 8 * 1024,
	}
}

func (c *FastCDC) Setup(options *chunkers.ChunkerOpts) error {
	NormalLevel := 2
	c.maskS, c.maskL = calculateMasks(options.NormalSize, NormalLevel)

	return nil
}

func (c *FastCDC) Validate(options *chunkers.ChunkerOpts) error {
	if options.NormalSize == 0 || options.NormalSize < 64 || options.NormalSize > 1024*1024*1024 {
		return ErrNormalSize
	}
	if options.MinSize < 64 || options.MinSize > 1024*1024*1024 || options.MinSize >= options.NormalSize {
		return ErrMinSize
	}
	if options.MaxSize < 64 || options.MaxSize > 1024*1024*1024 || options.MaxSize <= options.NormalSize {
		return ErrMaxSize
	}

	if c.normalLevel < 0 || c.normalLevel > 32 {
		return errors.New("NormalLevel must be between 0 and 32")
	}

	return nil
}

func (c *FastCDC) Algorithm(options *chunkers.ChunkerOpts, data []byte, n int) int {
	MinSize := options.MinSize
	MaxSize := options.MaxSize
	NormalSize := options.NormalSize

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
	mask := c.maskS

	p := unsafe.Pointer(&data[i])
	for ; i < n; i++ {
		if i == NormalSize {
			mask = c.maskL
		}
		fp = (fp << 1) + G[*(*byte)(p)]
		if (fp & mask) == 0 {
			return i
		}
		p = unsafe.Pointer(uintptr(p) + 1)
	}
	return i
}
