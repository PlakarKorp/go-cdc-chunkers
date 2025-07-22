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

package fastcdc

import (
	"encoding/binary"
	"errors"
	"math"
	"unsafe"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	"github.com/zeebo/blake3"
)

func init() {
	chunkers.Register("fastcdc", newLegacyFastCDC)
	chunkers.Register("kfastcdc", newLegacyKFastCDC)
	chunkers.Register("fastcdc-v1.0.0", newFastCDC)
}

var ErrNotPowerOfTwo = errors.New("NormalSize must be a power of two")
var ErrNormalSize = errors.New("NormalSize is required and must be 64B <= NormalSize <= 1GB")
var ErrMinSize = errors.New("MinSize is required and must be 64B <= MinSize <= 1GB && MinSize < NormalSize")
var ErrMaxSize = errors.New("MaxSize is required and must be 64B <= MaxSize <= 1GB && MaxSize > NormalSize")

func calculateMasks(normalSize, normalLevel int) (maskS, maskL uint64) {
	bits := uint64(math.Log2(float64(normalSize)))

	sBits := bits + uint64(normalLevel)
	lBits := bits - uint64(normalLevel)

	// Spread evenly the 1 bits over the entire 64bits word.
	maskS = generateSpacedMask(int(sBits), 64)
	maskL = generateSpacedMask(int(lBits), 64)

	return
}

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

type FastCDC struct {
	G           [256]uint64
	maskS       uint64
	maskL       uint64
	normalLevel int

	keyed  bool
	legacy bool
}

func newFastCDC() chunkers.ChunkerImplementation {
	return &FastCDC{
		normalLevel: 2,
	}
}

func newLegacyFastCDC() chunkers.ChunkerImplementation {
	return &FastCDC{
		normalLevel: 2,
		legacy:      true,
	}
}

func newLegacyKFastCDC() chunkers.ChunkerImplementation {
	return &FastCDC{
		normalLevel: 2,
		keyed:       true,
		legacy:      true,
	}
}

func (c *FastCDC) DefaultOptions() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		MaxSize:    64 * 1024,
		NormalSize: 8 * 1024,
		Key:        nil,
	}
}

func (c *FastCDC) Setup(options *chunkers.ChunkerOpts) error {
	defaultOptions := c.DefaultOptions()
	if options.MinSize == 0 {
		options.MinSize = defaultOptions.MinSize
	}
	if options.MaxSize == 0 {
		options.MaxSize = defaultOptions.MaxSize
	}
	if options.NormalSize == 0 {
		options.NormalSize = defaultOptions.NormalSize
	}

	if c.legacy || (options.MinSize == 2*1024 && options.MaxSize == 64*1024 && options.NormalSize == 8*1024) {
		c.maskS = uint64(0x0003590703530000)
		c.maskL = uint64(0x0000d90003530000)
	} else {
		c.maskS, c.maskL = calculateMasks(options.NormalSize, c.normalLevel)
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
		_, err = dgst.Read(digestBytes)
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
	if (options.NormalSize & (options.NormalSize - 1)) != 0 {
		return ErrNotPowerOfTwo
	}

	if c.normalLevel < 0 || c.normalLevel >= 32 {
		return errors.New("NormalLevel must be between 0 and 31")
	}

	bits := int(math.Log2(float64(options.MinSize)))
	if bits < c.normalLevel {
		return errors.New("NormalSize must be at least 2^NormalLevel")
	}

	if c.keyed && options.Key == nil {
		return errors.New("key is required for keyed FastCDC")
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
		fp = (fp << 1) + c.G[*(*byte)(p)]
		if (fp & mask) == 0 {
			return i
		}
		p = unsafe.Pointer(uintptr(p) + 1)
	}
	return i
}
