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

package kfastcdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	"github.com/zeebo/blake3"
)

func init() {
	chunkers.Register("kfastcdc", newKFastCDC)
}

var ErrNormalSize = errors.New("NormalSize is required and must be 64B <= NormalSize <= 1GB")
var ErrMinSize = errors.New("MinSize is required and must be 64B <= MinSize <= 1GB && MinSize < NormalSize")
var ErrMaxSize = errors.New("MaxSize is required and must be 64B <= MaxSize <= 1GB && MaxSize > NormalSize")
var ErrKeyRequired = errors.New("key is required")

type KFastCDC struct {
	G [256]uint64
}

func newKFastCDC() chunkers.ChunkerImplementation {
	return &KFastCDC{}
}

func (c *KFastCDC) DefaultOptions() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		MaxSize:    64 * 1024,
		NormalSize: 8 * 1024,
		Key:        nil,
	}
}

func (c *KFastCDC) Setup(options *chunkers.ChunkerOpts) error {
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
	if options.Key == nil {
		return ErrKeyRequired
	}

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
	for i := 0; i < 256; i++ {
		offset := i * 8
		c.G[i] = binary.LittleEndian.Uint64(digestBytes[offset : offset+8])
		fmt.Printf("%x\n", c.G[i])
	}

	return nil
}

func (c *KFastCDC) Validate(options *chunkers.ChunkerOpts) error {
	if options.NormalSize == 0 || options.NormalSize < 64 || options.NormalSize > 1024*1024*1024 {
		return ErrNormalSize
	}
	if options.MinSize < 64 || options.MinSize > 1024*1024*1024 || options.MinSize >= options.NormalSize {
		return ErrMinSize
	}
	if options.MaxSize < 64 || options.MaxSize > 1024*1024*1024 || options.MaxSize <= options.NormalSize {
		return ErrMaxSize
	}
	if options.Key == nil {
		return ErrKeyRequired
	}
	return nil
}

func (c *KFastCDC) Algorithm(options *chunkers.ChunkerOpts, data []byte, n int) int {
	MinSize := options.MinSize
	MaxSize := options.MaxSize
	NormalSize := options.NormalSize

	const (
		MaskS = uint64(0x0003590703530000)
		MaskL = uint64(0x0000d90003530000)
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
	mask := MaskS

	p := unsafe.Pointer(&data[i])
	for ; i < n; i++ {
		if i == NormalSize {
			mask = MaskL
		}
		fp = (fp << 1) + c.G[*(*byte)(p)]
		if (fp & mask) == 0 {
			return i
		}
		p = unsafe.Pointer(uintptr(p) + 1)
	}
	return i
}
