/*
 * Copyright (c) 2026 Gilles Chehade <gilles@poolp.org>
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

package fixed

import (
	"errors"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

func init() {
	chunkers.Register("fixed-v1.0.0", newFixed)
}

var readDigest = func(r interface{ Read([]byte) (int, error) }, p []byte) (int, error) {
	return r.Read(p)
}

var ErrNotPowerOfTwo = errors.New("ChunkSize must be a power of two")
var ErrChunkSize = errors.New("ChunkSize is required and must be 64B <= ChunkSize <= 1GB")

type FixedChunker struct {
}

func newFixed() chunkers.ChunkerImplementation {
	return &FixedChunker{}
}

func (c *FixedChunker) DefaultOptions() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{
		MinSize:    64 * 1024,
		MaxSize:    64 * 1024,
		NormalSize: 64 * 1024,
		Key:        nil,
	}
}

func (c *FixedChunker) Setup(options *chunkers.ChunkerOpts) error {
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

	if err := c.Validate(options); err != nil {
		return err
	}
	return nil
}

func (c *FixedChunker) Validate(options *chunkers.ChunkerOpts) error {
	if options.NormalSize == 0 || options.NormalSize < 64 || options.NormalSize > 1024*1024*1024 {
		return ErrChunkSize
	}
	if (options.NormalSize & (options.NormalSize - 1)) != 0 {
		return ErrNotPowerOfTwo
	}
	return nil
}

func (c *FixedChunker) Algorithm(options *chunkers.ChunkerOpts, data []byte, n int) int {
	size := options.NormalSize

	if size < options.MinSize {
		size = options.MinSize
	}
	if size > options.MaxSize {
		size = options.MaxSize
	}
	if n < size {
		return n
	}
	return size
}
