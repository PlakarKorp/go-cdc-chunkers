package chunkers

/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
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

import (
	"errors"
	"io"
)

type ChunkerOpts struct {
	MinSize    int
	MaxSize    int
	NormalSize int
	Key        []byte
}

type ChunkerImplementation interface {
	DefaultOptions() *ChunkerOpts
	Setup(*ChunkerOpts) error
	Validate(*ChunkerOpts) error
	Algorithm(*ChunkerOpts, []byte, int) int
}

type Chunker struct {
	rd             bufReader
	options        *ChunkerOpts
	implementation ChunkerImplementation

	cutpoint int
	isFirst  bool
}

func (c *Chunker) MinSize() int {
	return c.options.MinSize
}

func (c *Chunker) MaxSize() int {
	return c.options.MaxSize
}

func (c *Chunker) NormalSize() int {
	return c.options.NormalSize
}

var chunkers map[string]func() ChunkerImplementation = make(map[string]func() ChunkerImplementation)

func Register(name string, implementation func() ChunkerImplementation) error {
	if _, exists := chunkers[name]; exists {
		return errors.New("algorithm already registered")
	}
	chunkers[name] = implementation
	return nil
}

// ErrBufferTooSmall is returned by NewChunkerBuffer when the supplied buffer
// is smaller than the minimum the chunker needs (MaxSize bytes).
var ErrBufferTooSmall = errors.New("buffer must be at least MaxSize bytes")

func newChunker(algorithm string, opts *ChunkerOpts) (*Chunker, error) {
	implementationAllocator, exists := chunkers[algorithm]
	if !exists {
		return nil, errors.New("unknown algorithm")
	}

	// Allocate the implementation once and read its defaults from the same
	// instance; a second throwaway allocation here would be wasteful since
	// some implementations carry a sizeable table (e.g. FastCDC's 256-entry
	// Gear array).
	implementation := implementationAllocator()
	defaultOpts := implementation.DefaultOptions()

	if opts == nil {
		opts = defaultOpts
	} else {
		if opts.MinSize == 0 {
			opts.MinSize = defaultOpts.MinSize
		}
		if opts.MaxSize == 0 {
			opts.MaxSize = defaultOpts.MaxSize
		}
		if opts.NormalSize == 0 {
			opts.NormalSize = defaultOpts.NormalSize
		}
	}

	chunker := &Chunker{}
	chunker.isFirst = true
	chunker.implementation = implementation
	chunker.options = opts

	if err := chunker.implementation.Setup(chunker.options); err != nil {
		return nil, err
	}

	return chunker, nil
}

// defaultBufferFactor is the multiple of MaxSize used for the internally
// allocated scan buffer. A buffer of exactly MaxSize is correct but forces an
// in-place compaction on most chunks; 2*MaxSize lets the reader refill in
// larger strides and measures ~20% faster on random data, at the cost of
// twice the per-chunker footprint. Callers that care more about memory than
// throughput can pass a MaxSize-sized buffer to NewChunkerBuffer instead.
const defaultBufferFactor = 2

// NewChunker returns a Chunker that reads from reader using the named
// algorithm. It allocates an internal scan buffer (defaultBufferFactor*MaxSize)
// sized for throughput. Callers running many concurrent chunkers that want to
// own or pool that buffer — to bound peak memory rather than pay a fresh
// allocation per chunker — should use NewChunkerBuffer instead.
func NewChunker(algorithm string, reader io.Reader, opts *ChunkerOpts) (*Chunker, error) {
	chunker, err := newChunker(algorithm, opts)
	if err != nil {
		return nil, err
	}
	chunker.rd.reset(reader, make([]byte, chunker.options.MaxSize*defaultBufferFactor))
	return chunker, nil
}

// NewChunkerBuffer is like NewChunker but uses buf as the scan buffer instead
// of allocating one. buf must be at least MaxSize bytes (after option
// defaulting); otherwise ErrBufferTooSmall is returned. The chunker retains buf
// for its lifetime and the slices returned by Next alias it, so buf must not be
// mutated or reused until the chunker is done with it (or Reset onto a new
// stream). This lets callers reuse a single buffer per worker goroutine — for
// example via a sync.Pool — so peak memory scales with concurrency rather than
// with the number of chunkers created.
func NewChunkerBuffer(algorithm string, reader io.Reader, opts *ChunkerOpts, buf []byte) (*Chunker, error) {
	chunker, err := newChunker(algorithm, opts)
	if err != nil {
		return nil, err
	}
	if len(buf) < chunker.options.MaxSize {
		return nil, ErrBufferTooSmall
	}
	chunker.rd.reset(reader, buf)
	return chunker, nil
}

func (chunker *Chunker) Reset(reader io.Reader) {
	chunker.cutpoint = 0
	chunker.isFirst = true
	chunker.rd.reset(reader, chunker.rd.buf)
}

func (chunker *Chunker) Next() ([]byte, error) {
	if chunker.cutpoint != 0 {
		chunker.rd.discard(chunker.cutpoint)
		chunker.cutpoint = 0
	}

	data, err := chunker.rd.peek(chunker.options.MaxSize)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if chunker.isFirst {
		defer func() { chunker.isFirst = false }()
	}

	n := len(data)
	if n == 0 {
		if chunker.isFirst {
			return []byte{}, io.EOF
		}
		return nil, io.EOF
	}

	cutpoint := chunker.implementation.Algorithm(chunker.options, data, n)
	chunker.cutpoint = cutpoint

	if cutpoint < chunker.options.MinSize {
		return data[:cutpoint], io.EOF
	}

	return data[:cutpoint], nil
}

func (chunker *Chunker) Copy(dst io.Writer) (int64, error) {
	nbytes := int64(0)
	for {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			return nbytes, err
		}

		if len(chunk) != 0 {
			if _, werr := dst.Write(chunk); werr != nil {
				return nbytes, werr
			}
			nbytes += int64(len(chunk))
		}

		if err == io.EOF {
			break
		}
	}
	return nbytes, io.EOF
}

func (chunker *Chunker) Split(callback func(offset, length uint, chunk []byte) error) error {
	offset := uint(0)
	for {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			return err
		}

		if err := callback(offset, uint(len(chunk)), chunk); err != nil {
			return err
		}

		if err == io.EOF {
			break
		}
		offset += uint(len(chunk))
	}
	return nil
}
