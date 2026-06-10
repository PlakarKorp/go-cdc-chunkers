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

import "io"

// maxConsecutiveEmptyReads bounds how many (0, nil) reads we tolerate from the
// source before giving up with io.ErrNoProgress, matching bufio.Reader.
const maxConsecutiveEmptyReads = 100

// bufReader is a minimal buffered reader tailored to the chunker's access
// pattern: it only needs to peek a window of up to a fixed maximum and then
// discard a prefix once a cut-point is chosen. Unlike bufio.Reader, its
// backing buffer can be supplied by the caller, which lets callers pool and
// reuse it across many chunkers (e.g. one buffer per worker goroutine) instead
// of paying a fresh allocation per chunker.
//
// The buffer must be at least MaxSize bytes: a single peek never needs more
// than MaxSize, and the reader compacts in place rather than requiring the
// extra headroom that bufio.Reader needs to avoid frequent memmoves.
type bufReader struct {
	src io.Reader
	buf []byte
	r   int // read position: unconsumed data is buf[r:w]
	w   int // write position
	err error
}

func (cr *bufReader) reset(src io.Reader, buf []byte) {
	cr.src = src
	cr.buf = buf
	cr.r = 0
	cr.w = 0
	cr.err = nil
}

// peek returns up to n bytes of buffered data without consuming them, reading
// from the source as needed. It returns fewer than n bytes only at EOF (or on
// a read error, which is reported once the buffered data is exhausted). n must
// be <= len(cr.buf).
func (cr *bufReader) peek(n int) ([]byte, error) {
	// Fill until we have n bytes buffered or the source is drained.
	for cr.w-cr.r < n && cr.err == nil {
		// No room at the tail but reclaimable space at the front: slide the
		// unconsumed window down to offset 0 so the read can proceed. With a
		// buffer of exactly MaxSize this is what lets a full peek(MaxSize)
		// always succeed.
		if cr.w == len(cr.buf) && cr.r > 0 {
			cr.w = copy(cr.buf, cr.buf[cr.r:cr.w])
			cr.r = 0
		}
		// Guard against a pathological reader that returns (0, nil) forever,
		// mirroring bufio.Reader's behaviour.
		for tries := maxConsecutiveEmptyReads; ; tries-- {
			m, err := cr.src.Read(cr.buf[cr.w:])
			cr.w += m
			if err != nil {
				cr.err = err
				break
			}
			if m > 0 {
				break
			}
			if tries <= 1 {
				cr.err = io.ErrNoProgress
				break
			}
		}
	}

	avail := cr.w - cr.r
	if avail > n {
		avail = n
	}
	window := cr.buf[cr.r : cr.r+avail]
	if avail < n {
		// Not enough data: surface the pending error (EOF or otherwise).
		err := cr.err
		if err == nil {
			err = io.EOF
		}
		return window, err
	}
	return window, nil
}

// discard advances the read position by k bytes already returned by peek.
func (cr *bufReader) discard(k int) {
	cr.r += k
	if cr.r == cr.w {
		// Window emptied: rewind to the front so the next peek has the whole
		// buffer available without a compaction.
		cr.r = 0
		cr.w = 0
	}
}
