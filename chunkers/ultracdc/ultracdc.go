/*
 * Copyright (c) 2024 Jason E. Aten, Ph.D.
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

package ultracdc

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

var _ = fmt.Printf

func init() {
	chunkers.Register("ultracdc", newUltraCDC)
}

var ErrNormalSize = errors.New("NormalSize is required and must be 64B <= NormalSize <= 1GB")
var ErrMinSize = errors.New("MinSize is required and must be 64B <= MinSize <= 1GB && MinSize < NormalSize")
var ErrMaxSize = errors.New("MaxSize is required and must be 64B <= MaxSize <= 1GB && MaxSize > NormalSize")

type UltraCDC struct {
	Opts *chunkers.ChunkerOpts
}

// NewUltraCDC is for non-Plakar standalone clients. Plakar
// clients will use newUltraCDC via the
// chunkers.NewChunker("ultracdc", ...) factory.
func NewUltraCDC() *UltraCDC {
	u := &UltraCDC{}
	u.Opts = u.DefaultOptions()
	return u
}

func newUltraCDC() chunkers.ChunkerImplementation {
	return &UltraCDC{}
}

func (c *UltraCDC) DefaultOptions() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		NormalSize: 10 * 1024,
		MaxSize:    64 * 1024,
	}
}

func (c *UltraCDC) Validate(options *chunkers.ChunkerOpts) error {

	if options.NormalSize == 0 || options.NormalSize < 64 ||
		options.NormalSize > 1024*1024*1024 {
		return ErrNormalSize
	}
	if options.MinSize < 64 || options.MinSize > 1024*1024*1024 ||
		options.MinSize >= options.NormalSize {
		return ErrMinSize
	}
	if options.MaxSize < 64 || options.MaxSize > 1024*1024*1024 ||
		options.MaxSize <= options.NormalSize {
		return ErrMaxSize
	}
	return nil
}

// Algorithm's return value, cutpoint, might typically be used next in
// segment := data[:cutpoint], so we expect to exclude the cutpoint
// index value itself. Also commonly when n == len(data) and data is
// short, then the returned cutpoint will be n;
// n is the default to return when we did not find a shorter
// cutpoint. The segment := data[:len(data)] will then take
// all of data as the segment to hash.
//
// PRE condition: n must be <= len(data). We will panic if this does not hold.
// It is always safe to pass n = len(data).
//
// POST INVARIANT: cutpoint <= n. We never return a cutpoint > n.
func (c *UltraCDC) Algorithm(options *chunkers.ChunkerOpts, data []byte, n int) (cutpoint int) {

	// A common case will be n == len(data), but n could certainly be less.
	// Confirm that it is never more.
	if n > len(data) {
		panic(fmt.Sprintf("len(data) == %v and n == %v: n must be <= len(data)", len(data), n))
	}

	const (
		maskS uint64 = 0x2F // binary 101111

		// maskL ignores 2 more bits than maskS, so
		// it is easier to match (so we get a higher
		// probability of match after the normal point).
		maskL uint64 = 0x2C // binary 101100

		lowEntropyStringThreshold int = 64 // LEST in the paper.
	)
	minSize := options.MinSize
	maxSize := options.MaxSize
	normalSize := options.NormalSize

	var lowEntropyCount int

	// initial mask for small cuts below the Normal point.
	mask := maskS

	switch {
	case n <= minSize:
		cutpoint = n
		return
	case n >= maxSize:
		n = maxSize
	case n <= normalSize:
		normalSize = n
	}

	outBufWin := data[minSize : minSize+8]

	// Initialize hamming distance on outBufWin
	dist := 0
	for _, v := range outBufWin {
		// effectively the Pattern of 0xAAAAAAAAAAAAAAAA,
		// as referenced in the paper,
		// is expressed here, just one byte at a time.
		dist += bits.OnesCount8(v ^ 0xAA)
	}

	var inBufWin []byte
	for i := minSize + 8; i <= n-8; i += 8 {
		if i >= normalSize {
			// Yes, we write mask every time after the Normal point,
			// and at first this appears wasteful. However,
			// an engineering judgement call was made to keep it.
			//
			// The rationale is that this small redudancy
			// is cheaper, simpler, and safer
			// than duplicating all the logic below for the two different
			// masks and then having to be sure to keep
			// the duplicates in sync. We saw multiple bugs in the past from
			// MinSize and NormalSize not being 8 byte aligned,
			// and we'd also rather not impose that on users.
			// The POST invariance analysis is also much simplified.
			// The CPU has to do less branch prediction this way,
			// and maskL will almost surely be quickly
			// accessible in a cache line.
			mask = maskL
		}

		// If i == n-8 then i+8 == n, and since n <= len(data)
		// as a PRE condition, we never go out of bounds.
		inBufWin = data[i : i+8]

		if bytes.Equal(inBufWin, outBufWin) {
			lowEntropyCount++
			if lowEntropyCount >= lowEntropyStringThreshold {
				// on random (high-entropy) data, we don't expect to get here.

				// If i == n-8, its largest, then this returns n,
				// which maintains our POST INVARIANT that cutpoint <= n.
				cutpoint = i + 8
				return
			}
			continue
		}

		lowEntropyCount = 0
		for j := 0; j < 8; j++ {
			if (uint64(dist) & mask) == 0 {
				// Do we preserve the POST INVARIANT here?
				// if i == n-8 (the biggest possible), and
				// j is 7 (its biggest possible), then
				// i + j could here be as big as n - 8 + 7 == n-1
				// So, yes, n-1 is the biggest we could return here.
				cutpoint = i + j
				return
			}
			outByte := data[i+j-8]
			inByte := data[i+j]

			// The hamming distance instruction POPCNT is
			// typically available in today's hardware, but
			// upon measurement the lookup table still looks
			// faster; plus its more portable.
			//
			// I'll leave the bits.OnesCountXX (POPCNT based)
			// version here in case newer hardware gets even faster; or maybe we
			// weren't using the hardware right when we measured.
			// Or maybe only bits.OnesCount64 uses POPCNT? Not worth
			// going deeper at the moment.
			//
			// https://stackoverflow.com/questions/28802692/how-is-popcnt-implemented-in-hardware
			//
			//update := bits.OnesCount8(inByte^0xAA) - bits.OnesCount8(outByte^0xAA)
			update := hammingDistanceTo0xAA[inByte] - hammingDistanceTo0xAA[outByte]
			dist += update
		}
		outBufWin = inBufWin
	}

	// obviously preserves the POST INVARIANT that cutpoint <= n.
	cutpoint = n
	return
}

// Cutpoints computes all the cutpoints we can in a batch, all at once,
// if maxPoints <= 0; otherwise only up to a maximum of maxPoints.
// We may find fewer, of course. There will always be one, as
// len(data) is returned in cuts if no sooner cutpoint is found.
// If maxPoints <= 0 then the last cutpoint in cuts will
// always be len(data).
func (c *UltraCDC) Cutpoints(data []byte, maxPoints int) (cuts []int) {

	const (
		maskS uint64 = 0x2F // binary 101111

		// maskL ignores 2 more bits than maskS, so
		// it is easier to match (so we get a higher
		// probability of match after the normal point).
		maskL uint64 = 0x2C // binary 101100

		lowEntropyStringThreshold int = 64 // LEST in the paper.
	)
	minSize := c.Opts.MinSize
	maxSize := c.Opts.MaxSize
	normalSize := c.Opts.NormalSize

	if minSize <= 0 {
		panic("MinSize must be positive")
	}

	// most recently found cut.
	var cutpoint int

	// Find as many cutpoints as we can.
	// After each one, start again at begin.
begin:
	for {
		n := len(data)

		// We modify normal below, if end <= normalSize.
		// I doubt we can would normally do another loop
		// after that, but I cannot prove it would never
		// happen. So to be on the safe side, we just reset it
		// every time.
		normal := normalSize

		var lowEntropyCount int

		// initial mask for small cuts below the Normal point.
		mask := maskS

		//fmt.Printf("n = %v; minSize=%v; normalSize=%v; maxSize=%v; len(cuts)=%v\n",
		//	n, minSize, normalSize, maxSize, len(cuts))

		// how far we go on a single pass through
		// this loop looking for a cutpoint.
		end := maxSize
		if n < end {
			end = n
		}
		if end <= minSize {
			// clients should recognize that the very
			// last cut may be "pre-mature".
			cuts = append(cuts, end)
			//fmt.Printf("end %v <= minSize = %v, returning\n", end, minSize)
			return
		}
		//fmt.Printf("end %v > minSize = %v, not returning\n", end, minSize)

		if end <= normalSize {
			normal = end
		}

		outBufWin := data[minSize : minSize+8]

		// Initialize hamming distance on outBufWin
		dist := 0
		for _, v := range outBufWin {
			// effectively the Pattern of 0xAAAAAAAAAAAAAAAA,
			// as referenced in the paper,
			// is expressed here, just one byte at a time.
			dist += bits.OnesCount8(v ^ 0xAA)
		}

		var inBufWin []byte

	innerLoop:
		for i := minSize + 8; i <= end-8; i += 8 {
			if i >= normal {
				// Yes, we write mask every time after the Normal point,
				// and at first this appears wasteful. However,
				// an engineering judgement call was made to keep it.
				//
				// The rationale is that this small redudancy
				// is cheaper, simpler, and safer
				// than duplicating all the logic below for the two different
				// masks and then having to be sure to keep
				// the duplicates in sync. We saw multiple bugs in the past from
				// MinSize and NormalSize not being 8 byte aligned,
				// and we'd also rather not impose that on users.
				// The POST invariance analysis is also much simplified.
				// The CPU has to do less branch prediction this way,
				// and maskL will almost surely be quickly
				// accessible in a cache line.
				mask = maskL
			}

			// If i == n-8 then i+8 == n, and since n <= len(data)
			// as a PRE condition, we never go out of bounds.
			inBufWin = data[i : i+8]

			if bytes.Equal(inBufWin, outBufWin) {
				lowEntropyCount++
				if lowEntropyCount >= lowEntropyStringThreshold {
					// on random (high-entropy) data, we don't expect to get here.

					// If i == n-8, its largest, then this returns n,
					// which maintains our POST INVARIANT that cutpoint <= n.
					cut := i + 8
					cutpoint += cut
					cuts = append(cuts, cutpoint)
					data = data[cut:]
					//fmt.Printf("found LEST cutpoint %v; data now len %v\n", cutpoint, len(data))
					if len(data) == 0 || (maxPoints > 0 && len(cuts) >= maxPoints) {
						return
					}
					continue begin
				}
				continue innerLoop
			}

			lowEntropyCount = 0
			for j := 0; j < 8; j++ {
				if (uint64(dist) & mask) == 0 {
					// Do we preserve the POST INVARIANT here?
					// if i == n-8 (the biggest possible), and
					// j is 7 (its biggest possible), then
					// i + j could here be as big as n - 8 + 7 == n-1
					// So, yes, n-1 is the biggest we could return here.
					cut := i + j
					cutpoint += cut
					cuts = append(cuts, cutpoint)
					data = data[cut:]
					//fmt.Printf("found dist cut %v; cutpoint %v; data now len %v\n", cut, cutpoint, len(data))
					if len(data) == 0 || (maxPoints > 0 && len(cuts) >= maxPoints) {
						return
					}
					continue begin
				}
				outByte := data[i+j-8]
				inByte := data[i+j]

				// The hamming distance instruction POPCNT is
				// typically available in today's hardware, but
				// upon measurement the lookup table still looks
				// faster; plus its more portable.
				//
				// I'll leave the bits.OnesCountXX (POPCNT based)
				// version here in case newer hardware gets even faster; or maybe we
				// weren't using the hardware right when we measured.
				// Or maybe only bits.OnesCount64 uses POPCNT? Not worth
				// going deeper at the moment.
				//
				// https://stackoverflow.com/questions/28802692/how-is-popcnt-implemented-in-hardware
				//
				//update := bits.OnesCount8(inByte^0xAA) - bits.OnesCount8(outByte^0xAA)
				update := hammingDistanceTo0xAA[inByte] - hammingDistanceTo0xAA[outByte]
				dist += update
			}
			outBufWin = inBufWin

		} // end i

		cut := end
		cutpoint += cut
		cuts = append(cuts, cutpoint)
		data = data[cut:]
		//fmt.Printf("found no other cutpoint %v; data now len %v\n", cutpoint, len(data))
		if len(data) == 0 || (maxPoints > 0 && len(cuts) >= maxPoints) {
			return
		}
	}

	panic("should never be reached now.")
}
