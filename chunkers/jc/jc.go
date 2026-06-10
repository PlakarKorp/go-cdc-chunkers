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
	"sync"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	"github.com/zeebo/blake3"
)

// keyedTableCache memoizes key-derived Gear tables process-wide, indexed by the
// key bytes. Derived tables are immutable after construction, so a single
// pointer can be shared across all chunkers and goroutines using the same key —
// the same way unkeyed chunkers share the static table. This avoids both the
// allocation and the blake3 derivation on every Setup for a repeated key.
var keyedTableCache sync.Map // map[string]*[256]uint64

// getGearTable returns the Gear table to use for the given key. With a nil key
// it returns a pointer to the shared static table (no allocation). With a key
// it returns a cached derived table, deriving and caching one on first use.
func getGearTable(key []byte) (*[256]uint64, error) {
	if key == nil {
		return &G, nil
	}
	if cached, ok := keyedTableCache.Load(string(key)); ok {
		return cached.(*[256]uint64), nil
	}

	hasher, err := blake3.NewKeyed(key)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 8)
	for i := range 256 {
		binary.LittleEndian.PutUint64(buf, G[i])
		hasher.Write(buf)
	}
	dgst := hasher.Digest()
	digestBytes := make([]byte, 8*256)
	if _, err := readDigest(dgst, digestBytes); err != nil {
		return nil, err
	}
	table := new([256]uint64)
	for i := range 256 {
		table[i] = binary.LittleEndian.Uint64(digestBytes[i*8 : i*8+8])
	}

	// LoadOrStore so that two goroutines racing on the same fresh key converge
	// on a single shared table (the loser's derivation is simply discarded).
	actual, _ := keyedTableCache.LoadOrStore(string(key), table)
	return actual.(*[256]uint64), nil
}

func init() {
	chunkers.Register("jc", newLegacyJC)
	chunkers.Register("jc-v1.0.0", newJC)
	chunkers.Register("jc-v1.1.0", newSpecJC)
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
	// G points at the 256-entry Gear table. When unkeyed it aliases the
	// shared package-level table (no per-chunker copy or allocation); when
	// keyed it points at a freshly derived per-key table.
	G          *[256]uint64
	maskC      uint64
	maskJ      uint64
	jumpLength int

	legacy bool
	// specFaithful drops the n <= NormalSize early-return so that a final
	// segment shorter than NormalSize is still scanned for a content-defined
	// cut-point, exactly as in Algorithm 1 of the JC paper (Jin et al., IEEE
	// TPDS 2023). Without it, such a segment is returned whole. This only ever
	// affects the last chunk of a stream; see Algorithm below.
	specFaithful bool
}

func newLegacyJC() chunkers.ChunkerImplementation {
	return &JC{
		legacy: true,
	}
}

func newJC() chunkers.ChunkerImplementation {
	return &JC{}
}

// newSpecJC ("jc-v1.1.0") is the variant that follows the paper's Algorithm 1
// exactly: it uses the paper's masks and does not short-circuit a final
// sub-NormalSize segment. It is registered separately so that existing chunk
// stores produced by "jc"/"jc-v1.0.0" keep their boundaries.
func newSpecJC() chunkers.ChunkerImplementation {
	return &JC{
		legacy:       true,
		specFaithful: true,
	}
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

	table, err := getGearTable(options.Key)
	if err != nil {
		return err
	}
	c.G = table

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
	case c.specFaithful:
		// Algorithm 1 of the paper only clamps to MaxSize and guards the
		// minimum; a final segment shorter than NormalSize is still scanned.
		// The loop below returns min(i, n) == n when n <= MinSize, which is
		// equivalent to the paper's "if size < c_min: return size".
		if n >= MaxSize {
			n = MaxSize
		}
	case n <= NormalSize:
		// Legacy behaviour: return a final sub-NormalSize segment whole
		// without scanning it. Diverges from the paper; kept for boundary
		// compatibility with existing "jc"/"jc-v1.0.0" chunk stores.
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
