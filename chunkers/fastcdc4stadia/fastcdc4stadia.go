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
	"errors"
	"fmt"
	"math"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

func init() {
	chunkers.Register("fastcdc4stadia", newFastCDC4Stadia)
}

var errNormalSize = errors.New("NormalSize is required and must be 64B <= NormalSize <= 1GB")
var errMinSize = errors.New("MinSize is required and must be 64B <= MinSize <= 1GB && MinSize < NormalSize")
var errMaxSize = errors.New("MaxSize is required and must be 64B <= MaxSize <= 1GB && MaxSize > NormalSize")

type FastCDC4Stadia struct {
}

func newFastCDC4Stadia() chunkers.ChunkerImplementation {
	return &FastCDC4Stadia{}
}

func (c *FastCDC4Stadia) Setup(options *chunkers.ChunkerOpts) error {
	return nil
}

func (c *FastCDC4Stadia) DefaultOptions() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		MaxSize:    64 * 1024,
		NormalSize: 8 * 1024,
	}
}

func (c *FastCDC4Stadia) Validate(options *chunkers.ChunkerOpts) error {
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

// FastCDC4Stadia modifies the FastCDC algorithm: this
// is NOT the same as the original paper!
// We use a unint64 for the hash, so it is 64-bits wide.
// The gear table is the 64-bit version that
// provides slightly better dedup.
// references:
// [0] https://github.com/google/cdc-file-transfer/blob/main/fastcdc/fastcdc.h
// [1] https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf.
// [2] https://github.com/dbaarda/rollsum-chunking/blob/master/RESULTS.rst
// [3] https://www.usenix.org/system/files/conference/atc12/atc12-final293.pdf
func (c *FastCDC4Stadia) Algorithm(options *chunkers.ChunkerOpts, data []byte, N int) int {

	// A common case will be n == len(data), but n could certainly be less.
	// Confirm that it is never more.
	if N > len(data) {
		panic(fmt.Sprintf("len(data) == %v and n == %v: n must be <= len(data)", len(data), N))
	}

	// the code below needs n to be uint64, so rename the
	// formal argument to N and let little n be uint64 from now on.
	n := uint64(N)

	// NB: the very last chunk might be smaller than this, of course.
	minSize := uint64(options.MinSize)

	// chunks are never bigger.
	maxSize := uint64(options.MaxSize)

	// regression/reflection was introdcued
	// in the microsoft paper; giving flatter distribution
	// when maxSize is smaller.

	normalSize := uint64(options.NormalSize)

	thresh := uint64(math.MaxUint64) / (normalSize - minSize + 1)

	switch {
	case n <= minSize:
		return int(n)
	case n >= maxSize:
		n = maxSize
	}

	regressionLen := n
	var regressionMask uint64 // == 0 => match anything

	// "Init hash to all 1's to avoid zero-length chunks with min_size=0."
	var hash uint64 = math.MaxUint64

	const kHashBits = 64
	var i uint64
	if minSize > kHashBits {
		i = minSize - kHashBits
	}

	for ; i < minSize; i++ {
		hash = (hash << 1) + gear64[data[i]]
	}

	// (leave i at minSize! do not set back to 0)
	for ; i < n; i++ {

		if hash&regressionMask == 0 {

			if hash <= thresh {
				return int(i)
			}

			regressionLen = i
			regressionMask = math.MaxUint64

			for hash&regressionMask != 0 {
				regressionMask = regressionMask << 1 // inf loop here on all zero?
			}
		}
		hash = (hash << 1) + gear64[data[i]]
	}
	// "Return best regression point we found or the end if it's better."
	if hash&regressionMask != 0 {
		return int(regressionLen)
	}
	return int(i)
}

// random [256] slice
var gear64 = []uint64{
	0x8491247ace8fa4ed, 0xef6f83ef0eb0423a, 0x8e5c2be1f316d634,
	0x1a6b3add4d7fe997, 0x9e4e9c1b8856240d, 0x7901cdcba45eb71a,
	0x231e85f0faf483d8, 0x1b4ab739e20b8cb1, 0xdbda4432ca243f76,
	0xb2d894d2426310cd, 0x839995f33aabd8f1, 0x9cccd0bbabbaaf9d,
	0x3ef9fd960f823fb6, 0xd5d59780e1f38af3, 0x27facf53ac93364f,
	0x8dbae7fb1a94b826, 0xbcfaf245c81d5b9f, 0x49336c70f611b64e,
	0x3571e070f3a4cd59, 0x5dc06a5cf90a86f6, 0x99abb6ea0a4f156,
	0xb9b92296e5eb1e22, 0xf9ad0cc35f4ee97, 0x62cb5f8f88a0406b,
	0xc1a11da8e0e8cc2a, 0x8c239edaf6069a8c, 0x375e7af49e5244a2,
	0xc581ef8a03970488, 0xaf77f46327cdf5d, 0x7ef4664d220edc6,
	0x63f3a647be9f1614, 0xd2fc59c96d6b87ad, 0x88745637e11038c1,
	0x9c719e92a544113d, 0x6c3ee1140777a315, 0x9fd49ddce628e564,
	0x10bef8449642c051, 0x75ade0f3ad422274, 0x22ff8fbc4242b2c5,
	0x6a6fcdf012903a4b, 0x5ed90ba6df6f0575, 0xf0e561e75268c2a2,
	0x840d88d4bf9c0b74, 0x9ddb3916e5b076a, 0x49cee2f4c0320438,
	0xbcfa7fa4be4291e5, 0x6467b95e9a8356fc, 0xe7038d6f716b766d,
	0x9a69c0beec5adbec, 0x3f48ed09b0432b98, 0x60d541174db84de8,
	0xbfa499091125bf7d, 0x389aa4da4a299e7b, 0x4c9e09d859f70144,
	0x61a7986df7e97bce, 0x31e929cb879c6525, 0x924952ee09e2924f,
	0x1a922510c6fb5ca7, 0xe36a67aa8317d9d6, 0xfc4b6fc00cd35d2e,
	0xa1dfde3b89f7ecd9, 0x2d2738a1a871b031, 0x626dd9b2e1849709,
	0xc2e5fc1b73153f19, 0xfbdf8057f90cc597, 0xd6b0b92291914061,
	0x712691734a1327c8, 0xd326c9a24910b830, 0x3b5c57b7734b39fd,
	0xff5091cdb73cec7, 0x14d9919830abce04, 0xef599887f6a5abb1,
	0xe92b4a5d2512d9f, 0x8c1343905342c413, 0x557e6f4c5c58c3f6,
	0xe82cec1b269bbbbd, 0x8978d511054b3ab0, 0xd2fce22bf9f4e348,
	0x8bf144638a5f5796, 0x647efba66eaef57f, 0xa98d2d10a57e8a7d,
	0xbd3127b0a5d10ce9, 0x371ab70261b6ca43, 0xf0b946207000fbeb,
	0xd629ca24cdc4fd44, 0xef14b9e0844761a4, 0x3e59f32a56c1fffb,
	0x4e08a128dcda76ab, 0x6317214ea7d99fae, 0xff484be613728267,
	0x66a02126378c0480, 0x9d08f636207b4e5a, 0xb117fbf3f69eb6e9,
	0xa3c18816f9459e25, 0x59e006979053d9f0, 0xf2df699b7baf4f9a,
	0xcfbd687e95006ced, 0x7f506d200d86899e, 0x8762a217ec25d9c0,
	0x7362c031992d892d, 0xcdce287de14a4adf, 0x9cea7e1e5d565c7c,
	0x4a52376eb368942, 0xd0dc49a93e262bd2, 0xe17ede683f556d04,
	0xec8a9bbd5de07e1, 0x31d6b2a4e3bd47bf, 0x41136d5b7a1b7d67,
	0x64f41962fe98eb1f, 0x6788e4f777928ee7, 0x661405e078be20b,
	0x1965662e202a521, 0x7b722c2aa4a198d9, 0x66b4a1d2d763b34c,
	0x296dece82d0ccead, 0x5bc8bc380f8548a, 0xed5f0560f84b91f4,
	0xb82c8c27dc0768f1, 0xf5ae73b72c3830d6, 0x6d330e412d58c450,
	0xf0260bbf7eb6a5f6, 0x2eae75bd682d009c, 0xc50f47d01da153b4,
	0x82fda4160237328d, 0x71bf180eb671c7c6, 0x3c211cae288a846b,
	0xb83883a2ea404ed5, 0x301f89d274c8b96, 0x3028ffff46156359,
	0x9623cff53bd22f69, 0x254c8716768a76bd, 0xf43428b02ac7e71,
	0xef87f74136018cd7, 0xed70f6cc2e5a1b14, 0xcfce9591664decd0,
	0x526da3ec58c0eb1e, 0xb022b0de25996366, 0xee456d90b08673a5,
	0x6ee7b2a4afcaeded, 0xeedcadec61692821, 0xc890f956f371c6aa,
	0xdbb1355802cc4a14, 0x2aa96a60229886fb, 0xc9438611ed6d39a,
	0x48fdb9caa455e89f, 0xb7fb8a4a9e0431cd, 0xdb5d2a2c73183aab,
	0xc0cad5ed82cea56e, 0x8cd515d28962804c, 0xea2ede16fe381a33,
	0x80b05ffbb4831437, 0xcf784306c0e1da56, 0x25cfa51617691b76,
	0x2ddd6c7c41a9b6a1, 0xc06d1038b17c2df5, 0x322cd3d4ca044b65,
	0xa6fff882e0bffb20, 0xae836ccfae4a8daa, 0x688d1558d2a2889e,
	0xad6f0b615dbad0bc, 0xb63532f10c0b60d, 0x951fc0fe5888c690,
	0x313dfc918cb10a91, 0xbc6918a29ab8f646, 0xa623d7d58decf648,
	0xc6aae06bdc5afa94, 0x786216ece87786cd, 0x89690cf7bf52ae2a,
	0x183d1031e43ecc8, 0x9a4e252bfe5e7448, 0xc890305167fccf49,
	0xd9bd458ea0056928, 0xde45a84a1d88f826, 0xd11b9347a55c9d50,
	0x12517b203ba99caf, 0x7fbfdda8d0de88ca, 0xf781c2a0d2b990a8,
	0x96ab7398ce099b8f, 0x5f94ac89fa3c40ef, 0xfa8f052c301a6974,
	0x86792e4991df575f, 0x3c29997d479a7560, 0x1aa5808eef6ee029,
	0x808a5210862e83a, 0xf0255f3aaddb1d99, 0x137c229a37be7ee6,
	0xcccefc9fbdf1a5e3, 0xebbd33fb3af1d2f2, 0xb33a5454c9bdf708,
	0x3baf4c066aeb99f0, 0xcf9e7c9e38c9cbde, 0x41bcc7608e4358a,
	0x45e86bc18ebed4ed, 0x45151340bf7deaec, 0x2babdc7a53300776,
	0x7a8c8e69f1df2e17, 0x840fccf20170375, 0x258e78c689d7f1cc,
	0x513ad967f73a79f0, 0x572b72acc9fdc94e, 0x6716050e6d3c4bcd,
	0x417cd4ea3e740ea6, 0xb06821ae68f8f53, 0x30c00b83b62d41b5,
	0x6da4d1e65fb04b84, 0xb2259595a7bbc508, 0x5dde25e3d8cce8ab,
	0x5612238a2eb7bd60, 0x1518af25cee8b39d, 0x86c4d5c83d4f739,
	0xac6065d0956a8218, 0xed8a1d026cf49e4, 0x32f6ab67b23ca47,
	0x3f8a2d37ec384725, 0x1258e6fdd59e87d9, 0xb8e26ec0772caed3,
	0x14eab3cfca9095f5, 0x272a6400d862da91, 0xae9db7ecda64622c,
	0xf2a6239fcde76ba8, 0x6387a298ae9f57e4, 0xc55a0e84950a8f9,
	0xba71e19716954cfd, 0xd2e3cca8f3d0e7d3, 0xd9d0d222ec1a10d5,
	0x8f2cf116e24b08ba, 0x757b0f05c10c6643, 0x399e29aa2535cc45,
	0xc1c75686141dba8b, 0xe7cb6ae92546b537, 0xa2c78f99e81fc094,
	0x9f44f935b6e331c3, 0xd4f5bd7a41c444e4, 0xe9039fdd669d1ff2,
	0xe0529652c458f1e4, 0xc587376080a8635b, 0x8064da4ab9a978e5,
	0x86189cb0545c0cea, 0x57e9eac3ff58f820, 0xdb426e8ac3c6111f,
	0xfb4034c6b66a134d, 0xe6bf1b2f31ebb4f, 0xa4beabda26098f32,
	0x2a679aa4d23b8859, 0x26e660c6e4f04ced, 0xb984b1386de0d796,
	0x1db677b46e34d965, 0xb31e7767a947c68, 0x8f77a32a1e3be2d5,
	0x813137a9410ca6c5, 0x6aa07239ba3cae35, 0x7584b6295f9b266d,
	0xfed8b9effbed289b, 0x98ea5373bf61b09c, 0xc4b5e89cbb05c329,
	0x611f22b45da87895}
