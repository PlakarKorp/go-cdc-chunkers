/*
 * Copyright (c) 2024 Jason E. Aten, Ph.D.
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

package ultracdc

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	mathrand2 "math/rand/v2"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	//"github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
)

func hashOfBytes(by []byte) string {
	h := sha256.New()
	h.Write(by)
	enchex := hex.EncodeToString(h.Sum(nil))
	return enchex
}

// Prepending 2 bytes to the data should only change
// the first segment's hash.
func Test_Prepend_two_bytes(t *testing.T) {

	// deterministic pseudo-random numbers as data.
	var seed [32]byte
	generator := mathrand2.NewChaCha8(seed)
	data := make([]byte, 1<<20)
	generator.Read(data)

	u := newUltraCDC().(*UltraCDC)
	opt := u.DefaultOptions()
	opt.MinSize = 1
	opt.MaxSize = 8000
	opt.NormalSize = 24
	cuts, hashmap := getCuts("orig", data, u, opt)

	// how many segments change if we alter the data? just by prepending 2 bytes.
	differ := 0
	data = append([]byte{0x39, 0x46}, data...)
	cuts2, hashmap2 := getCuts("with prepend 2 bytes -- ", data, u, opt)
	for j, cut := range cuts2 {
		if cuts[j] != cut {
			differ++
		}
	}
	//fmt.Printf("after pre-pending 2 bytes, the number of cuts that differ = %v; out of %v\n", differ, len(cuts))

	matchingHashes := 0
	for hash0 := range hashmap {
		if hashmap2[hash0] {
			matchingHashes++
		}
	}
	//fmt.Printf("matchingHashes = %v\n", matchingHashes)

	// good: just the first segment changed.
	if matchingHashes != 500 || len(cuts) != 501 {
		t.Fatalf("should had 500 out of 501 matching hashes; matchingHashes = %v; len(cuts) = %v", matchingHashes, len(cuts))
	}
}

// Injecting 2 bytes into the middle/end of the data should only change
// the one or two segment's hash.
func Test_Middle_inject_two_bytes(t *testing.T) {

	for k := 0; k < 2; k++ {
		// deterministic pseudo-random numbers as data.
		var seed [32]byte
		generator := mathrand2.NewChaCha8(seed)
		data := make([]byte, 10<<20)
		generator.Read(data)

		u := newUltraCDC().(*UltraCDC)
		opt := u.DefaultOptions()
		// use unchanged defaults now
		//		opt.MinSize = 1
		//		opt.MaxSize = 8000
		//		opt.NormalSize = 24
		cuts, hashmap := getCuts("orig", data, u, opt)

		// how many segments change if we alter the data? by injecting 2 bytes in the middle.
		differ := 0
		if k == 0 {
			// add in proper middle
			data = append(data[:2<<20], append([]byte{0x32, 0x41}, data[2<<20:]...)...)
		} else {
			// append to tail
			data = append(data, []byte{0x32, 0x41}...)
		}
		cuts2, hashmap2 := getCuts("with middle injected 2 bytes -- ", data, u, opt)
		for j, cut := range cuts2 {
			if cuts[j] != cut {
				differ++
			}
		}
		//fmt.Printf("after pre-pending 2 bytes, the number of cuts that differ = %v; out of %v\n", differ, len(cuts))

		matchingHashes := 0
		for hash0 := range hashmap {
			if hashmap2[hash0] {
				matchingHashes++
			}
		}
		//fmt.Printf("matchingHashes = %v\n", matchingHashes)

		// good: just one segment changed.
		if matchingHashes != 921 || len(cuts) != 922 {
			t.Fatalf("should had 921 out of 922 matching hashes; matchingHashes = %v; len(cuts) = %v", matchingHashes, len(cuts))
		}
	}
}

// Sanity check that refactoring ultracdc.go has
// not changed its output. If you change the
// parameters to the algorithm, you may well
// have to regenerate the expectedCuts by
// setting regenerate = true below for one test run.
// Then use the test output to update the
// expected values below.
func Test_Splits_Not_Changed(t *testing.T) {

	// deterministic pseudo-random numbers as data.
	var seed [32]byte
	generator := mathrand2.NewChaCha8(seed)
	const N = 1<<20 + 1
	data := make([]byte, N)
	generator.Read(data)

	u := newUltraCDC().(*UltraCDC)
	opt := u.DefaultOptions()

	// in normal use MinSize = 0 is bad idea,
	// and it should be like 64; but this simplifies debugging edge cases.
	opt.MinSize = 1
	opt.MaxSize = 8000
	opt.NormalSize = 24

	const regenerate = false
	if regenerate {
		regenExpected(u, data, opt)
		return
	}

	cuts, _ := getCuts("orig", data, u, opt)

	for j, cut := range cuts {
		if expectedCuts[j] != cut {
			t.Fatalf(`expected %v but got %v at j = %v`, expectedCuts[j], cut, j)
		}
	}

	// check that Cutpoints() gives the same.
	//fmt.Printf("len(data) = %v, N = %v\n", len(data), N)
	u.Opts = opt
	cuts2 := u.Cutpoints(data, 0)
	//fmt.Printf("len(cuts2) = %v\n", len(cuts2))
	//fmt.Printf("cuts2[len(cuts2)-1] = '%v'\n", cuts2[len(cuts2)-1])
	//fmt.Printf("len(expectedCuts) = %v\n", len(expectedCuts))
	if len(cuts2) != len(expectedCuts) {
		t.Fatalf(`Cutpoints(): expected len(cuts2)=%v to be %v`, len(cuts2), len(expectedCuts))
	}
	for j, cut := range cuts2 {
		if expectedCuts[j] != cut {
			t.Fatalf(`Cutpoints(): expected %v but got %v at j = %v`, expectedCuts[j], cut, j)
		}
	}
}

func getCuts(
	title string,
	data []byte,
	u chunkers.ChunkerImplementation,
	opt *chunkers.ChunkerOpts,
) (cuts []int, hashmap map[string]bool) {

	hashmap = make(map[string]bool)
	last := 0
	j := 0
	for len(data) > opt.MinSize {
		cutpoint := u.Algorithm(opt, data, len(data))
		if cutpoint == 0 {
			panic("should never get cutpoint 0 now")
		}
		cut := last + cutpoint
		cuts = append(cuts, cut)
		last = cut
		j++
		hashmap[hashOfBytes(data[:cutpoint])] = true
		data = data[cutpoint:]
	}
	return
}

func regenExpected(u chunkers.ChunkerImplementation, data []byte, opt *chunkers.ChunkerOpts) {

	cuts, _ := getCuts("regen", data, u, opt)
	fmt.Printf("var expectedCuts = []int{\n")
	for i, cut := range cuts {
		fmt.Printf("%v, ", cut)
		if i%8 == 0 {
			fmt.Println()
		}
	}
	fmt.Printf("\n}\n")
}

var expectedCuts = []int{
	1300, 2533, 2716, 7542, 8163, 8850, 9235, 9352, 9768,
	10470, 18470, 22235, 25918, 27177, 28756, 29658, 32586,
	36105, 37570, 37873, 37992, 39888, 41003, 42307, 42559,
	43782, 45234, 45750, 46013, 48750, 50533, 58099, 59518,
	61436, 62688, 65228, 65852, 67631, 67945, 69414, 70697,
	74920, 75431, 75773, 83279, 88359, 90666, 94457, 95139,
	95662, 96581, 97432, 99945, 106660, 114660, 115713, 118308,
	122834, 124152, 126360, 127637, 128074, 132800, 134389, 134602,
	135825, 138274, 139792, 140752, 141179, 141980, 144085, 144781,
	145111, 146564, 147070, 147307, 147404, 147943, 148791, 153435,
	155065, 157115, 157592, 158550, 165067, 166956, 167907, 168980,
	171214, 172192, 173166, 174701, 182701, 183358, 183482, 185867,
	186417, 188136, 188986, 194808, 200547, 203352, 204792, 207122,
	209635, 211656, 212546, 213975, 214868, 215419, 216695, 216933,
	217858, 220934, 221114, 228454, 229046, 231288, 233942, 237413,
	238926, 239619, 240580, 240948, 242188, 246089, 246843, 248496,
	248670, 250837, 258561, 260561, 267187, 269882, 271787, 273895,
	274800, 278298, 285028, 285546, 288116, 288282, 289732, 290725,
	292164, 295772, 296387, 300323, 301863, 303788, 306780, 311018,
	313559, 314982, 316643, 317848, 320203, 325676, 327291, 328824,
	330424, 331969, 334782, 337218, 338207, 339894, 340684, 341706,
	341905, 348306, 352054, 353426, 354370, 357679, 361929, 362630,
	363709, 367474, 368808, 371583, 372705, 377749, 378421, 380413,
	381952, 382838, 382891, 383131, 383430, 384979, 385506, 386958,
	388518, 395927, 396589, 398009, 403212, 404599, 404685, 405602,
	406989, 407513, 409015, 410536, 414721, 415836, 417195, 417807,
	420053, 420531, 422450, 422837, 425287, 426261, 429586, 429935,
	431648, 432455, 432896, 433298, 433819, 434422, 438811, 442799,
	445011, 445212, 446851, 447117, 449599, 457599, 458274, 458600,
	459273, 459453, 462383, 470383, 473200, 478718, 480792, 482999,
	483217, 485614, 485917, 489160, 489859, 493918, 497068, 503441,
	504496, 505484, 507159, 513253, 518267, 518775, 518917, 520641,
	523932, 526400, 527094, 527280, 528269, 529543, 530875, 532796,
	532994, 534223, 534936, 541057, 542021, 544828, 549825, 553758,
	560348, 560402, 564595, 570024, 571366, 572482, 573249, 574182,
	575073, 576091, 580543, 584542, 587169, 587504, 588726, 589821,
	590150, 591083, 591763, 593621, 601621, 602111, 602664, 606393,
	607309, 607349, 611417, 612375, 612719, 613220, 614306, 615680,
	618538, 620320, 627628, 628997, 631679, 632387, 633227, 634708,
	635128, 635943, 636923, 637108, 637465, 640984, 642459, 643036,
	649099, 650755, 651793, 652355, 653353, 653868, 654316, 654754,
	654845, 656540, 663710, 667547, 667883, 668103, 668188, 668382,
	670124, 672950, 678381, 684322, 685822, 688443, 689210, 689593,
	696817, 698568, 699030, 699815, 705341, 712003, 713684, 714117,
	717815, 721236, 723764, 724585, 724932, 725852, 725995, 726761,
	726916, 727199, 728146, 729657, 732622, 734595, 741202, 743510,
	748711, 751960, 752154, 752425, 753866, 755052, 755137, 755903,
	756406, 757062, 757289, 760968, 761731, 763829, 771829, 774171,
	776935, 778402, 779091, 780789, 783448, 786351, 790532, 791887,
	792708, 792747, 794264, 794833, 794931, 795309, 797305, 798018,
	798757, 799848, 802729, 803295, 806451, 807210, 807727, 812843,
	816669, 820584, 820746, 822346, 825479, 827367, 832751, 833637,
	836227, 838212, 839421, 839571, 840855, 842411, 843342, 843583,
	847037, 852000, 852163, 860163, 861094, 863859, 864788, 865955,
	866110, 866877, 869881, 870026, 874703, 874958, 875038, 876020,
	877792, 879292, 880008, 882393, 884687, 885660, 885865, 890142,
	893908, 898870, 899300, 899802, 900212, 904499, 908520, 910601,
	911302, 914019, 921166, 923795, 930383, 933268, 934032, 938030,
	940450, 948450, 951689, 959501, 960631, 963249, 971249, 972391,
	975700, 977972, 978501, 978535, 983227, 986408, 994408, 995427,
	996368, 996794, 997507, 997593, 1004521, 1005709, 1010844, 1011449,
	1016752, 1017028, 1017782, 1023265, 1027555, 1035555, 1038852, 1039386,
	1045847, 1047729, 1047858, 1048577,
}
