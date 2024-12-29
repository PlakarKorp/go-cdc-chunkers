package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"math/rand"
	"testing"

	mhofmann "codeberg.org/mhofmann/fastcdc"
	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
	askeladdk "github.com/askeladdk/fastcdc"
	jotfs "github.com/jotfs/fastcdc-go"
	restic "github.com/restic/chunker"
	tigerwill90 "github.com/tigerwill90/fastcdc"
)

const (
	//minSize = 128 << 10
	//maxSize = 512 << 10
	//avgSize = 256 << 10

	//minSize = 256 << 10
	//maxSize = 1024 << 10
	//avgSize = 512 << 10

	/*
	   	minSize = 1000
	   	maxSize = 100_000 - 1
	   	avgSize = 12_000

	   jaten@rog ~/go/src/github.com/PlakarKorp/go-cdc-chunkers/tests (main) $ go test -v -run none -bench=.
	   goos: linux
	   goarch: amd64
	   pkg: github.com/PlakarKorp/go-cdc-chunkers/tests
	   cpu: AMD Ryzen Threadripper 3960X 24-Core Processor
	   Benchmark_Restic_Rabin_Next
	   Benchmark_Restic_Rabin_Next-48            	       1	2118265766 ns/op	 506.90 MB/s	     11252 chunks
	   Benchmark_Askeladdk_FastCDC_Copy
	   Benchmark_Askeladdk_FastCDC_Copy-48       	       2	 558987077 ns/op	1920.87 MB/s	    105327 chunks
	   Benchmark_Jotfs_FastCDC_Next
	   Benchmark_Jotfs_FastCDC_Next-48           	       2	 773748212 ns/op	1387.71 MB/s	     75294 chunks
	   Benchmark_Tigerwill90_FastCDC_Split
	   Benchmark_Tigerwill90_FastCDC_Split-48    	       2	 751956122 ns/op	1427.93 MB/s	     69624 chunks
	   Benchmark_Mhofmann_FastCDC_Next
	       chunkers_test.go:486: chunker error: invalid parameter
	   --- FAIL: Benchmark_Mhofmann_FastCDC_Next
	   Benchmark_PlakarKorp_FastCDC_Copy
	   Benchmark_PlakarKorp_FastCDC_Copy-48      	       2	 729818974 ns/op	1471.24 MB/s	     90981 chunks
	   Benchmark_PlakarKorp_FastCDC_Split
	   Benchmark_PlakarKorp_FastCDC_Split-48     	       2	 729276218 ns/op	1472.34 MB/s	     90981 chunks
	   Benchmark_PlakarKorp_FastCDC_Next
	   Benchmark_PlakarKorp_FastCDC_Next-48      	       2	 729959965 ns/op	1470.96 MB/s	     90982 chunks
	   Benchmark_PlakarKorp_UltraCDC_Copy
	   Benchmark_PlakarKorp_UltraCDC_Copy-48     	       7	 149703701 ns/op	7172.45 MB/s	   1029470 chunks
	   Benchmark_PlakarKorp_UltraCDC_Split
	   Benchmark_PlakarKorp_UltraCDC_Split-48    	       8	 144308116 ns/op	7440.62 MB/s	   1029470 chunks
	   Benchmark_PlakarKorp_UltraCDC_Next
	   Benchmark_PlakarKorp_UltraCDC_Next-48     	       8	 151717234 ns/op	7077.26 MB/s	   1029470 chunks
	   Benchmark_PlakarKorp_JC_Copy
	   Benchmark_PlakarKorp_JC_Copy-48           	       3	 375499484 ns/op	2859.50 MB/s	    210939 chunks
	   Benchmark_PlakarKorp_JC_Split
	   Benchmark_PlakarKorp_JC_Split-48          	       3	 377736304 ns/op	2842.57 MB/s	    210939 chunks
	   Benchmark_PlakarKorp_JC_Next
	   Benchmark_PlakarKorp_JC_Next-48           	       3	 376553953 ns/op	2851.50 MB/s	    210939 chunks
	   FAIL
	   exit status 1
	   FAIL	github.com/PlakarKorp/go-cdc-chunkers/tests	29.213s
	*/

	/*
	   	minSize = 1000
	   	maxSize = 100_000 - 1
	   	avgSize = 60_000

	   jaten@rog ~/go/src/github.com/PlakarKorp/go-cdc-chunkers/tests (main) $ go test -v -run none -bench=.
	   goos: linux
	   goarch: amd64
	   pkg: github.com/PlakarKorp/go-cdc-chunkers/tests
	   cpu: AMD Ryzen Threadripper 3960X 24-Core Processor
	   Benchmark_Restic_Rabin_Next
	   Benchmark_Restic_Rabin_Next-48            	       1	2064624054 ns/op	 520.07 MB/s	     11281 chunks
	   Benchmark_Askeladdk_FastCDC_Copy
	   Benchmark_Askeladdk_FastCDC_Copy-48       	       2	 557530840 ns/op	1925.89 MB/s	    105327 chunks
	   Benchmark_Jotfs_FastCDC_Next
	   Benchmark_Jotfs_FastCDC_Next-48           	       2	 811026758 ns/op	1323.93 MB/s	     16587 chunks
	   Benchmark_Tigerwill90_FastCDC_Split
	   Benchmark_Tigerwill90_FastCDC_Split-48    	       2	 787102344 ns/op	1364.17 MB/s	     17096 chunks
	   Benchmark_Mhofmann_FastCDC_Next
	       chunkers_test.go:530: chunker error: invalid parameter
	   --- FAIL: Benchmark_Mhofmann_FastCDC_Next
	   Benchmark_PlakarKorp_FastCDC_Copy
	   Benchmark_PlakarKorp_FastCDC_Copy-48      	       2	 760750140 ns/op	1411.43 MB/s	     37363 chunks
	   Benchmark_PlakarKorp_FastCDC_Split
	   Benchmark_PlakarKorp_FastCDC_Split-48     	       2	 759757907 ns/op	1413.27 MB/s	     37363 chunks
	   Benchmark_PlakarKorp_FastCDC_Next
	   Benchmark_PlakarKorp_FastCDC_Next-48      	       2	 760885612 ns/op	1411.17 MB/s	     37364 chunks
	   Benchmark_PlakarKorp_UltraCDC_Copy
	   Benchmark_PlakarKorp_UltraCDC_Copy-48     	       7	 153540522 ns/op	6993.21 MB/s	   1029470 chunks
	   Benchmark_PlakarKorp_UltraCDC_Split
	   Benchmark_PlakarKorp_UltraCDC_Split-48    	       7	 144855670 ns/op	7412.49 MB/s	   1029470 chunks
	   Benchmark_PlakarKorp_UltraCDC_Next
	   Benchmark_PlakarKorp_UltraCDC_Next-48     	       8	 141387931 ns/op	7594.30 MB/s	   1029470 chunks
	   Benchmark_PlakarKorp_JC_Copy
	   Benchmark_PlakarKorp_JC_Copy-48           	       3	 367975521 ns/op	2917.97 MB/s	    210939 chunks
	   Benchmark_PlakarKorp_JC_Split
	   Benchmark_PlakarKorp_JC_Split-48          	       3	 376776103 ns/op	2849.81 MB/s	    210939 chunks
	   Benchmark_PlakarKorp_JC_Next
	   Benchmark_PlakarKorp_JC_Next-48           	       3	 367556773 ns/op	2921.30 MB/s	    210939 chunks
	   FAIL
	   exit status 1
	   FAIL	github.com/PlakarKorp/go-cdc-chunkers/tests	28.270s
	   jaten@rog ~/go/src/github.com/PlakarKorp/go-cdc-chunkers/tests (main) $
	*/

	/*
	   	minSize = 1000
	   	maxSize = 40_000
	   	avgSize = 20_000

	   jaten@rog ~/go/src/github.com/PlakarKorp/go-cdc-chunkers/tests (main) $ go test -v -run none -bench=.
	   goos: linux
	   goarch: amd64
	   pkg: github.com/PlakarKorp/go-cdc-chunkers/tests
	   cpu: AMD Ryzen Threadripper 3960X 24-Core Processor
	   Benchmark_Restic_Rabin_Next
	   Benchmark_Restic_Rabin_Next-48            	       1	2083806431 ns/op	 515.28 MB/s	     27349 chunks
	   Benchmark_Askeladdk_FastCDC_Copy
	   Benchmark_Askeladdk_FastCDC_Copy-48       	       2	 595073582 ns/op	1804.38 MB/s	    105327 chunks
	   Benchmark_Jotfs_FastCDC_Next
	   Benchmark_Jotfs_FastCDC_Next-48           	       2	 789038673 ns/op	1360.82 MB/s	     53340 chunks
	   Benchmark_Tigerwill90_FastCDC_Split
	   Benchmark_Tigerwill90_FastCDC_Split-48    	       2	 764210288 ns/op	1405.03 MB/s	     55922 chunks
	   Benchmark_Mhofmann_FastCDC_Next
	       chunkers_test.go:575: chunker error: invalid parameter
	   --- FAIL: Benchmark_Mhofmann_FastCDC_Next
	   Benchmark_PlakarKorp_FastCDC_Copy
	   Benchmark_PlakarKorp_FastCDC_Copy-48      	       2	 741015875 ns/op	1449.01 MB/s	     64787 chunks
	   Benchmark_PlakarKorp_FastCDC_Split
	   Benchmark_PlakarKorp_FastCDC_Split-48     	       2	 740732957 ns/op	1449.57 MB/s	     64787 chunks
	   Benchmark_PlakarKorp_FastCDC_Next
	   Benchmark_PlakarKorp_FastCDC_Next-48      	       2	 739831066 ns/op	1451.33 MB/s	     64788 chunks
	   Benchmark_PlakarKorp_UltraCDC_Copy
	   Benchmark_PlakarKorp_UltraCDC_Copy-48     	       7	 143992312 ns/op	7456.94 MB/s	   1029470 chunks
	   Benchmark_PlakarKorp_UltraCDC_Split
	   Benchmark_PlakarKorp_UltraCDC_Split-48    	       7	 152004780 ns/op	7063.87 MB/s	   1029470 chunks
	   Benchmark_PlakarKorp_UltraCDC_Next
	   Benchmark_PlakarKorp_UltraCDC_Next-48     	       8	 139138232 ns/op	7717.09 MB/s	   1029470 chunks
	   Benchmark_PlakarKorp_JC_Copy
	   Benchmark_PlakarKorp_JC_Copy-48           	       3	 375477472 ns/op	2859.67 MB/s	    210955 chunks
	   Benchmark_PlakarKorp_JC_Split
	   Benchmark_PlakarKorp_JC_Split-48          	       3	 377241958 ns/op	2846.29 MB/s	    210955 chunks
	   Benchmark_PlakarKorp_JC_Next
	   Benchmark_PlakarKorp_JC_Next-48           	       3	 375318958 ns/op	2860.88 MB/s	    210955 chunks
	   FAIL
	   exit status 1
	   FAIL	github.com/PlakarKorp/go-cdc-chunkers/tests	28.275s
	*/

	datalen = 1024 << 20
)

type writerFunc func([]byte) (int, error)

func (fn writerFunc) Write(p []byte) (int, error) {
	return fn(p)
}

var rb, _ = io.ReadAll(io.LimitReader(rand.New(rand.NewSource(0)), datalen))

func Test_FastCDC_Next(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("fastcdc", r, nil)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}
	for err := error(nil); err == nil; {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			t.Fatalf(`chunker error: %s`, err)
		}
		if len(chunk) < int(chunker.MinSize()) && err != io.EOF {
			t.Fatalf(`chunker return a chunk below MinSize before last chunk: %s`, err)
		}
		if len(chunk) > int(chunker.MaxSize()) {
			t.Fatalf(`chunker return a chunk above MaxSize`)
		}
		hasher.Write(chunk)
		if err == io.EOF {
			break
		}
	}
	sum2 := hasher.Sum(nil)

	if !bytes.Equal(sum1, sum2) {
		t.Fatalf(`chunker produces incorrect output`)
	}
}

func Test_FastCDC_Copy(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("fastcdc", r, nil)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}

	saw_minsize := false
	w := writerFunc(func(p []byte) (int, error) {
		if len(p) < int(chunker.MinSize()) {
			if saw_minsize != false {
				t.Fatalf(`chunker return a chunk below MinSize before last chunk: %d < %d`, len(p), int(chunker.MinSize()))
			} else {
				saw_minsize = true
			}
		}
		if len(p) > int(chunker.MaxSize()) {
			t.Fatalf(`chunker return a chunk above MaxSize`)
		}
		hasher.Write(p)
		return len(p), nil
	})
	chunker.Copy(w)
	sum2 := hasher.Sum(nil)

	if !bytes.Equal(sum1, sum2) {
		t.Fatalf(`chunker produces incorrect output`)
	}
}

func Test_FastCDC_Split(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("fastcdc", r, nil)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}

	saw_minsize := false
	w := func(offset, length uint, chunk []byte) error {
		if len(chunk) < int(chunker.MinSize()) {
			if saw_minsize != false {
				t.Fatalf(`chunker return a chunk below MinSize before last chunk: %d < %d`, len(chunk), int(chunker.MinSize()))
			} else {
				saw_minsize = true
			}
		}
		if len(chunk) > int(chunker.MaxSize()) {
			t.Fatalf(`chunker return a chunk above MaxSize`)
		}
		hasher.Write(chunk)
		return nil
	}
	err = chunker.Split(w)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}
	sum2 := hasher.Sum(nil)

	if !bytes.Equal(sum1, sum2) {
		t.Fatalf(`chunker produces incorrect output`)
	}
}

func Test_JC_Next(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("jc", r, nil)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}
	for err := error(nil); err == nil; {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			t.Fatalf(`chunker error: %s`, err)
		}
		if len(chunk) < int(chunker.MinSize()) && err != io.EOF {
			t.Fatalf(`chunker return a chunk below MinSize before last chunk: %s`, err)
		}
		if len(chunk) > int(chunker.MaxSize()) {
			t.Fatalf(`chunker return a chunk above MaxSize`)
		}
		hasher.Write(chunk)
		if err == io.EOF {
			break
		}
	}
	sum2 := hasher.Sum(nil)

	if !bytes.Equal(sum1, sum2) {
		t.Fatalf(`chunker produces incorrect output`)
	}
}

func Test_JC_Copy(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("jc", r, nil)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}

	saw_minsize := false
	w := writerFunc(func(p []byte) (int, error) {
		if len(p) < int(chunker.MinSize()) {
			if saw_minsize != false {
				t.Fatalf(`chunker return a chunk below MinSize before last chunk: %d < %d`, len(p), int(chunker.MinSize()))
			} else {
				saw_minsize = true
			}
		}
		if len(p) > int(chunker.MaxSize()) {
			t.Fatalf(`chunker return a chunk above MaxSize`)
		}
		hasher.Write(p)
		return len(p), nil
	})
	chunker.Copy(w)
	sum2 := hasher.Sum(nil)

	if !bytes.Equal(sum1, sum2) {
		t.Fatalf(`chunker produces incorrect output`)
	}
}

func Test_JC_Split(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("jc", r, nil)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}

	saw_minsize := false
	w := func(offset, length uint, chunk []byte) error {
		if len(chunk) < int(chunker.MinSize()) {
			if saw_minsize != false {
				t.Fatalf(`chunker return a chunk below MinSize before last chunk: %d < %d`, len(chunk), int(chunker.MinSize()))
			} else {
				saw_minsize = true
			}
		}
		if len(chunk) > int(chunker.MaxSize()) {
			t.Fatalf(`chunker return a chunk above MaxSize`)
		}
		hasher.Write(chunk)
		return nil
	}
	err = chunker.Split(w)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}
	sum2 := hasher.Sum(nil)

	if !bytes.Equal(sum1, sum2) {
		t.Fatalf(`chunker produces incorrect output`)
	}
}

func Test_UltraCDC(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("ultracdc", r, nil)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}
	for err := error(nil); err == nil; {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			t.Fatalf(`chunker error: %s`, err)
		}
		if len(chunk) < int(chunker.MinSize()) && err != io.EOF {
			t.Fatalf(`chunker return a chunk below MinSize before last chunk: %s`, err)
		}
		if len(chunk) > int(chunker.MaxSize()) {
			t.Fatalf(`chunker return a chunk above MaxSize`)
		}
		hasher.Write(chunk)
		if err == io.EOF {
			break
		}
	}
	sum2 := hasher.Sum(nil)

	if !bytes.Equal(sum1, sum2) {
		t.Fatalf(`chunker produces incorrect output`)
	}
}

func Test_UltraCDC_Copy(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("ultracdc", r, nil)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}

	saw_minsize := false
	w := writerFunc(func(p []byte) (int, error) {
		if len(p) < int(chunker.MinSize()) {
			if saw_minsize != false {
				t.Fatalf(`chunker return a chunk below MinSize before last chunk: %d < %d`, len(p), int(chunker.MinSize()))
			} else {
				saw_minsize = true
			}
		}
		if len(p) > int(chunker.MaxSize()) {
			t.Fatalf(`chunker return a chunk above MaxSize`)
		}
		hasher.Write(p)
		return len(p), nil
	})
	chunker.Copy(w)
	sum2 := hasher.Sum(nil)

	if !bytes.Equal(sum1, sum2) {
		t.Fatalf(`chunker produces incorrect output`)
	}
}

func Test_UltraCDC_Split(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("ultracdc", r, nil)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}

	saw_minsize := false
	w := func(offset, length uint, chunk []byte) error {
		if len(chunk) < int(chunker.MinSize()) {
			if saw_minsize != false {
				t.Fatalf(`chunker return a chunk below MinSize before last chunk: %d < %d`, len(chunk), int(chunker.MinSize()))
			} else {
				saw_minsize = true
			}
		}
		if len(chunk) > int(chunker.MaxSize()) {
			t.Fatalf(`chunker return a chunk above MaxSize`)
		}
		hasher.Write(chunk)
		return nil
	}
	err = chunker.Split(w)
	if err != nil {
		t.Fatalf(`chunker error: %s`, err)
	}
	sum2 := hasher.Sum(nil)

	if !bytes.Equal(sum1, sum2) {
		t.Fatalf(`chunker produces incorrect output`)
	}
}

func Benchmark_Restic_Rabin_Next(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0
	buffer := make([]byte, restic.MaxSize)
	for i := 0; i < b.N; i++ {
		pol, err := restic.RandomPolynomial()
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		chunker := restic.New(r, pol)
		chunker.MinSize = minSize
		chunker.MaxSize = maxSize
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		for err := error(nil); err == nil; {
			_, err = chunker.Next(buffer)
			nchunks++
		}
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_Askeladdk_FastCDC_Copy(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	w := writerFunc(func(p []byte) (int, error) {
		nchunks++
		return len(p), nil
	})

	buf := make([]byte, maxSize<<1)
	for i := 0; i < b.N; i++ {
		_, _ = askeladdk.CopyBuffer(w, r, buf)
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_Jotfs_FastCDC_Next(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0
	for i := 0; i < b.N; i++ {
		chunker, err := jotfs.NewChunker(r, jotfs.Options{
			MinSize:     minSize,
			AverageSize: avgSize,
			MaxSize:     maxSize,
		})
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		for err := error(nil); err == nil; {
			_, err = chunker.Next()
			nchunks++
		}
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_Tigerwill90_FastCDC_Split(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0
	for i := 0; i < b.N; i++ {

		chunker, err := tigerwill90.NewChunker(context.Background(),
			tigerwill90.WithChunksSize(minSize, avgSize, maxSize))
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		err = chunker.Split(r, func(offset, length uint, chunk []byte) error {
			nchunks++
			return nil
		})
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_Mhofmann_FastCDC_Next(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0
	for i := 0; i < b.N; i++ {

		chunker, err := mhofmann.NewChunker(r, minSize, avgSize, maxSize)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}

		for hasChunk := chunker.Next(); hasChunk; hasChunk = chunker.Next() {
			// to be fair with other benchmarks, return and discard value
			// so that the implementation has to pass a buffer to caller.
			_ = chunker.Chunk()
			nchunks++
		}
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_PlakarKorp_FastCDC_Copy(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: avgSize,
		MaxSize:    maxSize,
	}

	w := writerFunc(func(p []byte) (int, error) {
		nchunks++
		return len(p), nil
	})

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("fastcdc", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		chunker.Copy(w)
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_PlakarKorp_FastCDC_Split(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: avgSize,
		MaxSize:    maxSize,
	}

	w := func(offset, length uint, chunk []byte) error {
		nchunks++
		return nil
	}

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("fastcdc", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		err = chunker.Split(w)
		if err != nil && err != io.EOF {
			b.Fatalf(`chunker error: %s`, err)
		}
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_PlakarKorp_FastCDC_Next(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: avgSize,
		MaxSize:    maxSize,
	}

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("fastcdc", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		for err := error(nil); err == nil; {
			_, err = chunker.Next()
			nchunks++
		}
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_PlakarKorp_UltraCDC_Copy(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: minSize + (8 << 10),
		MaxSize:    maxSize,
	}

	w := writerFunc(func(p []byte) (int, error) {
		nchunks++
		return len(p), nil
	})

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("ultracdc", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		chunker.Copy(w)
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_PlakarKorp_UltraCDC_Split(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: minSize + (8 << 10),
		MaxSize:    maxSize,
	}

	w := func(offset, length uint, chunk []byte) error {
		nchunks++
		return nil
	}

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("ultracdc", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		err = chunker.Split(w)
		if err != nil && err != io.EOF {
			b.Fatalf(`chunker error: %s`, err)
		}
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_PlakarKorp_UltraCDC_Next(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: minSize + (8 << 10),
		MaxSize:    maxSize,
	}

	b.ResetTimer()
	nchunks := 0
	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("ultracdc", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		for err := error(nil); err == nil; {
			_, err = chunker.Next()
			nchunks++
		}
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_PlakarKorp_JC_Copy(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: avgSize,
		MaxSize:    maxSize,
	}

	w := writerFunc(func(p []byte) (int, error) {
		nchunks++
		return len(p), nil
	})

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("jc", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		chunker.Copy(w)
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_PlakarKorp_JC_Split(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: avgSize,
		MaxSize:    maxSize,
	}

	w := func(offset, length uint, chunk []byte) error {
		nchunks++
		return nil
	}

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("jc", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		err = chunker.Split(w)
		if err != nil && err != io.EOF {
			b.Fatalf(`chunker error: %s`, err)
		}
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_PlakarKorp_JC_Next(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: avgSize,
		MaxSize:    maxSize,
	}

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("jc", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		for err := error(nil); err == nil; {
			_, err = chunker.Next()
			nchunks++
		}
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}
