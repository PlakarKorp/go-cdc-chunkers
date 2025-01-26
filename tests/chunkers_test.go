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

	minSize = 256 << 10
	maxSize = 1024 << 10
	avgSize = 512 << 10

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
