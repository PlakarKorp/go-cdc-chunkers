package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"io"
	"math/rand"
	"testing"

	mhofmann "codeberg.org/mhofmann/fastcdc"
	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc4stadia"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
	askeladdk "github.com/askeladdk/fastcdc"
	jotfs "github.com/jotfs/fastcdc-go"
	restic "github.com/restic/chunker"
	tigerwill90 "github.com/tigerwill90/fastcdc"
)

const (
	minSize = 2 * 1024
	maxSize = 64 * 1024
	avgSize = 8 * 1024

	datalen = 1024 << 20
)

type writerFunc func([]byte) (int, error)

func (fn writerFunc) Write(p []byte) (int, error) {
	return fn(p)
}

var rb, _ = io.ReadAll(io.LimitReader(rand.New(rand.NewSource(0)), datalen))

func Benchmark_Restic_Rabin(b *testing.B) {
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

func Benchmark_Askeladdk_FastCDC(b *testing.B) {
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

func Benchmark_Jotfs_FastCDC(b *testing.B) {
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

func Benchmark_Tigerwill90_FastCDC(b *testing.B) {
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

func Benchmark_Mhofmann_FastCDC(b *testing.B) {
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

func Benchmark_Plakar_LegacyFastCDC(b *testing.B) {
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

func Benchmark_Plakar_LegacyKFastCDC(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	key := make([]byte, 32)
	crand.Read(key)

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: avgSize,
		MaxSize:    maxSize,
		Key:        key,
	}

	w := writerFunc(func(p []byte) (int, error) {
		nchunks++
		return len(p), nil
	})

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("kfastcdc", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		chunker.Copy(w)
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_Plakar_FastCDC4Stadia(b *testing.B) {
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
		chunker, err := chunkers.NewChunker("fastcdc4stadia", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		chunker.Copy(w)
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_Plakar_FastCDC(b *testing.B) {
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
		chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		chunker.Copy(w)
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_Plakar_KeyedFastCDC(b *testing.B) {
	r := bytes.NewReader(rb)
	b.SetBytes(int64(r.Len()))
	b.ResetTimer()
	nchunks := 0

	key := make([]byte, 32)
	crand.Read(key)

	opts := &chunkers.ChunkerOpts{
		MinSize:    minSize,
		NormalSize: avgSize,
		MaxSize:    maxSize,
		Key:        key,
	}

	w := writerFunc(func(p []byte) (int, error) {
		nchunks++
		return len(p), nil
	})

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		chunker.Copy(w)
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}

func Benchmark_Plakar_UltraCDC(b *testing.B) {
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

func Benchmark_Plakar_LegacyJC(b *testing.B) {
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

/*
func Benchmark_Plakar_JC(b *testing.B) {
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
		chunker, err := chunkers.NewChunker("jc-v1.0.0", r, opts)
		if err != nil {
			b.Fatalf(`chunker error: %s`, err)
		}
		chunker.Copy(w)
		r.Reset(rb)
	}
	b.ReportMetric(float64(nchunks)/float64(b.N), "chunks")
}
*/
