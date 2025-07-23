package tests

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"io"
	"math/rand"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc4stadia"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/jc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
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

func Test_LegacyFastCDC_Next(t *testing.T) {
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

func Test_LegacyFastCDC_Copy(t *testing.T) {
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

func Test_LegacyFastCDC_Split(t *testing.T) {
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

func Test_LegacyFastCDC4Stadia_Next(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("fastcdc4stadia", r, nil)
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

func Test_LegacyFastCDC4Stadia_Copy(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("fastcdc4stadia", r, nil)
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

func Test_LegacyFastCDC4Stadia_Split(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("fastcdc4stadia", r, nil)
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

func Test_FastCDC_Next(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, nil)
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

	chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, nil)
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

	chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, nil)
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

func Test_LegacyJC_Next(t *testing.T) {
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

func Test_LegacyJC_Copy(t *testing.T) {
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

func Test_LegacyJC_Split(t *testing.T) {
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

func Test_JC_Next(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	chunker, err := chunkers.NewChunker("jc-v1.0.0", r, nil)
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

	chunker, err := chunkers.NewChunker("jc-v1.0.0", r, nil)
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

	chunker, err := chunkers.NewChunker("jc-v1.0.0", r, nil)
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

func Test_LegacyKFastCDC_Next(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	key := make([]byte, 32)
	crand.Read(key)

	chunker, err := chunkers.NewChunker("kfastcdc", r, &chunkers.ChunkerOpts{
		Key: key,
	})
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

func Test_LegacyKFastCDC_Copy(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	key := make([]byte, 32)
	crand.Read(key)

	chunker, err := chunkers.NewChunker("kfastcdc", r, &chunkers.ChunkerOpts{
		Key: key,
	})
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

func Test_LegacyKFastCDC_Split(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	key := make([]byte, 32)
	crand.Read(key)

	chunker, err := chunkers.NewChunker("kfastcdc", r, &chunkers.ChunkerOpts{
		Key: key,
	})
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

func Test_KeyedFastCDC_Next(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	key := make([]byte, 32)
	crand.Read(key)

	chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, &chunkers.ChunkerOpts{
		Key: key,
	})
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

func Test_KeyedFastCDC_Copy(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	key := make([]byte, 32)
	crand.Read(key)

	chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, &chunkers.ChunkerOpts{
		Key: key,
	})
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

func Test_KeyedFastCDC_Split(t *testing.T) {
	r := bytes.NewReader(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	sum1 := hasher.Sum(nil)

	hasher.Reset()

	key := make([]byte, 32)
	crand.Read(key)

	chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, &chunkers.ChunkerOpts{
		Key: key,
	})
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

func Benchmark_PlakarKorp_LegacyFastCDC_Copy(b *testing.B) {
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

func Benchmark_PlakarKorp_LegacyFastCDC_Split(b *testing.B) {
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

func Benchmark_PlakarKorp_LegacyFastCDC_Next(b *testing.B) {
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

func Benchmark_PlakarKorp_LegacyKFastCDC_Copy(b *testing.B) {
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

func Benchmark_PlakarKorp_LegacyKFastCDC_Split(b *testing.B) {
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

	w := func(offset, length uint, chunk []byte) error {
		nchunks++
		return nil
	}

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("kfastcdc", r, opts)
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

func Benchmark_PlakarKorp_LegacyKFastCDC_Next(b *testing.B) {
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

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("kfastcdc", r, opts)
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

func Benchmark_PlakarKorp_FastCDC4Stadia_Copy(b *testing.B) {
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

func Benchmark_PlakarKorp_FastCDC4Stadia_Split(b *testing.B) {
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
		chunker, err := chunkers.NewChunker("fastcdc4stadia", r, opts)
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

func Benchmark_PlakarKorp_FastCDC4Stadia_Next(b *testing.B) {
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
		chunker, err := chunkers.NewChunker("fastcdc4stadia", r, opts)
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
		chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, opts)
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
		chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, opts)
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
		chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, opts)
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

func Benchmark_PlakarKorp_KeyedFastCDC_Copy(b *testing.B) {
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

func Benchmark_PlakarKorp_KeyedFastCDC_Split(b *testing.B) {
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

	w := func(offset, length uint, chunk []byte) error {
		nchunks++
		return nil
	}

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, opts)
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

func Benchmark_PlakarKorp_KeyedFastCDC_Next(b *testing.B) {
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

	for i := 0; i < b.N; i++ {
		chunker, err := chunkers.NewChunker("fastcdc-v1.0.0", r, opts)
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

func Benchmark_PlakarKorp_LegacyJC_Copy(b *testing.B) {
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

func Benchmark_PlakarKorp_LegacyJC_Split(b *testing.B) {
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

func Benchmark_PlakarKorp_LegacyJC_Next(b *testing.B) {
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
		chunker, err := chunkers.NewChunker("jc-v1.0.0", r, opts)
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
		chunker, err := chunkers.NewChunker("jc-v1.0.0", r, opts)
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
		chunker, err := chunkers.NewChunker("jc-v1.0.0", r, opts)
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
