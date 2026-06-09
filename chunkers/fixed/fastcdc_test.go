package fixed

import (
	"bytes"
	"io"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

func TestFixedDefaultOptions(t *testing.T) {
	fixed := newFixed()
	opts := fixed.DefaultOptions()

	if opts.MinSize != 64*1024 {
		t.Errorf("Expected MinSize to be 64KB, got %d", opts.MinSize)
	}
	if opts.MaxSize != 64*1024 {
		t.Errorf("Expected MaxSize to be 64KB, got %d", opts.MaxSize)
	}
	if opts.NormalSize != 64*1024 {
		t.Errorf("Expected NormalSize to be 64KB, got %d", opts.NormalSize)
	}
	if opts.Key != nil {
		t.Errorf("Expected Key to be nil, got %v", opts.Key)
	}
}

func TestFixedValidate(t *testing.T) {
	fixed := newFixed()

	tests := []struct {
		name    string
		opts    *chunkers.ChunkerOpts
		wantErr error
	}{
		{
			name: "valid 64B",
			opts: &chunkers.ChunkerOpts{
				MinSize:    64,
				MaxSize:    64,
				NormalSize: 64,
			},
			wantErr: nil,
		},
		{
			name: "valid 64KB",
			opts: &chunkers.ChunkerOpts{
				MinSize:    64 * 1024,
				MaxSize:    64 * 1024,
				NormalSize: 64 * 1024,
			},
			wantErr: nil,
		},
		{
			name: "valid 1GB",
			opts: &chunkers.ChunkerOpts{
				MinSize:    1024 * 1024 * 1024,
				MaxSize:    1024 * 1024 * 1024,
				NormalSize: 1024 * 1024 * 1024,
			},
			wantErr: nil,
		},
		{
			name: "zero NormalSize",
			opts: &chunkers.ChunkerOpts{
				NormalSize: 0,
			},
			wantErr: ErrChunkSize,
		},
		{
			name: "NormalSize too small",
			opts: &chunkers.ChunkerOpts{
				NormalSize: 32,
			},
			wantErr: ErrChunkSize,
		},
		{
			name: "NormalSize too large",
			opts: &chunkers.ChunkerOpts{
				NormalSize: 1024*1024*1024 + 1,
			},
			wantErr: ErrChunkSize,
		},
		{
			name: "NormalSize not power of two",
			opts: &chunkers.ChunkerOpts{
				NormalSize: 96 * 1024,
			},
			wantErr: ErrNotPowerOfTwo,
		},
		{
			name: "MinSize differing from NormalSize is rejected",
			opts: &chunkers.ChunkerOpts{
				MinSize:    32 * 1024,
				MaxSize:    64 * 1024,
				NormalSize: 64 * 1024,
			},
			wantErr: ErrFixedSize,
		},
		{
			name: "MaxSize differing from NormalSize is rejected",
			opts: &chunkers.ChunkerOpts{
				MinSize:    64 * 1024,
				MaxSize:    128 * 1024,
				NormalSize: 64 * 1024,
			},
			wantErr: ErrFixedSize,
		},
		{
			name: "unset Min/Max is rejected (Setup is what populates them)",
			opts: &chunkers.ChunkerOpts{
				NormalSize: 64 * 1024,
			},
			wantErr: ErrFixedSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fixed.Validate(tt.opts)
			if err != tt.wantErr {
				t.Errorf("Validate() error = %#v, wantErr %#v", err, tt.wantErr)
			}
		})
	}
}

func TestFixedSetup(t *testing.T) {
	fixed := newFixed()

	t.Run("default setup", func(t *testing.T) {
		opts := &chunkers.ChunkerOpts{
			NormalSize: 0,
		}

		err := fixed.Setup(opts)
		if err != nil {
			t.Fatalf("Setup() error = %v", err)
		}

		if opts.NormalSize != 64*1024 {
			t.Errorf("Expected NormalSize to default to 64KB, got %d", opts.NormalSize)
		}
		// Setup mirrors the single size onto Min/Max for the framework.
		if opts.MinSize != 64*1024 {
			t.Errorf("Expected MinSize to be mirrored to 64KB, got %d", opts.MinSize)
		}
		if opts.MaxSize != 64*1024 {
			t.Errorf("Expected MaxSize to be mirrored to 64KB, got %d", opts.MaxSize)
		}
	})

	t.Run("custom valid setup", func(t *testing.T) {
		opts := &chunkers.ChunkerOpts{
			NormalSize: 128 * 1024,
		}

		err := fixed.Setup(opts)
		if err != nil {
			t.Fatalf("Setup() error = %v", err)
		}

		if opts.NormalSize != 128*1024 || opts.MinSize != 128*1024 || opts.MaxSize != 128*1024 {
			t.Errorf("Setup() did not mirror NormalSize onto Min/Max: %+v", opts)
		}
	})

	t.Run("invalid setup propagates validation error", func(t *testing.T) {
		opts := &chunkers.ChunkerOpts{
			NormalSize: 1000,
		}

		err := fixed.Setup(opts)
		if err != ErrNotPowerOfTwo {
			t.Fatalf("Setup() error = %v, want %v", err, ErrNotPowerOfTwo)
		}
	})
}

func TestFixedAlgorithm(t *testing.T) {
	fixed := newFixed()

	t.Run("returns n when n smaller than chunk size", func(t *testing.T) {
		opts := &chunkers.ChunkerOpts{
			NormalSize: 64 * 1024,
		}

		data := make([]byte, 32*1024)
		cutpoint := fixed.Algorithm(opts, data, len(data))
		if cutpoint != len(data) {
			t.Errorf("Expected cutpoint %d, got %d", len(data), cutpoint)
		}
	})

	t.Run("returns chunk size when n larger than chunk size", func(t *testing.T) {
		opts := &chunkers.ChunkerOpts{
			NormalSize: 64 * 1024,
		}

		data := make([]byte, 128*1024)
		cutpoint := fixed.Algorithm(opts, data, len(data))
		if cutpoint != 64*1024 {
			t.Errorf("Expected cutpoint 64KB, got %d", cutpoint)
		}
	})

	t.Run("returns chunk size when n equals chunk size", func(t *testing.T) {
		opts := &chunkers.ChunkerOpts{
			NormalSize: 64 * 1024,
		}

		data := make([]byte, 64*1024)
		cutpoint := fixed.Algorithm(opts, data, len(data))
		if cutpoint != 64*1024 {
			t.Errorf("Expected cutpoint 64KB, got %d", cutpoint)
		}
	})
}

func TestFixedChunking(t *testing.T) {
	data := make([]byte, 200*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	opts := &chunkers.ChunkerOpts{
		NormalSize: 64 * 1024,
	}

	reader := bytes.NewReader(data)
	chunker, err := chunkers.NewChunker("fixed-v1.0.0", reader, opts)
	if err != nil {
		t.Fatalf("Failed to create chunker: %v", err)
	}

	if chunker.MinSize() != 64*1024 {
		t.Errorf("Expected MinSize 64KB, got %d", chunker.MinSize())
	}
	if chunker.MaxSize() != 64*1024 {
		t.Errorf("Expected MaxSize 64KB, got %d", chunker.MaxSize())
	}
	if chunker.NormalSize() != 64*1024 {
		t.Errorf("Expected NormalSize 64KB, got %d", chunker.NormalSize())
	}

	var chunks [][]byte
	for {
		chunk, err := chunker.Next()
		if err != nil {
			if err == io.EOF {
				if len(chunk) > 0 {
					chunks = append(chunks, chunk)
				}
				break
			}
			t.Fatalf("Next() error: %v", err)
		}
		chunks = append(chunks, chunk)
	}

	if len(chunks) == 0 {
		t.Fatal("No chunks were generated")
	}

	for i, chunk := range chunks {
		if i != len(chunks)-1 && len(chunk) != 64*1024 {
			t.Errorf("Chunk %d size %d, expected exactly 64KB", i, len(chunk))
		}
		if i == len(chunks)-1 && len(chunk) > 64*1024 {
			t.Errorf("Last chunk size %d should not exceed 64KB", len(chunk))
		}
	}

	var reconstructed []byte
	for _, chunk := range chunks {
		reconstructed = append(reconstructed, chunk...)
	}

	if !bytes.Equal(data, reconstructed) {
		t.Fatal("Reconstructed data doesn't match original")
	}
}

func TestFixedChunkingWithDefaultOptions(t *testing.T) {
	data := make([]byte, 150*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	reader := bytes.NewReader(data)
	chunker, err := chunkers.NewChunker("fixed-v1.0.0", reader, nil)
	if err != nil {
		t.Fatalf("Failed to create chunker: %v", err)
	}

	var chunks [][]byte
	for {
		chunk, err := chunker.Next()
		if err != nil {
			if err == io.EOF {
				if len(chunk) > 0 {
					chunks = append(chunks, chunk)
				}
				break
			}
			t.Fatalf("Next() error: %v", err)
		}
		chunks = append(chunks, chunk)
	}

	if len(chunks) == 0 {
		t.Fatal("No chunks were generated")
	}

	for i, chunk := range chunks {
		if i != len(chunks)-1 && len(chunk) != 64*1024 {
			t.Errorf("Chunk %d size %d, expected exactly 64KB", i, len(chunk))
		}
	}
}

func TestFixedCopy(t *testing.T) {
	data := make([]byte, 150*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	reader := bytes.NewReader(data)
	chunker, err := chunkers.NewChunker("fixed-v1.0.0", reader, nil)
	if err != nil {
		t.Fatalf("Failed to create chunker: %v", err)
	}

	var buf bytes.Buffer
	written, err := chunker.Copy(&buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Copy() error: %v", err)
	}

	if written != int64(len(data)) {
		t.Errorf("Copy() wrote %d bytes, expected %d", written, len(data))
	}

	if !bytes.Equal(data, buf.Bytes()) {
		t.Fatal("Copied data doesn't match original")
	}
}

func TestFixedSplit(t *testing.T) {
	data := make([]byte, 150*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	reader := bytes.NewReader(data)
	chunker, err := chunkers.NewChunker("fixed-v1.0.0", reader, nil)
	if err != nil {
		t.Fatalf("Failed to create chunker: %v", err)
	}

	var chunks [][]byte
	var offsets []uint
	err = chunker.Split(func(offset, length uint, chunk []byte) error {
		chunks = append(chunks, append([]byte{}, chunk...))
		offsets = append(offsets, offset)
		return nil
	})
	if err != nil {
		t.Fatalf("Split() error: %v", err)
	}

	if len(chunks) == 0 {
		t.Fatal("No chunks were generated")
	}

	var reconstructed []byte
	for _, chunk := range chunks {
		reconstructed = append(reconstructed, chunk...)
	}

	if !bytes.Equal(data, reconstructed) {
		t.Fatal("Reconstructed data doesn't match original")
	}

	for i := 1; i < len(offsets); i++ {
		expectedOffset := offsets[i-1] + uint(len(chunks[i-1]))
		if offsets[i] != expectedOffset {
			t.Errorf("Offset %d should be %d, got %d", i, expectedOffset, offsets[i])
		}
	}
}

func TestFixedConsistency(t *testing.T) {
	data := make([]byte, 150*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	reader1 := bytes.NewReader(data)
	chunker1, err := chunkers.NewChunker("fixed-v1.0.0", reader1, nil)
	if err != nil {
		t.Fatalf("Failed to create first chunker: %v", err)
	}

	var chunks1 [][]byte
	for {
		chunk, err := chunker1.Next()
		if err != nil {
			if err == io.EOF {
				if len(chunk) > 0 {
					chunks1 = append(chunks1, chunk)
				}
				break
			}
			t.Fatalf("First chunker Next() error: %v", err)
		}
		chunks1 = append(chunks1, chunk)
	}

	reader2 := bytes.NewReader(data)
	chunker2, err := chunkers.NewChunker("fixed-v1.0.0", reader2, nil)
	if err != nil {
		t.Fatalf("Failed to create second chunker: %v", err)
	}

	var chunks2 [][]byte
	for {
		chunk, err := chunker2.Next()
		if err != nil {
			if err == io.EOF {
				if len(chunk) > 0 {
					chunks2 = append(chunks2, chunk)
				}
				break
			}
			t.Fatalf("Second chunker Next() error: %v", err)
		}
		chunks2 = append(chunks2, chunk)
	}

	if len(chunks1) != len(chunks2) {
		t.Fatalf("Different number of chunks: %d vs %d", len(chunks1), len(chunks2))
	}

	for i := range chunks1 {
		if !bytes.Equal(chunks1[i], chunks2[i]) {
			t.Fatalf("Chunk %d differs between runs", i)
		}
	}
}

func BenchmarkFixedAlgorithm(b *testing.B) {
	fixed := newFixed()
	opts := &chunkers.ChunkerOpts{
		NormalSize: 64 * 1024,
	}

	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fixed.Algorithm(opts, data, len(data))
	}
}

func BenchmarkFixedSetup(b *testing.B) {
	fixed := newFixed()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opts := &chunkers.ChunkerOpts{
			NormalSize: 64 * 1024,
		}
		err := fixed.Setup(opts)
		if err != nil {
			b.Fatalf("Setup() error = %v", err)
		}
	}
}
