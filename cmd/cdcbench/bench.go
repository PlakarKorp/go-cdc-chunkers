package main

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

// Sample is one point in the time series captured while the benchmark runs.
type Sample struct {
	ElapsedSec  float64 `json:"elapsed_sec"`
	Files       int64   `json:"files"`
	Bytes       int64   `json:"bytes"`
	Chunks      int64   `json:"chunks"`
	RSSMB       float64 `json:"rss_mb"`        // current resident set size
	HeapInuseMB float64 `json:"heap_inuse_mb"` // live heap
	HeapSysMB   float64 `json:"heap_sys_mb"`   // heap reserved from the OS
	CPUSec      float64 `json:"cpu_sec"`       // cumulative user+sys CPU time
	NumGC       uint32  `json:"num_gc"`
}

// Result is the end-of-run summary (the "statistics" output).
type Result struct {
	Mode          string   `json:"mode"`
	Algorithm     string   `json:"algorithm"`
	Concurrency   int      `json:"concurrency"`
	Files         int64    `json:"files"`
	Chunks        int64    `json:"chunks"`
	Bytes         int64    `json:"bytes"`
	ElapsedSec    float64  `json:"elapsed_sec"`
	ThroughputMBs float64  `json:"throughput_mb_s"`
	PeakRSSMB     float64  `json:"peak_rss_mb"`
	HeapSysMB     float64  `json:"heap_sys_mb"`
	TotalAllocGB  float64  `json:"total_alloc_gb"`
	NumGC         uint32   `json:"num_gc"`
	CPUSec        float64  `json:"cpu_sec"`
	Samples       []Sample `json:"samples"`
}

// maxRSSBytes reads the process peak resident set size. Maxrss is bytes on
// macOS (Darwin) and kilobytes on Linux.
func maxRSSBytes() int64 {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	v := int64(ru.Maxrss)
	if runtime.GOOS == "linux" {
		v *= 1024
	}
	return v
}

// cpuSeconds returns cumulative user+system CPU time consumed by the process.
func cpuSeconds() float64 {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(ru.Utime) + tv(ru.Stime)
}

// currentRSSBytes approximates the current (not peak) RSS. getrusage only
// exposes the high-water mark portably, so we report HeapSys as a live proxy
// alongside it; for the time series we record both the running Maxrss and the
// live heap so the grapher can show either.
func currentRSSBytes() int64 { return maxRSSBytes() }

// BenchConfig parameterizes a run.
type BenchConfig struct {
	Root        string
	Algorithm   string
	Concurrency int
	Pooled      bool
	Opts        *chunkers.ChunkerOpts
	SampleEvery time.Duration
}

// Run walks cfg.Root and chunks every regular file with cfg.Concurrency
// workers, discarding the chunks. A background sampler records the time series.
func Run(cfg BenchConfig) (Result, error) {
	var files []string
	filepath.Walk(cfg.Root, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if fi.Mode().IsRegular() && fi.Size() > 0 {
			files = append(files, p)
		}
		return nil
	})

	pool := &sync.Pool{New: func() any { return make([]byte, cfg.Opts.MaxSize) }}

	var bytesProcessed, filesProcessed, chunks int64

	worker := func(work <-chan string, wg *sync.WaitGroup) {
		defer wg.Done()
		for p := range work {
			f, err := os.Open(p)
			if err != nil {
				continue
			}
			var ck *chunkers.Chunker
			var buf []byte
			if cfg.Pooled {
				buf = pool.Get().([]byte)
				ck, err = chunkers.NewChunkerBuffer(cfg.Algorithm, f, cfg.Opts, buf)
			} else {
				ck, err = chunkers.NewChunker(cfg.Algorithm, f, cfg.Opts)
			}
			if err != nil {
				f.Close()
				if buf != nil {
					pool.Put(buf)
				}
				continue
			}
			for {
				c, e := ck.Next()
				atomic.AddInt64(&bytesProcessed, int64(len(c)))
				atomic.AddInt64(&chunks, 1)
				if e != nil {
					break
				}
			}
			f.Close()
			if buf != nil {
				pool.Put(buf)
			}
			atomic.AddInt64(&filesProcessed, 1)
		}
	}

	// Background sampler.
	var samples []Sample
	stop := make(chan struct{})
	var samplerDone sync.WaitGroup
	start := time.Now()
	sample := func() Sample {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		const mb = 1 << 20
		return Sample{
			ElapsedSec:  time.Since(start).Seconds(),
			Files:       atomic.LoadInt64(&filesProcessed),
			Bytes:       atomic.LoadInt64(&bytesProcessed),
			Chunks:      atomic.LoadInt64(&chunks),
			RSSMB:       float64(currentRSSBytes()) / mb,
			HeapInuseMB: float64(ms.HeapInuse) / mb,
			HeapSysMB:   float64(ms.HeapSys) / mb,
			CPUSec:      cpuSeconds(),
			NumGC:       ms.NumGC,
		}
	}
	samplerDone.Add(1)
	go func() {
		defer samplerDone.Done()
		t := time.NewTicker(cfg.SampleEvery)
		defer t.Stop()
		samples = append(samples, sample())
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				samples = append(samples, sample())
			}
		}
	}()

	work := make(chan string, cfg.Concurrency*2)
	var wg sync.WaitGroup
	wg.Add(cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		go worker(work, &wg)
	}
	for _, p := range files {
		work <- p
	}
	close(work)
	wg.Wait()
	elapsed := time.Since(start)

	close(stop)
	samplerDone.Wait()
	samples = append(samples, sample()) // final point

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	const mb = 1 << 20

	mode := "default"
	if cfg.Pooled {
		mode = "pooled"
	}
	res := Result{
		Mode:          mode,
		Algorithm:     cfg.Algorithm,
		Concurrency:   cfg.Concurrency,
		Files:         filesProcessed,
		Chunks:        chunks,
		Bytes:         bytesProcessed,
		ElapsedSec:    elapsed.Seconds(),
		ThroughputMBs: (float64(bytesProcessed) / mb) / elapsed.Seconds(),
		PeakRSSMB:     float64(maxRSSBytes()) / mb,
		HeapSysMB:     float64(ms.HeapSys) / mb,
		TotalAllocGB:  float64(ms.TotalAlloc) / (1 << 30),
		NumGC:         ms.NumGC,
		CPUSec:        cpuSeconds(),
		Samples:       samples,
	}
	return res, nil
}
