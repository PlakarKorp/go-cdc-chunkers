# go-cdc-chunkers benchmarks

Comparison benchmarks and chunk-distribution plots for
[go-cdc-chunkers](https://github.com/PlakarKorp/go-cdc-chunkers).

This directory is a **separate, nested Go module**
(`github.com/PlakarKorp/go-cdc-chunkers/benchmarks`). It is intentionally kept
out of the parent module's dependency graph: it pulls in a plotting stack
(`gonum.org/v1/plot`) and several competing CDC implementations purely for
comparison. None of those dependencies leak into the parent module, so
consumers of `go-cdc-chunkers` never download them.

A `replace github.com/PlakarKorp/go-cdc-chunkers => ../` directive makes the
benchmarks run against the in-tree version of the library.

## Usage

All commands run from this directory.

### Comparison benchmarks

Benchmarks `go-cdc-chunkers` against `restic/chunker`, and the `askeladdk`,
`jotfs`, `tigerwill90`, and `mhofmann` FastCDC implementations:

```sh
go test -run=^$ -bench=. -benchmem
```

### Chunk-size distribution plot

`main.go` chunks random data with the requested algorithm(s) and writes a
scatter plot of chunk sizes:

```sh
go run . -min 2048 -avg 8192 -max 65536 fastcdc ultracdc jc
```

### Cutpoint dump

`cutpoints/` prints the offsets at which a chunker cuts:

```sh
go run ./cutpoints -min 4096 -avg 16384 -max 65536 -chunker fastcdc < input
```

### benchplot

`benchplot/` renders `go test -bench` output (ns/op, throughput, chunk count)
into plots:

```sh
go test -run=^$ -bench=. | tee benchmark.txt
go run ./benchplot < benchmark.txt
```
