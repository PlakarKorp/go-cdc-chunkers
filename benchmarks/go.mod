// This is a separate, nested Go module so that the comparison/plotting
// dependencies it pulls in (gonum plotting stack, competing CDC libraries)
// never leak into the parent github.com/PlakarKorp/go-cdc-chunkers module.
// Consumers of go-cdc-chunkers therefore never download any of these.
//
// The replace directive points at the parent so benchmarks always run
// against the in-tree version of the library.
module github.com/PlakarKorp/go-cdc-chunkers/benchmarks

go 1.23.4

require (
	codeberg.org/mhofmann/fastcdc v1.0.0
	github.com/PlakarKorp/go-cdc-chunkers v1.0.1-0.20250722160759-054417df78de
	github.com/askeladdk/fastcdc v0.0.2
	github.com/jotfs/fastcdc-go v0.2.0
	github.com/restic/chunker v0.4.0
	github.com/tigerwill90/fastcdc v1.2.2
	gonum.org/v1/plot v0.16.0
)

require (
	codeberg.org/go-fonts/liberation v0.5.0 // indirect
	codeberg.org/go-latex/latex v0.1.0 // indirect
	codeberg.org/go-pdf/fpdf v0.10.0 // indirect
	git.sr.ht/~sbinet/gg v0.6.0 // indirect
	github.com/ajstarks/svgo v0.0.0-20211024235047-1546f124cd8b // indirect
	github.com/campoy/embedmd v1.0.0 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/klauspost/cpuid/v2 v2.0.12 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/zeebo/blake3 v0.2.4 // indirect
	golang.org/x/image v0.25.0 // indirect
	golang.org/x/text v0.23.0 // indirect
)

replace github.com/PlakarKorp/go-cdc-chunkers => ../
