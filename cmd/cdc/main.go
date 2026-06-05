/*
 * Copyright (c) 2025 Gilles Chehade <gilles@poolp.org>
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

// Command cdc analyzes and compares content-defined chunking algorithms. It
// depends only on the chunkers library (no plotting/heavy deps), so it can
// ship alongside the library itself.
//
// Subcommands:
//
//	cdc analyze  -chunker NAME [opts] FILE...
//	cdc compare  -a NAME -b NAME [opts] FILE...
//	cdc resync   -a NAME -b NAME [opts] [-edits N] FILE
package main

import (
	"fmt"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, `cdc - analyze and compare content-defined chunkers

usage:
  cdc analyze -chunker NAME [-min N -avg N -max N] FILE...
  cdc compare -a NAME -b NAME [-min N -avg N -max N] FILE...
  cdc resync  -a NAME -b NAME [-min N -avg N -max N] [-edits N] [-edit-size N] FILE

Common options:
  -min  minimum chunk size in bytes (default 2048)
  -avg  average/normal chunk size in bytes (default 8192)
  -max  maximum chunk size in bytes (default 65536)
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "analyze":
		err = runAnalyze(os.Args[2:])
	case "compare":
		err = runCompare(os.Args[2:])
	case "resync":
		err = runResync(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "cdc: %v\n", err)
		os.Exit(1)
	}
}
