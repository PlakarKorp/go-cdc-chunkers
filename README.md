# go-cdc-chunkers

This is an active project !

Feel free to join [our discord server](https://discord.gg/uuegtnF2Q5) or start discussions at Github.


## Overview
`go-cdc-chunkers` is a Golang package designed to provide unified access to multiple Content-Defined Chunking (CDC) algorithms.
With a simple and intuitive interface, users can effortlessly chunk data using their preferred CDC algorithm.

## Use-cases
Content-Defined Chunking (CDC) algorithms are used in data deduplication and backup systems to break up data into smaller chunks based on their content, rather than their size or location. This allows for more efficient storage and transfer of data, as identical chunks can be stored or transferred only once. CDC algorithms are useful because they can identify and isolate changes in data, making it easier to track and manage changes over time. Additionally, CDC algorithms can be optimized for performance, allowing for faster and more efficient processing of large amounts of data.


## Features
- Unified interface for multiple CDC algorithms.
- Supported algorithms: fastcdc, ultracdc.
- Efficient and optimized for performance.
- Comprehensive error handling.
- Supports KFastCDC, a Keyed variant of FastCDC for key-derived Gear

## Installation
```sh
go get github.com/PlakarKorp/go-cdc-chunkers
```


## Usage
Here's a basic example of how to use the package:

```go
    chunker, err := chunkers.NewChunker("fastcdc", rd)   // or ultracdc
    if err != nil {
        log.Fatal(err)
    }

    offset := 0
    for {
        chunk, err := chunker.Next()
        if err != nil && err != io.EOF {
            log.Fatal(err)
        }

        chunkLen := len(chunk)
        fmt.Println(offset, chunkLen)

        if err == io.EOF {
            // no more chunks to read
            break
        }
        offset += chunkLen
    }
```

## Benchmarks
Performances is a key feature in CDC, `go-cdc-chunkers` strives at optimizing its implementation of CDC algorithms,
finding the proper balance in usability, CPU-usage and memory-usage.

The following benchmark shows the performances of chunking 1GB of random data,
with a minimum chunk size of 256KB and a maximum chunk size of 1MB,
for multiple implementations available as well as multiple methods of consumption of the chunks:

```
goos: darwin
goarch: arm64
pkg: github.com/PlakarKorp/go-cdc-chunkers/tests
cpu: Apple M4 Pro
Benchmark_Restic_Rabin_Next-14                         1        1932542209 ns/op         555.61 MB/s          1300 chunks
Benchmark_Askeladdk_FastCDC_Copy-14                    2         579593250 ns/op        1852.58 MB/s        105327 chunks
Benchmark_Jotfs_FastCDC_Next-14                        3         448508056 ns/op        2394.03 MB/s          1725 chunks
Benchmark_Tigerwill90_FastCDC_Split-14                 3         377360430 ns/op        2845.40 MB/s          2013 chunks
Benchmark_Mhofmann_FastCDC_Next-14                     2         572578979 ns/op        1875.27 MB/s          1718 chunks
Benchmark_PlakarKorp_FastCDC_Copy-14                   9         117534472 ns/op        9135.55 MB/s          3647 chunks
Benchmark_PlakarKorp_FastCDC_Split-14                  9         117849120 ns/op        9111.16 MB/s          3647 chunks
Benchmark_PlakarKorp_FastCDC_Next-14                   9         117847486 ns/op        9111.28 MB/s          3647 chunks
Benchmark_PlakarKorp_KFastCDC_Copy-14                  9         118699393 ns/op        9045.89 MB/s          3646 chunks
Benchmark_PlakarKorp_KFastCDC_Split-14                 9         117607542 ns/op        9129.87 MB/s          3650 chunks
Benchmark_PlakarKorp_KFastCDC_Next-14                  9         115304560 ns/op        9312.22 MB/s          3639 chunks
Benchmark_PlakarKorp_UltraCDC_Copy-14                 15          80695064 ns/op        13306.16 MB/s         3955 chunks
Benchmark_PlakarKorp_UltraCDC_Split-14                14          79441967 ns/op        13516.05 MB/s         3955 chunks
Benchmark_PlakarKorp_UltraCDC_Next-14                 14          80221119 ns/op        13384.78 MB/s         3955 chunks
Benchmark_PlakarKorp_JC_Copy-14                       22          49784102 ns/op        21567.97 MB/s         4033 chunks
Benchmark_PlakarKorp_JC_Split-14                      22          49855737 ns/op        21536.98 MB/s         4033 chunks
Benchmark_PlakarKorp_JC_Next-14                       22          49993044 ns/op        21477.82 MB/s         4033 chunks
PASS
ok      github.com/PlakarKorp/go-cdc-chunkers/tests     44.269s
```

## Contributing
We welcome contributions!
If you have a feature request, bug report, or wish to contribute code, please open an issue or pull request.

## Support
If you find `go-cdc-chunkers` useful, please consider supporting its development by [sponsoring the project on GitHub](https://github.com/sponsors/poolpOrg).
Your support helps ensure the project's continued maintenance and improvement.


## License
This project is licensed under the ISC License. See the [LICENSE.md](LICENSE.md) file for details.


## Reference

  - [Xia, Wen, et al. "Fastcdc: a fast and efficient content-defined chunking approach for data deduplication." 2016 USENIX Annual Technical Conference](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)
  - [Zhou, Wang, Xia, Zhang "UltraCDC:A Fast and Stable Content-Defined Chunking Algorithm for Deduplication-based Backup Storage Systems" 2022 IEEE](https://ieeexplore.ieee.org/document/9894295)
  - [Xiaozhong Jin, Haikun Liu, Chencheng Ye, Xiaofei Liao, Hai Jin and Yu Zhang "Accelerating Content-Defined Chunking for Data Deduplication Based on Speculative Jump" IEEE TRANSACTIONS ON PARALLEL AND DISTRIBUTED SYSTEMS, VOL. 34, NO. 9, SEPTEMBER 2023](https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=10168293)
  
