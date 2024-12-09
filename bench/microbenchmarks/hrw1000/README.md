```console
$ go test -bench=. -benchtime=10s
goos: linux
goarch: amd64
pkg: github.com/NVIDIA/aistore/bench/microbenchmarks/hrw1000
cpu: 11th Gen Intel(R) Core(TM) i7-11850H @ 2.50GHz

BenchmarkHRW/cluster[10]-bucket[4,000,000]-uname[80]-16                 554728353               19.12 ns/op
BenchmarkHRW/cluster[100]-bucket[4,000,000]-uname[80]-16                84540405               140.3 ns/op
BenchmarkHRW/cluster[1,000]-bucket[4,000,000]-uname[80]-16               7170206              1618 ns/op
BenchmarkHRW/cluster[10,000]-bucket[4,000,000]-uname[80]-16               600126             18721 ns/op
PASS
```

and one more time, for consistency:

```console
$ go test -bench=. -benchtime=10s
goos: linux
goarch: amd64
pkg: github.com/NVIDIA/aistore/bench/microbenchmarks/hrw1000
cpu: 11th Gen Intel(R) Core(TM) i7-11850H @ 2.50GHz
BenchmarkHRW/cluster[10]-bucket[4,000,000]-uname[80]-16                 613542771               18.86 ns/op
BenchmarkHRW/cluster[100]-bucket[4,000,000]-uname[80]-16                82590082               140.1 ns/op
BenchmarkHRW/cluster[1,000]-bucket[4,000,000]-uname[80]-16               7185032              1621 ns/op
BenchmarkHRW/cluster[10,000]-bucket[4,000,000]-uname[80]-16               589813             18785 ns/op
PASS
```
