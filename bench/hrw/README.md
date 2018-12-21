# Benchmaking various HRW variants

This module provides a way to benchmark different HRW variants. The following approaches were considered for comparison:
1. Using XXHash for computation of checksum
2. Using XXHash + [xorshift64*](https://en.wikipedia.org/wiki/Xorshift#xorshift*) for computation of checksum
3. Using XXHash + [xoshiro256**](http://xoshiro.di.unimi.it/) for computation of checksum

For a detailed analysis of the experiment results, please refer to this [PDF](experiments.pdf).

## How to run tests and benchmarks?

```bash
$ go test -tags hrw -bench=. 
```