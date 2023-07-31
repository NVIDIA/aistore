## Map Benchmark

This benchmark tests the performance of regular maps versus sync.Maps focusing on the primitive operations of get, put, and delete while under contention. For the use case of AIStore, we want the read times to be as fast as possible.

### Getting started

Run

```console
$ go run main.go -maxsize 1000000
```

This will start the benchmark with the map size to be 1 million elements. 

The list of available options are:

* `-iter` int - Number of iterations to run (default 10)
* `-maxsize` int - Maximum size of map (default 1000000)
* `-minsize` int - Shrink size of map (default 10000)
* `-numread` int - Number of random reads (default 200000)
* `-putpct` int - Percentage of puts (default 10)
* `-shrink` bool - Enable shrink (deletion) of map keys (default true)
* `-workers` int - Number of Read/Write go-routines (default 8)

### Result

From the results of the benchmark, `sync.Map` does appear to out perform regular maps for read operations when there is contention from multiple go routines. The performance of `sync.Map` for put and delete operations are usually slower than regular maps, but if we expect > `80%` of the requests to be reads, sync.Map will be faster than regular maps. Overall, if we expect that the maps will have much higher read requests than put/delete requests, `sync.Maps` are the better option.