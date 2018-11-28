Mem2: SGL allocator and memory manager
-----------------------------------------------------------------

## Overview


Mem2 is, simultaneously, a) Slab and SGL allocator, and b) memory manager
responsible to optimize memory usage between different (more vs less) utilized
Slabs.

Multiple Mem2 instances may coexist in the system, each having its own
constraints and managing its own Slabs and SGLs.

Mem2 is a "runner" that can be Run() to monitor system resources, automatically
adjust Slab sizes based on their respective usages, and incrementally
deallocate idle Slabs.

There will be use cases, however, when actually running a Mem2 instance
won't be necessary: e.g., when an app utilizes a single (or a few distinct)
Slab size(s) for the duration of its relatively short lifecycle,
while at the same time preferring minimal interference with other running apps.

## Construction

In that sense, a typical initialization sequence includes 2 or 3 steps, e.g.:
1) construct:
```go
	mem2 := &memsys.Mem2{Period: ..., MinPctFree: ..., Name: ..., Debug: ...}
```
2) initialize:
```go
	err := mem2.Init()
	if err != nil {
		...
	}
```
3) optionally, run:
```go
	go mem2.Run()
```

In addition, there are several environment variables that can be used
(to circumvent the need to change the code, for instance):
```shell
	DFC_MINMEM_FREE
	DFC_MINMEM_PCT_TOTAL
	DFC_MINMEM_PCT_FREE
	DFC_MEM_DEBUG
```
These names must be self-explanatory.

## Termination

If the memory manager is no longer needed, terminating the Mem2 instance is recommended.
This will free up all the slabs allocated to the memory manager instance.
Halt a running or initialized Mem2 instance is done by:
```go
    mem2.Stop(nil)
```
Note that `nil` is used to denote that the Mem2 instance termination was done intentionally as part of cleanup.

## Operation

Once constructed and initialized, memory-manager-and-slab-allocator
(Mem2, for shortness) can be exercised via its public API that includes
`GetSlab2`, `SelectSlab2` and `AllocFromSlab2`. Notice the difference between
the first and the second: `GetSlab2(128KB)` will return the Slab that contains
size=128KB reusable buffers, while `SelectSlab2(128KB)` - the Slab that is
considered optimal for the (estimated) total size 128KB.

Once selected, each Slab2 instance can be used via its own public API that
includes `Alloc` and `Free` methods. In addition, each allocated SGL internally
utilizes one of the existing enumerated slabs to "grow" (that is, allocate more
buffers from the slab) on demand. For details, look for "grow" in the iosgl.go.

When being run (as in: `go mem2.Run()`), the memory manager periodically evaluates
the remaining free memory resource and adjusts its slabs accordingly.
The entire logic is consolidated in one `work()` method that can, for instance,
"cleanup" (see `cleanup()`) an existing "idle" slab,
or forcefully "reduce" (see `reduce()`) one if and when the amount of free
memory falls below watermark.

## Testing

* To run all tests while redirecting errors to standard error:
```
go test -v -logtostderr=true
```

* To run one of the named tests for 100 seconds:

```
go test -v -logtostderr=true -run=Test_Sleep -duration=100s
```

* All tests for 2 minutes with verbose tracing and debug (i.e., assertions) enabled:

```
DFC_MEM_DEBUG=1 go test -v -logtostderr=true -duration=2m
```

## Global Memory Manager

In the interest of reusing a single memory manager instance across multiple packages outside the dfc core package, the memsys package declares a `gMem2` variable that can be accessed through the matching exported Getter.
The notable runtime parameters that are used for the global memory manager are MinFreePct and Period which are set to 50% and 2 minutes, respectively.
Note that more specialized use cases which warrant custom memory managers with finely tuned parameters are free to create their own separate `Mem2` instances.

Usage:

To access the global memory manager, a single call to `memsys.Init()` is all that is required. Separate `Init()` nor `Run()` calls should not be made on the returned Mem2 instance.