## Memory Manager, Slab Allocator (MMSA)

MMSA is, simultaneously, a) Slab and SGL allocator, and b) memory manager
that is responsible to optimize memory usage between different (more vs less) utilized
Slabs.

Multiple MMSA instances may coexist in the system, each having its own
constraints and managing its own Slabs and SGLs.

MMSA includes a "house-keeping" part to monitor system resources,
adjust Slab sizes based on their respective usages, and incrementally
deallocate idle Slabs. To that end, MMSA utilizes `housekeep` (project and runner).

## Construction

A typical initialization sequence includes steps, e.g.:

1. Construct:

    ```go
    mm := &memsys.MMSA{Name: ..., TimeIval: ..., MinPctFree: ..., Name: ...}
    ```

    **Note** that with the only exception of `Name` all the rest member variables (above) have their system defaults and can be omitted.

2. Initialize:

    ```go
    err := mm.Init(false /* don't panic on error */)
    if err != nil {
        ...
    }
    ```

The example above shows initialization that ignores errors - in particular, insufficient minimum required memory (see the previous section).

Alternatively, MMSA can be initialized *not* to panic on errors:

```go
 mm.Init(true /* panic on error */)
```

In addition, there are several environment variables that can be used
(to circumvent the need to change the code, for instance):

```
AIS_MINMEM_FREE
AIS_MINMEM_PCT_TOTAL
AIS_MINMEM_PCT_FREE
```

## Minimum Available Memory

MMSA will try to make sure that there's a certain specified amount of memory that remains available at all times.
Following are the rules to set this minimum:

1. environment `AIS_MINMEM_FREE` takes precedence over everything else listed below;
2. if `AIS_MINMEM_FREE` is not defined, variables `AIS_MINMEM_PCT_TOTAL` and/or
 `AIS_MINMEM_PCT_FREE` define percentages to compute the minimum based on the total
  or the currently available memory, respectively;
3. with no environment, the minimum is computed based on the following MMSA member variables:
    * `MinFree`     - memory that must be available at all times
    * `MinPctTotal` - same, via percentage of total
    * `MinPctFree`  - ditto, as % of free at init time
    * Example:
        ```go
        memsys.MMSA{MinPctTotal: 4, MinFree: cmn.GiB * 2}
        ```
4. finally, if none of the above is specified, the constant `minMemFree` in the source

## Termination

If the memory manager is no longer needed, terminating the MMSA instance is recommended.
This will free up all the slabs allocated to the memory manager instance.
Halt a running or initialized MMSA instance is done by:

```go
mm.Terminate()
```

## Operation

Once constructed and initialized, memory-manager-and-slab-allocator (MMSA) can be exercised via its public API that includes `GetSlab`, on the one hand and `Alloc`/`AllocSize` on the other.

Notice the difference:
* `GetSlab(fixed-bufsize)` returns Slab that contains precisely fixed-bufsize sized reusable buffers
* `Alloc()` and `AllocSize()` return both a Slab and an already allocated buffer from this Slab.

Note as well that `Alloc()` uses default buffer size for a given MMSA, while `AllocSize()` accepts the specified size (as the name implies).

Once selected, each Slab can be used via its own public API that
includes `Alloc` and `Free` methods. In addition, each allocated SGL internally
utilizes one of the existing enumerated slabs to "grow" (that is, allocate more
buffers from the slab) on demand. For details, look for "grow" in the iosgl.go.

When running, the memory manager periodically evaluates
the remaining free memory resource and adjusts its slabs accordingly.
The entire logic is consolidated in one `work()` method that can, for instance,
"cleanup" (see `cleanup()`) an existing "idle" slab,
or forcefully "reduce" (see `reduce()`) one if and when the amount of free
memory falls below watermark.

## Testing

* **Run all tests in debug mode**:

```console
$ go test -v -logtostderr=true -duration 2m -tags=debug
```

* **Run one of the named tests for 100 seconds**:

```console
$ go test -v -run=Test_Sleep -duration=100s
```

* **Same as above with debug and deadbeef (build tags) enabled**:

```console
$ go test -v -tags=debug,deadbeef -run=Test_Sleep -duration=100s
```

* **Run each test for 10 minutes with the permission to use up to 90% of total RAM**

```console
$ AIS_MINMEM_PCT_TOTAL=10 go test -v -run=No -duration 10m -timeout=1h -tags=debug
```

## Global Memory Manager

In the interest of reusing a single memory manager instance across multiple packages outside the ais core package, the memsys package declares a `gMem2` variable that can be accessed through the matching exported Getter.
The notable runtime parameters that are used for the global memory manager are MinFreePct and TimeIval which are set to 50% and 2 minutes, respectively.
Note that more specialized use cases which warrant custom memory managers with finely tuned parameters are free to create their own separate `MMSA` instances.

Usage:

To access the global memory manager, a single call to `memsys.Init()` is all that is required. Separate `Init()` nor `Run()` calls should not be made on the returned MMSA instance.
