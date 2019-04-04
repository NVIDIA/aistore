// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
	sigar "github.com/cloudfoundry/gosigar"
)

const (
	memoryReservedInterval    = 50 * time.Millisecond
	memoryExcessInterval      = time.Second
	unreserveMemoryBufferSize = 10000
)

// memoryWatcher is responsible for monitoring memory changes and decide
// wether specific action should happen or not. It may also decide to return
type memoryWatcher struct {
	m *Manager

	excessTicker      *time.Ticker
	reservedTicker    *time.Ticker
	maxMemoryToUse    uint64
	reservedMemory    uint64
	memoryUsed        uint64 // memory used in specifc point in time, it is refreshed once in a while
	unreserveMemoryCh chan uint64
	stopCh            cmn.StopCh
}

func newMemoryWatcher(m *Manager, maxMemoryUsage uint64) *memoryWatcher {
	return &memoryWatcher{
		m: m,

		excessTicker:      time.NewTicker(memoryExcessInterval),
		reservedTicker:    time.NewTicker(memoryReservedInterval),
		maxMemoryToUse:    maxMemoryUsage,
		unreserveMemoryCh: make(chan uint64, unreserveMemoryBufferSize),
		stopCh:            cmn.NewStopCh(),
	}
}

func (mw *memoryWatcher) watch() error {
	mem := sigar.Mem{}
	if err := mem.Get(); err != nil {
		return err
	}
	mw.memoryUsed = mem.ActualUsed

	go mw.watchReserved()
	go mw.watchExcess(mem)
	return nil
}

func (mw *memoryWatcher) watchReserved() {
	// Starting memory updater. Since extraction phase is concurrent and we
	// cannot know how much memory will given compressed shard extract we need
	// to employ mechanism for updating memory. Just before extraction we
	// estimate how much memory shard will contain (by multiplying file size and
	// avg compress ratio). Then we update currently used memory to actual used
	// + reserved. After we finish extraction we put reserved memory for the
	// shard into the unreserve memory channel. Note that we cannot unreserve it
	// right away because actual used memory has not yet been updated (but it
	// surely changed). Once memory updater will fetch and update currently used
	// memory in system we can unreserve memory (it is already calculated in
	// newly fetched memory usage value). This way it is almost impossible to
	// exceed maximum memory which we are able to use (set by user) -
	// unfortunately it can happen when we underestimate the amount of memory
	// which we will use when extracting compressed file.
	for range mw.reservedTicker.C {
		curMem := sigar.Mem{}
		if err := curMem.Get(); err == nil {
			atomic.StoreUint64(&mw.memoryUsed, curMem.ActualUsed)

			unreserve := true
			for unreserve {
				select {
				case size := <-mw.unreserveMemoryCh:
					atomic.AddUint64(&mw.reservedMemory, ^uint64(size-1)) // decrement by size
				default:
					unreserve = false
				}
			}
		}
	}
}

// watchExcess watches memory in order to prevent exceeding provided by user
// limit. If the limit is exceeded watcher tries to spill memory resources
// to the disk.
//
// NOTE: We also watch the memory in `watchReserved` but this may be
// insufficient because there is more factors than just `SGL`s: `Records`,
// `Shards`, `RecordContents`, `ExtractionPaths` etc. All these structures
// require memory, sometimes it can be counted in GBs. That is why we also need
// excess watcher so that it prevents memory overuse.
func (mw *memoryWatcher) watchExcess(memStat sigar.Mem) {
	buf, slab := mem.AllocFromSlab2(cmn.MiB)
	defer slab.Free(buf)

	lastMemoryUsage := memStat.ActualUsed
	for {
		select {
		case <-mw.excessTicker.C:
			curMem := sigar.Mem{}
			if err := curMem.Get(); err != nil {
				continue
			}

			// We should look at difference of memory that have accumulated
			// during the interval. It would be incorrect to calculate
			// difference between `maxMemoryToUse - curMem.ActualUsed` and
			// treat as memory excess because Go's runtime does not return the
			// memory immediately to the system and for most of the time the
			// difference would be always `> 0` and never subside to anything
			// lower so we would free SGLs without seeing any improvement in
			// memory usage (we could even free all SGLs, which is absurd!)
			memExcess := int64(curMem.ActualUsed - lastMemoryUsage)
			lastMemoryUsage = curMem.ActualUsed

			if curMem.ActualUsed < mw.maxMemoryToUse {
				continue
			}

			// In case memory is exceeded spill sgls to disk
			rc, ep := mw.m.recManager.RecordContents(), mw.m.recManager.ExtractionPaths()
			rc.Range(func(path, value interface{}) bool {
				// No matter what the outcome we should store `path` in
				// `extractionPaths` to make sure that all files, even
				// incomplete ones, are deleted (if the file will not exist this
				// is not much of a problem).
				ep.Store(path, struct{}{})

				sgl := value.(*memsys.SGL)
				if err := cmn.SaveReader(path.(string), sgl, buf); err != nil {
					glog.Error(err)
				} else {
					rc.Delete(path)
					memExcess -= sgl.Size()
					sgl.Free()
				}
				return memExcess > 0 // continue only if we still need to do some memory cleanup
			})

			debug.FreeOSMemory() // try to free the memory
		case <-mw.m.listenAborted():
			return
		case <-mw.stopCh.Listen():
			return
		}
	}
}

func (mw *memoryWatcher) reserveMem(toReserve uint64) (exceeding bool) {
	newReservedMemory := atomic.AddUint64(&mw.reservedMemory, toReserve)
	// expected total memory after all objects will be extracted is equal
	// to: previously reserved memory + uncompressed size of shard + current memory used
	expectedTotalMemoryUsed := newReservedMemory + atomic.LoadUint64(&mw.memoryUsed)

	exceeding = expectedTotalMemoryUsed >= mw.maxMemoryToUse
	return
}

func (mw *memoryWatcher) unreserveMem(toUnreserve uint64) {
	mw.unreserveMemoryCh <- toUnreserve
}

func (mw *memoryWatcher) stopWatchingExcess() {
	mw.excessTicker.Stop()
}

func (mw *memoryWatcher) stopWatchingReserved() {
	mw.reservedTicker.Stop()
}

func (mw *memoryWatcher) stop() {
	mw.stopWatchingExcess()
	mw.stopWatchingReserved()
	mw.stopCh.Close()
	close(mw.unreserveMemoryCh)
}
