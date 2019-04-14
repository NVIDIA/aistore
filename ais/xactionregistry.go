// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/NVIDIA/aistore/3rdparty/glog"

	"github.com/NVIDIA/aistore/cmn"
)

type (
	xactRebBase struct {
		cmn.XactBase
		runnerCnt int
		confirmCh chan struct{}
	}
	xactGlobalReb struct {
		xactRebBase
		smapVersion int64 // smap version on which this rebalance has started
	}
	xactLocalReb struct {
		xactRebBase
	}
	xactLRU struct {
		cmn.XactBase
	}
	xactPrefetch struct {
		cmn.XactBase
	}
	xactEvictDelete struct {
		cmn.XactBase
	}
	xactElection struct {
		cmn.XactBase
		proxyrunner *proxyrunner
		vr          *VoteRecord
	}
)

type xactionsRegistry struct {
	sync.Mutex
	nextid      int64
	globalXacts sync.Map
	buckets     sync.Map

	byID sync.Map
}

func newXactions() *xactionsRegistry {
	return &xactionsRegistry{}
}

var mountpathXactions = []string{cmn.ActLRU, cmn.ActPutCopies, cmn.ActMakeNCopies, cmn.ActECGet, cmn.ActECPut, cmn.ActECRespond, cmn.ActLocalReb}

func (r *xactionsRegistry) abortBuckets(buckets ...string) {
	wg := &sync.WaitGroup{}
	for _, b := range buckets {
		wg.Add(1)
		go func(b string) {
			defer wg.Done()
			val, ok := r.buckets.Load(b)

			if !ok {
				glog.Warningf("Can't abort nonexistent xactions for bucket %s", b)
				return
			}

			bucketsXacts := val.(*sync.Map)
			r.abortAllEntries(bucketsXacts)
			// TODO: cleanup bucket from r.buckets
		}(b)
	}

	wg.Wait()
}

func (r *xactionsRegistry) abortAllEntries(entriesMap *sync.Map) bool {
	sleep := false
	wg := &sync.WaitGroup{}

	entriesMap.Range(func(_, val interface{}) bool {
		entry := val.(xactEntry)
		entry.RLock()

		if entry.Get().Finished() {
			entry.RUnlock()
		} else {
			// sync.Map.Range is sequential, safe to wg.Add inside the loop
			wg.Add(1)
			go func() {
				entry.Abort()
				entry.RUnlock()
				wg.Done()
			}()
			sleep = true
		}

		return true
	})

	wg.Wait()
	return sleep
}

func (r *xactionsRegistry) abortAll() bool {
	sleep := int32(0)

	if r.abortAllEntries(&r.globalXacts) {
		sleep = 1
	}

	wg := &sync.WaitGroup{}
	r.buckets.Range(func(_, val interface{}) bool {
		wg.Add(1)
		go func() {
			bucketsXact := val.(*sync.Map)
			if r.abortAllEntries(bucketsXact) {
				atomic.AddInt32(&sleep, 1)
			}
			wg.Done()
		}()
		return true
	})

	wg.Wait()
	return sleep > 0
}

func (r *xactionsRegistry) localRebStatus() (aborted, running bool) {
	pmarker := persistentMarker(localRebType)
	_, err := os.Stat(pmarker)
	if err == nil {
		aborted = true
	}

	val, ok := r.globalXacts.Load(cmn.ActLocalReb)
	if !ok {
		return
	}

	entry := val.(*localRebEntry)
	entry.RLock()
	running = entry.xact != nil && !entry.xact.Finished()
	entry.RUnlock()

	return
}

func (r *xactionsRegistry) globalRebStatus() (aborted, running bool) {
	pmarker := persistentMarker(globalRebType)
	_, err := os.Stat(pmarker)
	if err == nil {
		aborted = true
	}

	val, ok := r.globalXacts.Load(cmn.ActGlobalReb)
	if !ok {
		return
	}

	entry := val.(*globalRebEntry)
	entry.RLock()
	running = entry.xact != nil && !entry.xact.Finished()
	entry.RUnlock()
	return
}

func (r *xactionsRegistry) uniqueID() int64 {
	return atomic.AddInt64(&r.nextid, 1)
}

func (r *xactionsRegistry) kindRange(kind string, doAction func(cmn.Xact)) {
	r.xactsRange(func(m string) bool { return strings.Contains(m, kind) }, doAction)
}

func (r *xactionsRegistry) xactsRange(matches func(string) bool, doAction func(cmn.Xact)) {
	r.byID.Range(func(_, value interface{}) bool {
		entry := value.(xactEntry)
		entry.RLock()

		if matches(entry.Get().Kind()) {
			doAction(entry.Get())
		}

		entry.RUnlock()
		return true
	})
}

func (r *xactionsRegistry) abortBucketXact(kind, bucket string) {
	val, ok := r.buckets.Load(bucket)

	if !ok {
		glog.V(4).Infof("Can't abort nonexistent xaction for bucket %s", bucket)
		return
	}

	bucketsXacts := val.(*sync.Map)
	val, ok = bucketsXacts.Load(kind)
	if !ok {
		glog.V(4).Infof("Can't abort nonexistent xaction for bucket %s", bucket)
		return
	}

	entry := val.(xactEntry)
	entry.RLock()
	entry.Abort()
	entry.RUnlock()
}

func (r *xactionsRegistry) globalXactRunning(kind string) bool {
	val, ok := r.globalXacts.Load(kind)
	if !ok {
		return false
	}

	entry := val.(xactEntry)
	entry.RLock()
	defer entry.RUnlock()
	return !entry.Get().Finished()
}

//nolint:unused
func (r *xactionsRegistry) globalXactStats(kind string) (xactStats, error) {
	val, ok := r.globalXacts.Load(kind)
	if !ok {
		return nil, fmt.Errorf("xact %s does not exist", kind)
	}

	entry := val.(xactEntry)
	entry.RLock()
	defer entry.RUnlock()
	return entry.Stats(), nil
}

func (r *xactionsRegistry) abortGlobalXact(kind string) {
	val, ok := r.globalXacts.Load(kind)
	if !ok {
		return
	}

	entry := val.(xactEntry)
	entry.RLock()
	entry.Abort()
	entry.RUnlock()
}

//nolint:unused
func (r *xactionsRegistry) bucketXactRunning(kind, bucket string) bool {
	val, ok := r.buckets.Load(bucket)
	if !ok {
		return false
	}

	xacts := val.(*sync.Map)
	val, ok = xacts.Load(kind)
	if !ok {
		return false
	}

	entry := val.(xactEntry)
	entry.RLock()
	defer entry.RUnlock()
	return !entry.Get().Finished()
}

//nolint:unused
func (r *xactionsRegistry) bucketXactStats(kind, bucket string) (xactStats, error) {
	val, ok := r.buckets.Load(bucket)
	if !ok {
		return nil, fmt.Errorf("xact %s for bucket %s does not exist", kind, bucket)
	}

	xacts := val.(*sync.Map)
	val, ok = xacts.Load(bucket)
	if !ok {
		return nil, fmt.Errorf("xact %s for bucket %s does not exist", kind, bucket)
	}

	entry := val.(xactEntry)
	entry.RLock()
	defer entry.RUnlock()
	return entry.Stats(), nil
}
