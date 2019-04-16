// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
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
	nextid      atomic.Int64
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
	sleep := atomic.NewInt32(0)

	if r.abortAllEntries(&r.globalXacts) {
		sleep.Store(1)
	}

	wg := &sync.WaitGroup{}
	r.buckets.Range(func(_, val interface{}) bool {
		wg.Add(1)
		go func() {
			bucketsXact := val.(*sync.Map)
			if r.abortAllEntries(bucketsXact) {
				sleep.Inc()
			}
			wg.Done()
		}()
		return true
	})

	wg.Wait()
	return sleep.Load() > 0
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
	return r.nextid.Inc()
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
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Can't abort nonexistent xaction for bucket %s", bucket)
		}
		return
	}
	bucketsXacts := val.(*sync.Map)
	val, ok = bucketsXacts.Load(kind)
	if !ok {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Can't abort nonexistent xaction for bucket %s", bucket)
		}
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
func (r *xactionsRegistry) globalXactStats(kind string) ([]stats.XactStats, error) {
	val, ok := r.globalXacts.Load(kind)
	if !ok {
		return nil, fmt.Errorf("xact %s does not exist", kind)
	}

	entry := val.(xactEntry)
	entry.RLock()
	defer entry.RUnlock()
	return []stats.XactStats{entry.Stats()}, nil
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

// Returns stats of xaction with given 'kind' on a given bucket
func (r *xactionsRegistry) bucketSingleXactStats(kind, bucket string) ([]stats.XactStats, error) {
	bucketXats, ok := r.getBucketsXacts(bucket)
	if !ok {
		return nil, fmt.Errorf("no bucket %s for xact %s", bucket, kind)
	}

	val, ok := bucketXats.Load(kind)
	if !ok {
		return nil, fmt.Errorf("xact %s for bucket %s does not exist", kind, bucket)
	}

	entry := val.(xactEntry)
	entry.RLock()
	defer entry.RUnlock()
	return []stats.XactStats{entry.Stats()}, nil
}

// Returns stats of all present xactions
func (r *xactionsRegistry) allXactsStats() []stats.XactStats {
	return statsFromXactionsMap(&r.byID)
}

func (r *xactionsRegistry) getNonBucketSpecificStats(kind string) ([]stats.XactStats, error) {
	// no bucket and no kind - request for all xactions
	if kind == "" {
		return r.allXactsStats(), nil
	}

	global, err := cmn.XactKind.IsGlobalKind(kind)

	if err != nil {
		return nil, err
	}

	if global {
		return r.globalXactStats(kind)
	}

	return nil, fmt.Errorf("xaction %s is not a global xaction", kind)
}

// Returns stats of all xactions of a given bucket
func (r *xactionsRegistry) bucketAllXactsStats(bucket string) ([]stats.XactStats, error) {
	bucketsXacts, ok := r.getBucketsXacts(bucket)

	if !ok {
		return nil, fmt.Errorf("xactions for %s bucket not found", bucket)
	}

	return statsFromXactionsMap(bucketsXacts), nil
}

func (r *xactionsRegistry) getStats(kind, bucket string) ([]stats.XactStats, error) {
	if bucket == "" {
		// no bucket - either all xactions or a global xaction
		return r.getNonBucketSpecificStats(kind)
	}

	// both bucket and kind present - request for specific bucket's xaction
	if kind != "" {
		return r.bucketSingleXactStats(kind, bucket)
	}

	// bucket present and no kind - request for all available bucket's xactions
	return r.bucketAllXactsStats(bucket)
}

func (r *xactionsRegistry) doAbort(kind, bucket string) {
	// no bucket and no kind - request for all available xactions
	if bucket == "" && kind == "" {
		r.abortAll()
	}
	// bucket present and no kind - request for all available bucket's xactions
	if bucket != "" && kind == "" {
		r.abortBuckets(bucket)
	}
	// both bucket and kind present - request for specific bucket's xaction
	if bucket != "" && kind != "" {
		r.abortBucketXact(kind, bucket)
	}
	// no bucket, but kind present - request for specific global xaction
	if bucket == "" && kind != "" {
		r.abortGlobalXact(kind)
	}
}

func (r *xactionsRegistry) getBucketsXacts(bucket string) (m *sync.Map, ok bool) {
	val, ok := r.buckets.Load(bucket)
	if !ok {
		return nil, false
	}
	return val.(*sync.Map), true
}

func statsFromXactionsMap(m *sync.Map) []stats.XactStats {
	const expectedXactionsSize = 10
	statsList := make([]stats.XactStats, 0, expectedXactionsSize)

	m.Range(func(_, val interface{}) bool {
		entry := val.(xactEntry)
		entry.RLock()
		statsList = append(statsList, entry.Stats())
		entry.RUnlock()
		return true
	})

	return statsList
}
