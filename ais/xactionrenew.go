// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/fs"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/mirror"
)

type xactEntry interface {
	Get() cmn.Xact
	Abort()
	RLock()
	RUnlock()
	Lock()
	Unlock()
	Stats() xactStats
}

type xactStats interface {
	Count() int64
}

// RENEW FUNCTIONS

func (r *xactionsRegistry) renewLRU() *xactLRU {
	newEntry := &lruEntry{}
	newEntry.Lock()
	defer newEntry.Unlock()

	val, loaded := r.globalXacts.LoadOrStore(cmn.ActLRU, newEntry)

	if !loaded {
		// new lru
		id := r.uniqueID()
		newEntry.xact = &xactLRU{XactBase: *cmn.NewXactBase(id, cmn.ActLRU)}
		r.byID.Store(id, newEntry)
		return newEntry.xact
	}

	if glog.V(4) {
		lruEntry := val.(*lruEntry)
		lruEntry.RLock()
		glog.Infof("%s already running, nothing to do", lruEntry.xact)
		lruEntry.RUnlock()
	}

	return nil
}

func (r *xactionsRegistry) renewPrefetch() *xactPrefetch {
	newEntry := &prefetchEntry{}
	newEntry.Lock()
	defer newEntry.Unlock()

	val, loaded := r.globalXacts.LoadOrStore(cmn.ActPrefetch, newEntry)

	if !loaded {
		// new lru
		id := r.uniqueID()
		newEntry.xact = &xactPrefetch{XactBase: *cmn.NewXactBase(id, cmn.ActPrefetch)}
		r.byID.Store(id, newEntry)
		return newEntry.xact
	}

	if glog.V(4) {
		prefetchEntry := val.(*prefetchEntry)
		prefetchEntry.RLock()
		glog.Infof("%s already running, nothing to do", prefetchEntry.xact)
		prefetchEntry.RUnlock()
	}

	return nil
}

func (r *xactionsRegistry) renewGlobalReb(smapVersion int64, runnerCnt int) *xactRebBase {
	entry := &globalRebEntry{}
	entry.Lock()
	defer entry.Unlock()

	val, loaded := r.globalXacts.LoadOrStore(cmn.ActGlobalReb, entry)

	if loaded {
		entry = val.(*globalRebEntry)
		entry.Lock()
		defer entry.Unlock()

		xGlobalReb := entry.xact

		if xGlobalReb.smapVersion > smapVersion {
			glog.Errorf("(reb: %s) Smap v%d is greater than v%d", xGlobalReb, xGlobalReb.smapVersion, smapVersion)
			return nil
		}
		if xGlobalReb.smapVersion == smapVersion {
			glog.V(4).Infof("%s already running, nothing to do", xGlobalReb)
			return nil
		}

		if !xGlobalReb.Finished() {
			xGlobalReb.Abort()
			for i := 0; i < xGlobalReb.runnerCnt; i++ {
				<-xGlobalReb.confirmCh
			}
			close(xGlobalReb.confirmCh)
		}
	}

	// here we have possibly both locks
	// entry variable is one which is actually present in r.globalXacts under globalReb
	id := r.uniqueID()
	xGlobalReb := &xactGlobalReb{
		xactRebBase: makeXactRebBase(id, globalRebType, runnerCnt),
		smapVersion: smapVersion,
	}

	entry.xact = xGlobalReb
	r.byID.Store(id, entry)
	return &xGlobalReb.xactRebBase
}

func (r *xactionsRegistry) renewLocalReb(runnerCnt int) *xactRebBase {
	entry := &localRebEntry{}
	entry.Lock()
	defer entry.Unlock()

	val, loaded := r.globalXacts.LoadOrStore(cmn.ActLocalReb, entry)

	if loaded {
		entry = val.(*localRebEntry)
		entry.Lock()
		defer entry.Unlock()

		if !entry.xact.Finished() {
			xLocalReb := entry.xact
			xLocalReb.Abort()

			for i := 0; i < xLocalReb.runnerCnt; i++ {
				<-xLocalReb.confirmCh
			}
			close(xLocalReb.confirmCh)
		}

	}

	// here we have possibly both locks
	// entry variable is one which is actually present in r.globalXacts under localReb

	id := r.uniqueID()
	xLocalReb := &xactLocalReb{
		xactRebBase: makeXactRebBase(id, localRebType, runnerCnt),
	}
	entry.xact = xLocalReb

	r.byID.Store(id, entry)
	return &xLocalReb.xactRebBase
}

func (r *xactionsRegistry) renewElection(p *proxyrunner, vr *VoteRecord) *xactElection {
	newEntry := &electionEntry{}
	newEntry.Lock()
	defer newEntry.Unlock()

	val, loaded := r.globalXacts.LoadOrStore(cmn.ActElection, newEntry)

	if loaded {
		if glog.V(4) {
			electionEntry := val.(*electionEntry)
			electionEntry.RLock()
			xElection := electionEntry.xact
			glog.Infof("%s already running, nothing to do", xElection)
			electionEntry.RUnlock()
		}

		return nil
	}

	id := r.uniqueID()
	xele := &xactElection{
		XactBase:    *cmn.NewXactBase(id, cmn.ActElection),
		proxyrunner: p,
		vr:          vr,
	}
	newEntry.xact = xele
	r.byID.Store(id, newEntry)
	return xele
}

func (r *xactionsRegistry) renewEvictDelete(evict bool) *xactEvictDelete {
	newEntry := &evictDeleteEntry{}

	xact := cmn.ActDelete
	if evict {
		xact = cmn.ActEvictObjects
	}
	id := r.uniqueID()
	xdel := &xactEvictDelete{XactBase: *cmn.NewXactBase(id, xact)}
	newEntry.xact = xdel

	r.globalXacts.Store(xact, newEntry)
	r.byID.Store(id, newEntry)
	return xdel
}

func (r *xactionsRegistry) renewDownloader(t *targetrunner) (*downloader.Downloader, error) {
	newEntry := &downloaderEntry{}
	newEntry.Lock()
	defer newEntry.Unlock()

	val, loaded := r.globalXacts.LoadOrStore(cmn.Download, newEntry)

	if loaded {
		entry := val.(*downloaderEntry)
		entry.RLock()
		defer entry.RUnlock()

		xdl := entry.xact
		xdl.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
		return xdl, nil
	}

	id := r.uniqueID()
	xdl, err := downloader.NewDownloader(t, t.statsif, fs.Mountpaths, id, cmn.Download)
	if err != nil {
		return nil, err
	}

	newEntry.xact = xdl
	go xdl.Run()
	r.byID.Store(id, newEntry)
	return xdl, nil
}

func (r *xactionsRegistry) bucketsXacts(bucket string) *sync.Map {
	val, _ := r.buckets.LoadOrStore(bucket, &sync.Map{})
	return val.(*sync.Map)
}

func (r *xactionsRegistry) renewGetEC(bucket string) *ec.XactGet {
	bckXacts := r.bucketsXacts(bucket)

	entry := &getECEntry{}
	entry.Lock()
	defer entry.Unlock()
	val, loaded := bckXacts.LoadOrStore(cmn.ActECGet, entry)

	if loaded {
		entry = val.(*getECEntry)
		entry.RLock()
		if !entry.xact.Finished() {
			xec := entry.xact
			entry.RUnlock()
			xec.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
			glog.V(4).Infof("%s already running, nothing to do", xec)
			return xec
		}
		entry.RUnlock()
		entry.Lock()
		defer entry.Unlock()
	}

	id := r.uniqueID()
	xec := ECM.newGetXact(bucket)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECGet, bucket)
	go xec.Run()
	entry.xact = xec
	r.byID.Store(id, entry)
	return xec
}

func (r *xactionsRegistry) renewPutEC(bucket string) *ec.XactPut {
	bckXacts := r.bucketsXacts(bucket)

	entry := &putECEntry{}
	entry.Lock()
	defer entry.Unlock()
	val, loaded := bckXacts.LoadOrStore(cmn.ActECPut, entry)

	if loaded {
		entry = val.(*putECEntry)
		entry.RLock()
		if !entry.xact.Finished() {
			xec := entry.xact
			entry.RUnlock()
			xec.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
			glog.V(4).Infof("%s already running, nothing to do", xec)
			return xec
		}
		entry.RUnlock()
		entry.Lock()
		defer entry.Unlock()
	}

	id := r.uniqueID()
	xec := ECM.newPutXact(bucket)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECPut, bucket)
	go xec.Run()
	entry.xact = xec
	r.byID.Store(id, entry)
	return xec
}

func (r *xactionsRegistry) renewRespondEC(bucket string) *ec.XactRespond {
	bckXacts := r.bucketsXacts(bucket)

	entry := &respondECEntry{}
	entry.Lock()
	defer entry.Unlock()
	val, loaded := bckXacts.LoadOrStore(cmn.ActECRespond, entry)

	if loaded {
		entry = val.(*respondECEntry)
		entry.RLock()
		if !entry.xact.Finished() {
			xec := entry.xact
			entry.RUnlock()
			xec.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
			glog.V(4).Infof("%s already running, nothing to do", xec)
			return xec
		}
		entry.RUnlock()
		entry.Lock()
		defer entry.Unlock()
	}

	id := r.uniqueID()
	xec := ECM.newRespondXact(bucket)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECRespond, bucket)
	go xec.Run()
	entry.xact = xec
	r.byID.Store(id, entry)
	return xec
}

func (r *xactionsRegistry) renewBckMakeNCopies(bucket string, t *targetrunner, copies int, bckIsLocal bool) {
	bckXacts := r.bucketsXacts(bucket)

	newEntry := &makeNCopiesEntry{}
	newEntry.Lock()
	defer newEntry.Unlock()
	val, loaded := bckXacts.LoadOrStore(cmn.ActMakeNCopies, newEntry)

	var entry *makeNCopiesEntry

	if loaded {
		entry = val.(*makeNCopiesEntry)
		entry.Lock()
		defer entry.Unlock()

		if !entry.xact.Finished() {
			glog.V(4).Infof("nothing to do: %s", entry.xact)
			return
		}
	} else {
		entry = newEntry
	}

	// we already have possibly both locks
	// entry variable is one which is actually present in bucketXacts under makeNCopies
	id := r.uniqueID()
	base := cmn.NewXactBase(id, cmn.ActMakeNCopies, bucket)
	slab := gmem2.SelectSlab2(cmn.MiB) // FIXME: estimate
	xmnc := &mirror.XactBckMakeNCopies{
		XactBase:   *base,
		T:          t,
		Namelocker: t.rtnamemap,
		Slab:       slab,
		Copies:     copies,
		BckIsLocal: bckIsLocal,
	}

	go xmnc.Run()
	entry.xact = xmnc
	r.byID.Store(id, entry)
}

func (r *xactionsRegistry) renewPutCopies(lom *cluster.LOM, t *targetrunner) *mirror.XactCopy {
	bckXacts := r.bucketsXacts(lom.Bucket)

	newEntry := &putCopiesEntry{}
	newEntry.Lock()
	defer newEntry.Unlock()

	val, loaded := bckXacts.LoadOrStore(cmn.ActPutCopies, newEntry)

	if loaded {
		entry := val.(*putCopiesEntry)
		entry.RLock()
		defer entry.RUnlock()

		if !entry.xact.Finished() {
			return entry.xact
		}
	}

	val, ok := bckXacts.Load(cmn.ActMakeNCopies)

	if ok {
		copiesEntry := val.(*makeNCopiesEntry)
		copiesEntry.RLock()
		defer copiesEntry.RUnlock()

		if !copiesEntry.xact.Finished() {
			glog.Errorf("cannot start '%s' xaction when %s is running", cmn.ActPutCopies, copiesEntry.xact)
			return nil
		}
	}

	// construct new
	id := r.uniqueID()
	base := cmn.NewXactDemandBase(id, cmn.ActPutCopies, lom.Bucket)
	slab := gmem2.SelectSlab2(cmn.MiB) // FIXME: estimate
	xcopy := &mirror.XactCopy{
		XactDemandBase: *base,
		Slab:           slab,
		Mirror:         *lom.MirrorConf(),
		T:              t,
		Namelocker:     t.rtnamemap,
		BckIsLocal:     lom.BckIsLocal,
	}

	if err := xcopy.InitAndRun(); err != nil {
		glog.Errorln(err)
		xcopy = nil
	} else {
		newEntry.xact = xcopy
		r.byID.Store(id, newEntry)
	}
	return xcopy
}

func (r *xactionsRegistry) renewRechecksum(bucket string) *xactRechecksum {
	bckXacts := r.bucketsXacts(bucket)

	newEntry := &checksumEntry{}
	newEntry.Lock()
	defer newEntry.Unlock()

	val, loaded := bckXacts.LoadOrStore(cmn.ActRechecksum, newEntry)

	if loaded {
		if glog.V(4) {
			entry := val.(*checksumEntry)
			entry.RLock()
			glog.Infof("%s already running for bucket %s, nothing to do", entry.xact, bucket)
			entry.RUnlock()
		}

		return nil
	}

	id := r.uniqueID()
	xrcksum := &xactRechecksum{XactBase: *cmn.NewXactBase(id, cmn.ActRechecksum), bucket: bucket}
	newEntry.xact = xrcksum
	r.byID.Store(id, newEntry)
	return xrcksum
}

// HELPERS

func makeXactRebBase(id int64, rebType int, runnerCnt int) xactRebBase {
	kind := ""
	switch rebType {
	case localRebType:
		kind = cmn.ActLocalReb
	case globalRebType:
		kind = cmn.ActGlobalReb
	default:
		cmn.AssertMsg(false, fmt.Sprintf("unknown rebalance type: %d", rebType))
	}

	return xactRebBase{
		XactBase:  *cmn.NewXactBase(id, kind),
		runnerCnt: runnerCnt,
		confirmCh: make(chan struct{}, runnerCnt),
	}
}

// ENTRIES

type (
	baseXactEntry struct {
		sync.RWMutex
		baseXactStats
	}

	baseXactStats struct {
		count int64
	}

	lruEntry struct {
		baseXactEntry
		xact *xactLRU
	}

	prefetchEntry struct {
		baseXactEntry
		xact *xactPrefetch
	}

	globalRebEntry struct {
		baseXactEntry
		xact *xactGlobalReb
	}

	localRebEntry struct {
		baseXactEntry
		xact *xactLocalReb
	}

	electionEntry struct {
		baseXactEntry
		xact *xactElection
	}

	evictDeleteEntry struct {
		baseXactEntry
		xact *xactEvictDelete
	}

	downloaderEntry struct {
		baseXactEntry
		xact *downloader.Downloader
	}

	getECEntry struct {
		baseXactEntry
		xact *ec.XactGet
	}

	putECEntry struct {
		baseXactEntry
		xact *ec.XactPut
	}

	respondECEntry struct {
		baseXactEntry
		xact *ec.XactRespond
	}

	putCopiesEntry struct {
		baseXactEntry
		xact *mirror.XactCopy
	}

	checksumEntry struct {
		baseXactEntry
		xact *xactRechecksum
	}
)

func (b baseXactStats) Count() int64 {
	return b.count
}

func (e *lruEntry) Get() cmn.Xact { return e.xact }

func (e *lruEntry) Stats() xactStats { return e.baseXactStats }
func (e *lruEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *prefetchEntry) Get() cmn.Xact    { return e.xact }
func (e *prefetchEntry) Stats() xactStats { return e.baseXactStats }

func (e *prefetchEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *globalRebEntry) Get() cmn.Xact    { return e.xact }
func (e *globalRebEntry) Stats() xactStats { return e.baseXactStats }

func (e *globalRebEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *localRebEntry) Get() cmn.Xact    { return e.xact }
func (e *localRebEntry) Stats() xactStats { return e.baseXactStats }

func (e *localRebEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *electionEntry) Get() cmn.Xact    { return e.xact }
func (e *electionEntry) Stats() xactStats { return e.baseXactStats }

func (e *electionEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *evictDeleteEntry) Get() cmn.Xact    { return e.xact }
func (e *evictDeleteEntry) Stats() xactStats { return e.baseXactStats }

func (e *evictDeleteEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *downloaderEntry) Get() cmn.Xact { return e.xact }

func (e *downloaderEntry) Stats() xactStats { return e.baseXactStats }
func (e *downloaderEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *getECEntry) Get() cmn.Xact { return e.xact }
func (e *getECEntry) Stats() xactStats {
	e.RLock()
	defer e.RUnlock()
	e.baseXactStats.count = e.xact.Stats().GetReq
	return e.baseXactStats
}
func (e *getECEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *putECEntry) Get() cmn.Xact { return e.xact }
func (e *putECEntry) Stats() xactStats {
	e.RLock()
	defer e.RUnlock()
	e.baseXactStats.count = e.xact.Stats().PutReq
	return e.baseXactStats
}

func (e *putECEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *respondECEntry) Get() cmn.Xact    { return e.xact }
func (e *respondECEntry) Stats() xactStats { return e.baseXactStats }
func (e *respondECEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

type makeNCopiesEntry struct {
	baseXactEntry
	xact *mirror.XactBckMakeNCopies
}

func (e *makeNCopiesEntry) Get() cmn.Xact    { return e.xact }
func (e *makeNCopiesEntry) Stats() xactStats { return e.baseXactStats }
func (e *makeNCopiesEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *putCopiesEntry) Get() cmn.Xact    { return e.xact }
func (e *putCopiesEntry) Stats() xactStats { return e.baseXactStats }
func (e *putCopiesEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *checksumEntry) Get() cmn.Xact    { return e.xact }
func (e *checksumEntry) Stats() xactStats { return e.baseXactStats }
func (e *checksumEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}
