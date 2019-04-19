// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/stats"
)

// RENEW FUNCTIONS

func (r *xactionsRegistry) renewLRU() *xactLRU {
	entry := &lruEntry{}
	entry.Lock()
	defer entry.Unlock()

	val, loaded := r.globalXacts.LoadOrStore(cmn.ActLRU, entry)

	if loaded {
		entry = val.(*lruEntry)
		entry.Lock()
		defer entry.Unlock()

		if !entry.xact.Finished() {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s already running, nothing to do", entry.xact)
			}
			return nil
		}
	}

	// new lru
	id := r.uniqueID()
	entry.xact = &xactLRU{XactBase: *cmn.NewXactBase(id, cmn.ActLRU)}
	r.byID.Store(id, entry)
	return entry.xact
}

func (r *xactionsRegistry) renewPrefetch() *xactPrefetch {
	entry := &prefetchEntry{}
	entry.Lock()
	defer entry.Unlock()

	val, loaded := r.globalXacts.LoadOrStore(cmn.ActPrefetch, entry)

	if loaded {
		entry = val.(*prefetchEntry)
		entry.Lock()
		defer entry.Unlock()

		if !entry.xact.Finished() {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s already running, nothing to do", entry.xact)
			}
			return nil
		}
	}

	id := r.uniqueID()
	entry.xact = &xactPrefetch{XactBase: *cmn.NewXactBase(id, cmn.ActPrefetch)}
	r.byID.Store(id, entry)
	return entry.xact
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
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s already running, nothing to do", xGlobalReb)
			}
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
	entry := &electionEntry{}
	entry.Lock()
	defer entry.Unlock()

	val, loaded := r.globalXacts.LoadOrStore(cmn.ActElection, entry)

	if loaded {
		entry := val.(*electionEntry)
		entry.Lock()
		defer entry.Unlock()

		if !entry.xact.Finished() {
			// election still in progress
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s already running, nothing to do", entry.xact)
			}
			return nil
		}
	}

	id := r.uniqueID()
	xele := &xactElection{
		XactBase:    *cmn.NewXactBase(id, cmn.ActElection),
		proxyrunner: p,
		vr:          vr,
	}
	entry.xact = xele
	r.byID.Store(id, entry)
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
	entry := &downloaderEntry{}
	entry.Lock()
	defer entry.Unlock()

	val, loaded := r.globalXacts.LoadOrStore(cmn.Download, entry)

	if loaded {
		entry = val.(*downloaderEntry)
		entry.Lock()
		defer entry.Unlock()

		if !entry.xact.Finished() {
			xdl := entry.xact
			xdl.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
			return xdl, nil
		}
	}

	id := r.uniqueID()
	xdl, err := downloader.NewDownloader(t, t.statsif, fs.Mountpaths, id, cmn.Download)
	if err != nil {
		return nil, err
	}

	entry.xact = xdl
	go xdl.Run()
	r.byID.Store(id, entry)
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
		entry.Lock()
		defer entry.Unlock()
		if !entry.xact.Finished() {
			xec := entry.xact
			xec.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s already running, nothing to do", xec)
			}
			return xec
		}
	}

	id := r.uniqueID()
	xec := ECM.newGetXact(bucket)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECGet, bucket, true /* local*/) // TODO: EC support for Cloud
	go xec.Run()
	entry.xact = xec
	entry.bucket = bucket
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
		entry.Lock()
		defer entry.Unlock()
		if !entry.xact.Finished() {
			xec := entry.xact
			xec.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s already running, nothing to do", xec)
			}
			return xec
		}
	}

	id := r.uniqueID()
	xec := ECM.newPutXact(bucket)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECPut, bucket, true /* local */) // TODO: EC support for Cloud
	go xec.Run()
	entry.xact = xec
	entry.bucket = bucket
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
		entry.Lock()
		defer entry.Unlock()
		if !entry.xact.Finished() {
			xec := entry.xact
			xec.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s already running, nothing to do", xec)
			}
			return xec
		}
	}

	id := r.uniqueID()
	xec := ECM.newRespondXact(bucket)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECRespond, bucket, true /* local */) // TODO: EC support for Cloud
	go xec.Run()
	entry.xact = xec
	entry.bucket = bucket
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
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("nothing to do: %s", entry.xact)
			}
			return
		}
	} else {
		entry = newEntry
	}

	// we already have possibly both locks
	// entry variable is one which is actually present in bucketXacts under makeNCopies
	id := r.uniqueID()
	slab := gmem2.SelectSlab2(cmn.MiB) // FIXME: estimate
	xmnc := mirror.NewXactMNC(id, bucket, t, t.rtnamemap, slab, copies, bckIsLocal)
	go xmnc.Run()
	entry.xact = xmnc
	entry.bucket = bucket
	r.byID.Store(id, entry)
}

//
// FIXME: copy-paste
//

func (r *xactionsRegistry) renewBckLoadLomCache(bucket string, t cluster.Target, bckIsLocal bool) {
	if true {
		return
	}
	bckXacts := r.bucketsXacts(bucket)

	newEntry := &loadLomCacheEntry{}
	newEntry.Lock()
	defer newEntry.Unlock()
	val, loaded := bckXacts.LoadOrStore(cmn.ActLoadLomCache, newEntry)

	var entry *loadLomCacheEntry

	if loaded {
		entry = val.(*loadLomCacheEntry)
		entry.Lock()
		defer entry.Unlock()

		if !entry.xact.Finished() {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("nothing to do: %s", entry.xact)
			}
			return
		}
	} else {
		entry = newEntry
	}
	id := r.uniqueID()
	x := mirror.NewXactLLC(id, bucket, t, bckIsLocal)
	go x.Run()
	entry.xact = x
	entry.bucket = bucket
	r.byID.Store(id, entry)
}

func (r *xactionsRegistry) renewPutLocReplicas(lom *cluster.LOM, nl cluster.NameLocker) *mirror.XactPutLRepl {
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
	id := r.uniqueID()
	slab := gmem2.SelectSlab2(cmn.MiB) // TODO: estimate
	// construct and RUN
	x, err := mirror.RunXactPutLRepl(id, lom, nl, slab)
	if err != nil {
		glog.Errorln(err)
		x = nil
	} else {
		newEntry.xact = x
		newEntry.bucket = lom.Bucket
		r.byID.Store(id, newEntry)
	}
	return x
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
		stats stats.BaseXactStats
	}
	lruEntry struct {
		baseXactEntry
		xact *xactLRU
	}
	prefetchEntry struct {
		sync.RWMutex
		stats stats.PrefetchTargetStats
		xact  *xactPrefetch
	}
	globalRebEntry struct {
		sync.RWMutex
		stats stats.RebalanceTargetStats
		xact  *xactGlobalReb
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
		xact   *ec.XactGet
		bucket string
	}
	putECEntry struct {
		baseXactEntry
		xact   *ec.XactPut
		bucket string
	}
	respondECEntry struct {
		baseXactEntry
		xact   *ec.XactRespond
		bucket string
	}
	putCopiesEntry struct {
		baseXactEntry
		xact   *mirror.XactPutLRepl
		bucket string
	}
	makeNCopiesEntry struct {
		baseXactEntry
		xact   *mirror.XactBckMakeNCopies
		bucket string
	}
	loadLomCacheEntry struct {
		baseXactEntry
		xact   *mirror.XactBckLoadLomCache
		bucket string
	}
)

type xactEntry interface {
	Get() cmn.Xact
	Abort()
	RLock()
	RUnlock()
	Lock()
	Unlock()
	Stats() stats.XactStats
}

func (e *lruEntry) Get() cmn.Xact { return e.xact }

//
// FIXME: copy-paste L562 - eof
//

func (e *lruEntry) Stats() stats.XactStats {
	e.RLock()
	s := e.stats.FromXact(e.xact, "")
	e.RUnlock()
	return s
}
func (e *lruEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *prefetchEntry) Get() cmn.Xact { return e.xact }
func (e *prefetchEntry) Stats() stats.XactStats {
	e.RLock()
	e.stats.FromXact(e.xact, "")
	e.stats.FillFromTrunner(getstorstatsrunner())
	s := &e.stats
	e.RUnlock()
	return s
}

func (e *prefetchEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *globalRebEntry) Get() cmn.Xact { return e.xact }
func (e *globalRebEntry) Stats() stats.XactStats {
	e.RLock()
	e.stats.FromXact(e.xact, "")
	e.stats.FillFromTrunner(getstorstatsrunner())
	s := &e.stats
	e.RUnlock()
	return s
}

func (e *globalRebEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *localRebEntry) Get() cmn.Xact { return e.xact }
func (e *localRebEntry) Stats() stats.XactStats {
	e.RLock()
	s := e.stats.FromXact(e.xact, "")
	e.RUnlock()
	return s
}

func (e *localRebEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *electionEntry) Get() cmn.Xact { return e.xact }
func (e *electionEntry) Stats() stats.XactStats {
	e.RLock()
	s := e.stats.FromXact(e.xact, "")
	e.RUnlock()
	return s
}

func (e *electionEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *evictDeleteEntry) Get() cmn.Xact { return e.xact }
func (e *evictDeleteEntry) Stats() stats.XactStats {
	e.RLock()
	s := e.stats.FromXact(e.xact, "")
	e.RUnlock()
	return s
}

func (e *evictDeleteEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *downloaderEntry) Get() cmn.Xact { return e.xact }
func (e *downloaderEntry) Stats() stats.XactStats {
	e.RLock()
	s := e.stats.FromXact(e.xact, "")
	e.RUnlock()
	return s
}
func (e *downloaderEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *getECEntry) Get() cmn.Xact { return e.xact }
func (e *getECEntry) Stats() stats.XactStats {
	e.RLock()
	e.stats.XactCountX = e.xact.Stats().GetReq
	s := e.stats.FromXact(e.xact, e.bucket)
	e.RUnlock()
	return s
}
func (e *getECEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *putECEntry) Get() cmn.Xact { return e.xact }
func (e *putECEntry) Stats() stats.XactStats {
	e.RLock()
	e.stats.XactCountX = e.xact.Stats().PutReq
	s := e.stats.FromXact(e.xact, e.bucket)
	e.RUnlock()
	return s
}

func (e *putECEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *respondECEntry) Get() cmn.Xact { return e.xact }
func (e *respondECEntry) Stats() stats.XactStats {
	e.RLock()
	s := e.stats.FromXact(e.xact, e.bucket)
	e.RUnlock()
	return s
}
func (e *respondECEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *makeNCopiesEntry) Get() cmn.Xact { return e.xact }
func (e *makeNCopiesEntry) Stats() stats.XactStats {
	e.RLock()
	s := e.stats.FromXact(e.xact, e.bucket)
	e.RUnlock()
	return s
}
func (e *makeNCopiesEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *putCopiesEntry) Get() cmn.Xact { return e.xact }
func (e *putCopiesEntry) Stats() stats.XactStats {
	e.RLock()
	s := e.stats.FromXact(e.xact, e.bucket)
	e.RUnlock()
	return s
}
func (e *putCopiesEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}

func (e *loadLomCacheEntry) Get() cmn.Xact { return e.xact }
func (e *loadLomCacheEntry) Stats() stats.XactStats {
	e.RLock()
	s := e.stats.FromXact(e.xact, e.bucket)
	e.RUnlock()
	return s
}
func (e *loadLomCacheEntry) Abort() {
	if e.xact != nil && !e.xact.Finished() {
		e.xact.Abort()
	}
}
