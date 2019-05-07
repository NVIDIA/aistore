// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/lru"
	"github.com/NVIDIA/aistore/stats"
)

type xactionGlobalEntry interface {
	xactionEntry
	// returns if renew should be canceled when previous entry exists
	EndRenewOnPrevious(entry xactionGlobalEntry) (end bool)
	// performs some actions before returning from renew, when previous entry exists, for example Renew()
	ActOnPrevious(entr xactionGlobalEntry)
	// CleanupPrevious is supposed to perform necessary cleanup on previous entry,
	// when this one in successfully stored. Done under Lock
	CleanupPrevious(entry xactionGlobalEntry)
}

// Global entries

type lruEntry struct {
	baseGlobalEntry
	xact *lru.Xaction
}

func (e *lruEntry) Start(id int64) error {
	e.xact = &lru.Xaction{XactBase: *cmn.NewXactBase(id, cmn.ActLRU)}
	return nil
}

func (e *lruEntry) Kind() string  { return cmn.ActLRU }
func (e *lruEntry) Get() cmn.Xact { return e.xact }

type prefetchEntry struct {
	baseGlobalEntry
	r    *stats.Trunner
	xact *xactPrefetch
}

func (e *prefetchEntry) Start(id int64) error {
	e.xact = &xactPrefetch{XactBase: *cmn.NewXactBase(id, cmn.ActPrefetch), r: e.r}
	return nil
}
func (e *prefetchEntry) Kind() string  { return cmn.ActPrefetch }
func (e *prefetchEntry) Get() cmn.Xact { return e.xact }

type globalRebEntry struct {
	baseGlobalEntry
	stats       stats.RebalanceTargetStats
	smapVersion int64
	runnerCnt   int
	xact        *xactGlobalReb
}

func (e *globalRebEntry) Start(id int64) error {
	xGlobalReb := &xactGlobalReb{
		xactRebBase: makeXactRebBase(id, globalRebType, e.runnerCnt),
		smapVersion: e.smapVersion,
	}

	e.xact = xGlobalReb
	return nil
}
func (e *globalRebEntry) Kind() string  { return cmn.ActGlobalReb }
func (e *globalRebEntry) Get() cmn.Xact { return e.xact }
func (e *globalRebEntry) EndRenewOnPrevious(entry xactionGlobalEntry) (end bool) {
	if entry == nil {
		return
	}

	xGlobalReb := entry.(*globalRebEntry).xact
	if xGlobalReb.smapVersion > e.smapVersion {
		glog.Errorf("(reb: %s) Smap v%d is greater than v%d", xGlobalReb, xGlobalReb.smapVersion, e.smapVersion)
		return true
	} else if xGlobalReb.smapVersion == e.smapVersion {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s already running, nothing to do", xGlobalReb)
		}
		return true
	}
	return
}
func (e *globalRebEntry) ActOnPrevious(_ xactionGlobalEntry) {}
func (e *globalRebEntry) CleanupPrevious(entry xactionGlobalEntry) {
	// Function called with lock
	if entry == nil {
		return
	}
	xGlobalReb := entry.(*globalRebEntry).xact

	if !xGlobalReb.Finished() {
		xGlobalReb.Abort()
		for i := 0; i < xGlobalReb.runnerCnt; i++ {
			<-xGlobalReb.confirmCh
		}
		close(xGlobalReb.confirmCh)
	}
}

type localRebEntry struct {
	baseGlobalEntry
	runnerCnt int
	xact      *xactLocalReb
}

func (e *localRebEntry) Start(id int64) error {
	xLocalReb := &xactLocalReb{
		xactRebBase: makeXactRebBase(id, localRebType, e.runnerCnt),
	}
	e.xact = xLocalReb
	return nil
}
func (e *localRebEntry) Get() cmn.Xact                                          { return e.xact }
func (e *localRebEntry) Kind() string                                           { return cmn.ActLocalReb }
func (e *localRebEntry) EndRenewOnPrevious(entry xactionGlobalEntry) (end bool) { return false }
func (e *localRebEntry) ActOnPrevious(entry xactionGlobalEntry)                 {}

func (e *localRebEntry) CleanupPrevious(entry xactionGlobalEntry) {
	if entry == nil {
		return
	}

	lRebEntry := entry.(*localRebEntry)
	if !lRebEntry.xact.Finished() {
		lRebEntry.xact.Abort()

		for i := 0; i < lRebEntry.xact.runnerCnt; i++ {
			<-lRebEntry.xact.confirmCh
		}
		close(lRebEntry.xact.confirmCh)
	}
}

type electionEntry struct {
	baseGlobalEntry
	p    *proxyrunner
	vr   *VoteRecord
	xact *xactElection
}

func (e *electionEntry) Start(id int64) error {
	xele := &xactElection{
		XactBase:    *cmn.NewXactBase(id, cmn.ActElection),
		proxyrunner: e.p,
		vr:          e.vr,
	}
	e.xact = xele
	return nil
}
func (e *electionEntry) Get() cmn.Xact { return e.xact }
func (e *electionEntry) Kind() string  { return cmn.ActElection }

type evictDeleteEntry struct {
	baseGlobalEntry
	evict bool
	xact  *xactEvictDelete
}

func (e *evictDeleteEntry) Start(id int64) error {
	xdel := &xactEvictDelete{XactBase: *cmn.NewXactBase(id, e.Kind())}
	e.xact = xdel
	return nil
}
func (e *evictDeleteEntry) Get() cmn.Xact { return e.xact }
func (e *evictDeleteEntry) Kind() string {
	if e.evict {
		return cmn.ActEvictObjects
	}
	return cmn.ActDelete
}
func (e *evictDeleteEntry) EndRenewOnPrevious(entry xactionGlobalEntry) (end bool) { return false }
func (e *evictDeleteEntry) ActOnPrevious(entry xactionGlobalEntry)                 {}

type downloaderEntry struct {
	baseGlobalEntry
	t    *targetrunner
	xact *downloader.Downloader
}

func (e *downloaderEntry) Start(id int64) error {
	xdl, err := downloader.NewDownloader(e.t, e.t.statsif, fs.Mountpaths, id, cmn.Download)
	if err != nil {
		return err
	}

	e.xact = xdl
	go xdl.Run()
	return nil
}
func (e *downloaderEntry) Get() cmn.Xact { return e.xact }
func (e *downloaderEntry) Kind() string  { return cmn.ActDownload }

// Base implementations

type (
	baseEntry struct {
		stats stats.BaseXactStats
	}

	baseGlobalEntry struct {
		baseEntry
	}
)

func (*baseGlobalEntry) IsGlobal() bool                             { return true }
func (b *baseGlobalEntry) CleanupPrevious(entry xactionGlobalEntry) {}
func (b *baseGlobalEntry) EndRenewOnPrevious(entry xactionGlobalEntry) (end bool) {
	return !entry.Get().Finished()
}
func (b *baseGlobalEntry) ActOnPrevious(entry xactionGlobalEntry) {
	if demandEntry, ok := entry.Get().(cmn.XactDemand); ok {
		demandEntry.Renew()
	}
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

// STATS

func (e *globalRebEntry) Stats() stats.XactStats {
	e.stats.FromXact(e.xact, "")
	e.stats.FillFromTrunner(getstorstatsrunner())
	return &e.stats
}

func (e *lruEntry) Stats() stats.XactStats         { return e.stats.FromXact(e.xact, "") }
func (e *localRebEntry) Stats() stats.XactStats    { return e.stats.FromXact(e.xact, "") }
func (e *electionEntry) Stats() stats.XactStats    { return e.stats.FromXact(e.xact, "") }
func (e *evictDeleteEntry) Stats() stats.XactStats { return e.stats.FromXact(e.xact, "") }
func (e *downloaderEntry) Stats() stats.XactStats  { return e.stats.FromXact(e.xact, "") }
func (e *prefetchEntry) Stats() stats.XactStats    { return e.stats.FromXact(e.xact, "") }
