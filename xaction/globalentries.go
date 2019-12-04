// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/housekeep/lru"
	"github.com/NVIDIA/aistore/stats"
)

type globalEntry interface {
	entry
	// pre-renew: returns true iff the current active one exists and is either
	// - ok to keep running as is, or
	// - has been renew(ed) and is still ok
	preRenewHook(previousEntry globalEntry) (done bool)
	// post-renew hook
	postRenewHook(previousEntry globalEntry)
}

//
// lruEntry
//
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

func (e *lruEntry) preRenewHook(_ globalEntry) bool { return true }

type prefetchEntry struct {
	baseGlobalEntry
	xact *prefetch
	r    *stats.Trunner
}

//
// prefetchEntry
//
func (e *prefetchEntry) Start(id int64) error {
	e.xact = &prefetch{XactBase: *cmn.NewXactBase(id, cmn.ActPrefetch), r: e.r}
	return nil
}
func (e *prefetchEntry) Kind() string  { return cmn.ActPrefetch }
func (e *prefetchEntry) Get() cmn.Xact { return e.xact }

func (e *prefetchEntry) preRenewHook(_ globalEntry) bool { return true }

//
// globalRebEntry
//
type globalRebEntry struct {
	baseGlobalEntry
	xact        *GlobalReb
	stats       stats.RebalanceTargetStats
	statRunner  *stats.Trunner
	smapVersion int64
	globRebID   int64
	runnerCnt   int
}

func (e *globalRebEntry) Start(id int64) error {
	xGlobalReb := &GlobalReb{
		RebBase:     makeXactRebBase(id, cmn.ActGlobalReb, e.runnerCnt),
		SmapVersion: e.smapVersion,
	}
	xGlobalReb.XactBase.SetGID(e.globRebID)
	e.xact = xGlobalReb
	return nil
}
func (e *globalRebEntry) Kind() string  { return cmn.ActGlobalReb }
func (e *globalRebEntry) Get() cmn.Xact { return e.xact }

func (e *globalRebEntry) preRenewHook(previousEntry globalEntry) (keep bool) {
	xGlobalReb := previousEntry.(*globalRebEntry).xact
	if xGlobalReb.SmapVersion > e.smapVersion {
		glog.Errorf("(reb: %s) Smap v%d is greater than v%d", xGlobalReb, xGlobalReb.SmapVersion, e.smapVersion)
		keep = true
	} else if xGlobalReb.SmapVersion == e.smapVersion {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s already running, nothing to do", xGlobalReb)
		}
		keep = true
	}
	return
}

func (e *globalRebEntry) postRenewHook(previousEntry globalEntry) {
	xGlobalReb := previousEntry.(*globalRebEntry).xact
	xGlobalReb.Abort()
	for i := 0; i < xGlobalReb.runnerCnt; i++ {
		<-xGlobalReb.confirmCh
	}
	close(xGlobalReb.confirmCh)
}

func (e *globalRebEntry) Stats(xact cmn.Xact) stats.XactStats {
	cmn.Assert(xact == e.xact)
	e.stats.FillFromXact(e.xact)
	e.stats.FillFromTrunner(e.statRunner)
	return &e.stats
}

//
// local|global reb helper
//
func makeXactRebBase(id int64, kind string, runnerCnt int) RebBase {
	return RebBase{
		XactBase:  *cmn.NewXactBase(id, kind),
		runnerCnt: runnerCnt,
		confirmCh: make(chan struct{}, runnerCnt),
	}
}

//
// localRebEntry
//
type localRebEntry struct {
	baseGlobalEntry
	xact      *LocalReb
	runnerCnt int
}

func (e *localRebEntry) Start(id int64) error {
	xLocalReb := &LocalReb{
		RebBase: makeXactRebBase(id, cmn.ActLocalReb, e.runnerCnt),
	}
	e.xact = xLocalReb
	return nil
}
func (e *localRebEntry) Get() cmn.Xact { return e.xact }
func (e *localRebEntry) Kind() string  { return cmn.ActLocalReb }

func (e *localRebEntry) postRenewHook(previousEntry globalEntry) {
	lRebEntry := previousEntry.(*localRebEntry)
	lRebEntry.xact.Abort()
	for i := 0; i < lRebEntry.xact.runnerCnt; i++ {
		<-lRebEntry.xact.confirmCh
	}
	close(lRebEntry.xact.confirmCh)
}

//
// electionEntry
//
type electionEntry struct {
	baseGlobalEntry
	xact *Election
}

func (e *electionEntry) Start(id int64) error {
	xele := &Election{
		XactBase: *cmn.NewXactBase(id, cmn.ActElection),
	}
	e.xact = xele
	return nil
}
func (e *electionEntry) Get() cmn.Xact { return e.xact }
func (e *electionEntry) Kind() string  { return cmn.ActElection }

func (e *electionEntry) preRenewHook(previousEntry globalEntry) bool { return true }

//
// evictDeleteEntry
//
type evictDeleteEntry struct {
	baseGlobalEntry
	xact  *evictDelete
	evict bool
}

func (e *evictDeleteEntry) Start(id int64) error {
	xdel := &evictDelete{XactBase: *cmn.NewXactBase(id, e.Kind())}
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

func (e *evictDeleteEntry) preRenewHook(previousEntry globalEntry) bool { return true }

type downloaderEntry struct {
	baseGlobalEntry
	xact *downloader.Downloader
	t    cluster.Target
	stat stats.Tracker
}

func (e *downloaderEntry) Start(id int64) error {
	xdl := downloader.NewDownloader(e.t, e.stat, fs.Mountpaths, id, cmn.Download)
	e.xact = xdl
	go xdl.Run()
	return nil
}
func (e *downloaderEntry) Get() cmn.Xact { return e.xact }
func (e *downloaderEntry) Kind() string  { return cmn.ActDownload }

//
// base*Entry
//
type (
	baseEntry struct {
		stats stats.BaseXactStats
	}
	baseGlobalEntry struct {
		baseEntry
	}
)

func (*baseGlobalEntry) IsGlobal() bool { return true }
func (*baseGlobalEntry) IsTask() bool   { return false }

func (b *baseGlobalEntry) preRenewHook(previousEntry globalEntry) (done bool) {
	e := previousEntry.Get()
	if demandEntry, ok := e.(cmn.XactDemand); ok {
		demandEntry.Renew()
		done = true
	}
	return
}

func (b *baseGlobalEntry) postRenewHook(_ globalEntry) {}

func (b *baseGlobalEntry) Stats(xact cmn.Xact) stats.XactStats { return b.stats.FillFromXact(xact) }
