// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/housekeep/lru"
	"github.com/NVIDIA/aistore/stats"
)

type globalEntry interface {
	baseEntry
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

func (e *lruEntry) Start(id string, _ cmn.Bck) error {
	e.xact = &lru.Xaction{XactBase: *cmn.NewXactBase(id, cmn.ActLRU)}
	return nil
}

func (e *lruEntry) Kind() string  { return cmn.ActLRU }
func (e *lruEntry) Get() cmn.Xact { return e.xact }

func (e *lruEntry) preRenewHook(_ globalEntry) bool { return true }

//
// rebalanceEntry
//
type rebalanceEntry struct {
	baseGlobalEntry
	id          int64 // rebalance id
	xact        *Rebalance
	statRunner  *stats.Trunner
	smapVersion int64
}

func (e *rebalanceEntry) Start(id string, _ cmn.Bck) error {
	xreb := &Rebalance{
		RebBase:     makeXactRebBase(id, cmn.ActRebalance),
		SmapVersion: e.smapVersion,
	}
	xreb.XactBase.SetGID(e.id)
	e.xact = xreb
	return nil
}
func (e *rebalanceEntry) Kind() string  { return cmn.ActRebalance }
func (e *rebalanceEntry) Get() cmn.Xact { return e.xact }

func (e *rebalanceEntry) preRenewHook(previousEntry globalEntry) (keep bool) {
	xreb := previousEntry.(*rebalanceEntry).xact
	if xreb.SmapVersion > e.smapVersion {
		glog.Errorf("(reb: %s) Smap v%d is greater than v%d", xreb, xreb.SmapVersion, e.smapVersion)
		keep = true
	} else if xreb.SmapVersion == e.smapVersion {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s already running, nothing to do", xreb)
		}
		keep = true
	}
	return
}

func (e *rebalanceEntry) postRenewHook(previousEntry globalEntry) {
	xreb := previousEntry.(*rebalanceEntry).xact
	xreb.Abort()
	xreb.WaitForFinish()
}

func (e *rebalanceEntry) Stats(xact cmn.Xact) stats.XactStats {
	cmn.Assert(xact == e.xact)
	rebStats := &stats.RebalanceTargetStats{BaseXactStats: *stats.NewXactStats(e.xact)}
	rebStats.FillFromTrunner(e.statRunner)
	return rebStats
}

//
// resilver|rebalance helper
//
func makeXactRebBase(id, kind string) RebBase {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return RebBase{
		XactBase: *cmn.NewXactBase(id, kind),
		wg:       wg,
	}
}

//
// resilverEntry
//
type resilverEntry struct {
	baseGlobalEntry
	xact *Resilver
}

func (e *resilverEntry) Start(id string, _ cmn.Bck) error {
	e.xact = &Resilver{
		RebBase: makeXactRebBase(id, cmn.ActResilver),
	}
	return nil
}
func (e *resilverEntry) Get() cmn.Xact { return e.xact }
func (e *resilverEntry) Kind() string  { return cmn.ActResilver }

func (e *resilverEntry) postRenewHook(previousEntry globalEntry) {
	xresilver := previousEntry.(*resilverEntry).xact
	xresilver.Abort()
	xresilver.WaitForFinish()
}

//
// electionEntry
//
type electionEntry struct {
	baseGlobalEntry
	xact *Election
}

func (e *electionEntry) Start(id string, _ cmn.Bck) error {
	e.xact = &Election{
		XactBase: *cmn.NewXactBase(id, cmn.ActElection),
	}
	return nil
}
func (e *electionEntry) Get() cmn.Xact { return e.xact }
func (e *electionEntry) Kind() string  { return cmn.ActElection }

func (e *electionEntry) preRenewHook(_ globalEntry) bool { return true }

//
// downloadEntry
//

type downloaderEntry struct {
	baseGlobalEntry
	xact   *downloader.Downloader
	t      cluster.Target
	statsT stats.Tracker
}

func (e *downloaderEntry) Start(id string, _ cmn.Bck) error {
	xdl := downloader.NewDownloader(e.t, e.statsT, fs.Mountpaths, id, cmn.Download)
	e.xact = xdl
	go xdl.Run()
	return nil
}
func (e *downloaderEntry) Get() cmn.Xact { return e.xact }
func (e *downloaderEntry) Kind() string  { return cmn.ActDownload }

//
// baseGlobalEntry
//

type (
	baseGlobalEntry struct{}
)

func (b *baseGlobalEntry) preRenewHook(previousEntry globalEntry) (done bool) {
	e := previousEntry.Get()
	if demandEntry, ok := e.(cmn.XactDemand); ok {
		demandEntry.Renew()
		done = true
	}
	return
}

func (b *baseGlobalEntry) postRenewHook(_ globalEntry) {}

func (b *baseGlobalEntry) Stats(xact cmn.Xact) stats.XactStats {
	return stats.NewXactStats(xact)
}
