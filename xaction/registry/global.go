// Package registry provides core functionality for the AIStore extended actions registry.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package registry

import (
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/lru"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/runners"
)

type globalEntry interface {
	baseEntry
	// pre-renew: returns true iff the current active one exists and is either
	// - ok to keep running as is, or
	// - has been renew(ed) and is still ok
	preRenewHook(previousEntry globalEntry) (keep bool)
	// post-renew hook
	postRenewHook(previousEntry globalEntry)
}

//
// lruEntry
//
const lruIdleTime = 30 * time.Second

type lruEntry struct {
	baseGlobalEntry
	id   string
	xact *lru.Xaction
}

func (e *lruEntry) Start(_ cmn.Bck) error {
	e.xact = &lru.Xaction{
		XactDemandBase: *xaction.NewXactDemandBase(e.id, cmn.ActLRU, lruIdleTime),
		Renewed:        make(chan struct{}, 10),
		OkRemoveMisplaced: func() bool {
			g, l := GetRebMarked(), GetResilverMarked()
			return !g.Interrupted && !l.Interrupted && g.Xact == nil && l.Xact == nil
		},
	}
	e.xact.InitIdle()
	return nil
}

func (e *lruEntry) Kind() string      { return cmn.ActLRU }
func (e *lruEntry) Get() cluster.Xact { return e.xact }

func (e *lruEntry) preRenewHook(_ globalEntry) bool { return true }

//
// rebalanceEntry
//

type rebalanceEntry struct {
	id          xaction.RebID // rebalance id
	xact        *runners.Rebalance
	statsRunner *stats.Trunner
}

func (e *rebalanceEntry) Start(_ cmn.Bck) error {
	xreb := runners.NewRebalance(e.id, e.Kind(), e.statsRunner, GetRebMarked)
	e.xact = xreb
	return nil
}
func (e *rebalanceEntry) Kind() string      { return cmn.ActRebalance }
func (e *rebalanceEntry) Get() cluster.Xact { return e.xact }

func (e *rebalanceEntry) preRenewHook(previousEntry globalEntry) (keep bool) {
	xreb := previousEntry.(*rebalanceEntry)
	if xreb.id > e.id {
		glog.Errorf("(reb: %s) g%d is greater than g%d", xreb.xact, xreb.id, e.id)
		keep = true
	} else if xreb.id == e.id {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s already running, nothing to do", xreb.xact)
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

//
// resilverEntry
//
type resilverEntry struct {
	baseGlobalEntry
	id   string
	xact *runners.Resilver
}

func (e *resilverEntry) Start(_ cmn.Bck) error {
	e.xact = runners.NewResilver(e.id, e.Kind())
	return nil
}
func (e *resilverEntry) Get() cluster.Xact { return e.xact }
func (e *resilverEntry) Kind() string      { return cmn.ActResilver }

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
	xact *runners.Election
}

func (e *electionEntry) Start(_ cmn.Bck) error {
	e.xact = &runners.Election{
		XactBase: *xaction.NewXactBase(xaction.XactBaseID(""), cmn.ActElection),
	}
	return nil
}
func (e *electionEntry) Get() cluster.Xact { return e.xact }
func (e *electionEntry) Kind() string      { return cmn.ActElection }

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

func (e *downloaderEntry) Start(_ cmn.Bck) error {
	xdl := downloader.NewDownloader(e.t, e.statsT)
	e.xact = xdl
	go xdl.Run()
	return nil
}
func (e *downloaderEntry) Get() cluster.Xact { return e.xact }
func (e *downloaderEntry) Kind() string      { return cmn.ActDownload }

//
// baseGlobalEntry
//

type (
	baseGlobalEntry struct{}
)

func (b *baseGlobalEntry) preRenewHook(previousEntry globalEntry) (keep bool) {
	e := previousEntry.Get()
	_, keep = e.(xaction.XactDemand)
	return
}

func (b *baseGlobalEntry) postRenewHook(_ globalEntry) {}
