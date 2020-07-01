// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/housekeep/lru"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction/demand"
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
type lruEntry struct {
	baseGlobalEntry
	id   string
	xact *lru.Xaction
}

func (e *lruEntry) Start(_ cmn.Bck) error {
	e.xact = &lru.Xaction{XactBase: *cmn.NewXactBase(cmn.XactBaseID(e.id), cmn.ActLRU)}
	return nil
}

func (e *lruEntry) Kind() string  { return cmn.ActLRU }
func (e *lruEntry) Get() cmn.Xact { return e.xact }

func (e *lruEntry) preRenewHook(_ globalEntry) bool { return true }

//
// rebalanceEntry
//

type rebID int64

type rebalanceEntry struct {
	id          rebID // rebalance id
	xact        *Rebalance
	statsRunner *stats.Trunner
}

var (
	// interface guard
	_ cmn.XactID = rebID(0)
)

func (id rebID) String() string { return fmt.Sprintf("g%d", id) }
func (id rebID) Int() int64     { return int64(id) }
func (id rebID) Compare(other string) int {
	var (
		o   int64
		err error
	)
	if o, err = strconv.ParseInt(other, 10, 64); err == nil {
		goto compare
	} else if o, err = strconv.ParseInt(other[1:], 10, 64); err == nil {
		goto compare
	} else {
		return -1
	}
compare:
	if int64(id) < o {
		return -1
	}
	if int64(id) > o {
		return 1
	}
	return 0
}

func (e *rebalanceEntry) Start(_ cmn.Bck) error {
	xreb := &Rebalance{RebBase: makeXactRebBase(e.id, cmn.ActRebalance)}
	xreb.statsRunner = e.statsRunner
	e.xact = xreb
	return nil
}
func (e *rebalanceEntry) Kind() string  { return cmn.ActRebalance }
func (e *rebalanceEntry) Get() cmn.Xact { return e.xact }

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
// resilver|rebalance helper
//
func makeXactRebBase(id cmn.XactID, kind string) RebBase {
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
	id   string
	xact *Resilver
}

func (e *resilverEntry) Start(_ cmn.Bck) error {
	e.xact = &Resilver{
		RebBase: makeXactRebBase(cmn.XactBaseID(e.id), cmn.ActResilver),
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

func (e *electionEntry) Start(_ cmn.Bck) error {
	e.xact = &Election{
		XactBase: *cmn.NewXactBase(cmn.XactBaseID(""), cmn.ActElection),
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

func (e *downloaderEntry) Start(_ cmn.Bck) error {
	xdl := downloader.NewDownloader(e.t, e.statsT, fs.Mountpaths)
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

func (b *baseGlobalEntry) preRenewHook(previousEntry globalEntry) (keep bool) {
	e := previousEntry.Get()
	_, keep = e.(demand.XactDemand)
	return
}

func (b *baseGlobalEntry) postRenewHook(_ globalEntry) {}
