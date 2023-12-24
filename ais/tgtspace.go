// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/space"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

const (
	// - note that an API call (e.g. CLI) will go through anyway
	// - compare with cmn/cos/oom.go
	minAutoDetectInterval = 10 * time.Minute
)

var (
	lastTrigOOS atomic.Int64
)

// triggers by an out-of-space condition or a suspicion of thereof
func (t *target) OOS(csRefreshed *fs.CapStatus) (cs fs.CapStatus) {
	var errCap error
	if csRefreshed != nil {
		cs = *csRefreshed
		errCap = cs.Err()
	} else {
		var err error
		cs, err, errCap = fs.CapRefresh(nil, nil)
		if err != nil {
			nlog.Errorln(t.String(), "failed to update capacity stats:", err)
			return
		}
	}
	if errCap == nil {
		return // unlikely; nothing to do
	}
	if prev := lastTrigOOS.Load(); mono.Since(prev) < minAutoDetectInterval {
		nlog.Warningf("%s: _not_ running store cleanup: (%v, %v), %s", t, prev, minAutoDetectInterval, cs.String())
		return
	}

	nlog.Warningln(t.String(), "running store cleanup:", cs.String())
	// run serially, cleanup first and LRU second, iff out-of-space persists
	go func() {
		cs := t.runStoreCleanup("" /*uuid*/, nil /*wg*/)
		lastTrigOOS.Store(mono.NanoTime())
		if cs.Err() != nil {
			nlog.Warningln(t.String(), "still out of space, running LRU eviction now:", cs.String())
			t.runLRU("" /*uuid*/, nil /*wg*/, false)
		}
	}()
	return
}

func (t *target) runLRU(id string, wg *sync.WaitGroup, force bool, bcks ...cmn.Bck) {
	regToIC := id == ""
	if regToIC {
		id = cos.GenUUID()
	}
	rns := xreg.RenewLRU(id)
	if rns.Err != nil || rns.IsRunning() {
		debug.Assert(rns.Err == nil || cmn.IsErrXactUsePrev(rns.Err))
		if wg != nil {
			wg.Done()
		}
		return
	}
	xlru := rns.Entry.Get()
	if regToIC && xlru.ID() == id {
		// pre-existing UUID: notify IC members
		regMsg := xactRegMsg{UUID: id, Kind: apc.ActLRU, Srcs: []string{t.SID()}}
		msg := t.newAmsgActVal(apc.ActRegGlobalXaction, regMsg)
		t.bcastAsyncIC(msg)
	}
	ini := space.IniLRU{
		Xaction:             xlru.(*space.XactLRU),
		Config:              cmn.GCO.Get(),
		StatsT:              t.statsT,
		Buckets:             bcks,
		GetFSUsedPercentage: ios.GetFSUsedPercentage,
		GetFSStats:          ios.GetFSStats,
		WG:                  wg,
		Force:               force,
	}
	xlru.AddNotif(&xact.NotifXact{
		Base: nl.Base{When: core.UponTerm, Dsts: []string{equalIC}, F: t.notifyTerm},
		Xact: xlru,
	})
	space.RunLRU(&ini)
}

func (t *target) runStoreCleanup(id string, wg *sync.WaitGroup, bcks ...cmn.Bck) fs.CapStatus {
	regToIC := id == ""
	if regToIC {
		id = cos.GenUUID()
	}
	rns := xreg.RenewStoreCleanup(id)
	if rns.Err != nil || rns.IsRunning() {
		debug.Assert(rns.Err == nil || cmn.IsErrXactUsePrev(rns.Err))
		if wg != nil {
			wg.Done()
		}
		return fs.CapStatus{}
	}
	xcln := rns.Entry.Get()
	if regToIC && xcln.ID() == id {
		// pre-existing UUID: notify IC members
		regMsg := xactRegMsg{UUID: id, Kind: apc.ActStoreCleanup, Srcs: []string{t.SID()}}
		msg := t.newAmsgActVal(apc.ActRegGlobalXaction, regMsg)
		t.bcastAsyncIC(msg)
	}
	ini := space.IniCln{
		Xaction: xcln.(*space.XactCln),
		Config:  cmn.GCO.Get(),
		StatsT:  t.statsT,
		Buckets: bcks,
		WG:      wg,
	}
	xcln.AddNotif(&xact.NotifXact{
		Base: nl.Base{When: core.UponTerm, Dsts: []string{equalIC}, F: t.notifyTerm},
		Xact: xcln,
	})
	return space.RunCleanup(&ini)
}
