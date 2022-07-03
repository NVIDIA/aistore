// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/space"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// triggers by an out-of-space condition or a suspicion of thereof
func (t *target) OOS(csRefreshed *fs.CapStatus) (cs fs.CapStatus) {
	var err error
	if csRefreshed != nil {
		cs = *csRefreshed
	} else {
		cs, err = fs.RefreshCapStatus(nil, nil)
		if err != nil {
			glog.Errorf("%s: %v", t, err)
			return
		}
	}
	if cs.Err != nil {
		glog.Warningf("%s: %s", t, cs.String())
	}
	// run serially, cleanup first and LRU iff out-of-space persists
	go func() {
		cs := t.runStoreCleanup("" /*uuid*/, nil /*wg*/)
		if cs.Err != nil {
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
		debug.Assert(rns.Err == nil || cmn.IsErrUsePrevXaction(rns.Err))
		if wg != nil {
			wg.Done()
		}
		return
	}
	xlru := rns.Entry.Get()
	if regToIC && xlru.ID() == id {
		// pre-existing UUID: notify IC members
		regMsg := xactRegMsg{UUID: id, Kind: apc.ActLRU, Srcs: []string{t.si.ID()}}
		msg := t.newAmsgActVal(apc.ActRegGlobalXaction, regMsg)
		t.bcastAsyncIC(msg)
	}
	ini := space.IniLRU{
		T:                   t,
		Xaction:             xlru.(*space.XactLRU),
		StatsT:              t.statsT,
		Buckets:             bcks,
		GetFSUsedPercentage: ios.GetFSUsedPercentage,
		GetFSStats:          ios.GetFSStats,
		WG:                  wg,
		Force:               force,
	}
	xlru.AddNotif(&xact.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: []string{equalIC}, F: t.callerNotifyFin},
		Xact:      xlru,
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
		debug.Assert(rns.Err == nil || cmn.IsErrUsePrevXaction(rns.Err))
		if wg != nil {
			wg.Done()
		}
		return fs.CapStatus{}
	}
	xcln := rns.Entry.Get()
	if regToIC && xcln.ID() == id {
		// pre-existing UUID: notify IC members
		regMsg := xactRegMsg{UUID: id, Kind: apc.ActStoreCleanup, Srcs: []string{t.si.ID()}}
		msg := t.newAmsgActVal(apc.ActRegGlobalXaction, regMsg)
		t.bcastAsyncIC(msg)
	}
	ini := space.IniCln{
		T:       t,
		Xaction: xcln.(*space.XactCln),
		StatsT:  t.statsT,
		Buckets: bcks,
		WG:      wg,
	}
	xcln.AddNotif(&xact.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: []string{equalIC}, F: t.callerNotifyFin},
		Xact:      xcln,
	})
	return space.RunCleanup(&ini)
}
