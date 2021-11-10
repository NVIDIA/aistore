// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/space"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
)

// triggers by an out-of-space condition or a suspicion of thereof
func (t *targetrunner) OOS(csRefreshed *fs.CapStatus) (cs fs.CapStatus) {
	var err error
	if csRefreshed != nil {
		cs = *csRefreshed
	} else {
		cs, err = fs.RefreshCapStatus(nil, nil)
		if err != nil {
			glog.Errorf("%s: %v", t.si, err)
			return
		}
	}
	if cs.Err != nil {
		glog.Warningf("%s: %s", t.si, cs)
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

func (t *targetrunner) runLRU(id string, wg *sync.WaitGroup, force bool, bcks ...cmn.Bck) {
	regToIC := id == ""
	if regToIC {
		id = cos.GenUUID()
	}
	rns := xreg.RenewLRU(id)
	if rns.IsRunning() {
		if wg != nil {
			wg.Done()
		}
		return
	}
	debug.AssertNoErr(rns.Err) // see xlru.WhenPrevIsRunning() and xreg logic
	xlru := rns.Entry.Get()
	if regToIC && xlru.ID() == id {
		// pre-existing UUID: notify IC members
		regMsg := xactRegMsg{UUID: id, Kind: cmn.ActLRU, Srcs: []string{t.si.ID()}}
		msg := t.newAmsgActVal(cmn.ActRegGlobalXaction, regMsg)
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
	xlru.AddNotif(&xaction.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: []string{equalIC}, F: t.callerNotifyFin},
		Xact:      xlru,
	})
	space.RunLRU(&ini)
}

func (t *targetrunner) runStoreCleanup(id string, wg *sync.WaitGroup, bcks ...cmn.Bck) fs.CapStatus {
	regToIC := id == ""
	if regToIC {
		id = cos.GenUUID()
	}
	rns := xreg.RenewStoreCleanup(id)
	if rns.IsRunning() {
		if wg != nil {
			wg.Done()
		}
		return fs.CapStatus{}
	}
	debug.AssertNoErr(rns.Err) // see xcln.WhenPrevIsRunning() and xreg logic
	xcln := rns.Entry.Get()
	if regToIC && xcln.ID() == id {
		// pre-existing UUID: notify IC members
		regMsg := xactRegMsg{UUID: id, Kind: cmn.ActStoreCleanup, Srcs: []string{t.si.ID()}}
		msg := t.newAmsgActVal(cmn.ActRegGlobalXaction, regMsg)
		t.bcastAsyncIC(msg)
	}
	ini := space.IniCln{
		T:       t,
		Xaction: xcln.(*space.XactCln),
		StatsT:  t.statsT,
		Buckets: bcks,
		WG:      wg,
	}
	xcln.AddNotif(&xaction.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: []string{equalIC}, F: t.callerNotifyFin},
		Xact:      xcln,
	})
	return space.RunCleanup(&ini)
}

func (t *targetrunner) TrashNonExistingBucket(bck cmn.Bck) {
	const op = "trash-non-existing"
	b := cluster.NewBckEmbed(bck)
	err := b.Init(t.owner.bmd)
	if err == nil {
		return
	}
	if cmn.IsErrBckNotFound(err) || cmn.IsErrRemoteBckNotFound(err) {
		if err = fs.DestroyBucket(op, bck, 0 /*unknown bid*/); err == nil {
			glog.Infof("%s: %s %s", t.si, op, bck)
		} else {
			glog.Errorf("%s: failed to %s %s, err %v", t.si, op, bck, err)
		}
	}
}
