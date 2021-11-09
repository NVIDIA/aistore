// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/lru"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
	"github.com/NVIDIA/aistore/xs"
)

// triggers by the out-of-space condition or suspicion of thereof
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
	glog.Warningf("%s: %s", t.si, cs)
	go t.runStoreCleanup("" /*uuid*/, nil /*wg*/)
	if cs.OOS {
		// TODO: when cleanup is running LRU must take a back seat (rm sleep & revise)
		go func() {
			config := cmn.GCO.Get()
			sleep := cos.MinDuration(config.Timeout.MaxHostBusy.D(), time.Minute)
			time.Sleep(sleep)
			t.runLRU("" /*uuid*/, nil /*wg*/, false)
		}()
	}
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
	ini := lru.InitLRU{
		T:                   t,
		Xaction:             xlru.(*lru.Xaction),
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
	lru.Run(&ini)
}

func (t *targetrunner) runStoreCleanup(id string, wg *sync.WaitGroup, bcks ...cmn.Bck) {
	regToIC := id == ""
	if regToIC {
		id = cos.GenUUID()
	}
	rns := xreg.RenewStoreCleanup(id)
	if rns.IsRunning() {
		if wg != nil {
			wg.Done()
		}
		return
	}
	debug.AssertNoErr(rns.Err) // see xcln.WhenPrevIsRunning() and xreg logic
	xcln := rns.Entry.Get()
	if regToIC && xcln.ID() == id {
		// pre-existing UUID: notify IC members
		regMsg := xactRegMsg{UUID: id, Kind: cmn.ActStoreCleanup, Srcs: []string{t.si.ID()}}
		msg := t.newAmsgActVal(cmn.ActRegGlobalXaction, regMsg)
		t.bcastAsyncIC(msg)
	}
	ini := xs.InitStoreCln{
		T:       t,
		Xaction: xcln.(*xs.StoreClnXaction),
		StatsT:  t.statsT,
		Buckets: bcks,
		WG:      wg,
	}
	xcln.AddNotif(&xaction.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: []string{equalIC}, F: t.callerNotifyFin},
		Xact:      xcln,
	})
	xs.RunStoreCleanup(&ini)
}

func (t *targetrunner) TrashNonExistingBucket(bck cmn.Bck) {
	if err := cluster.NewBckEmbed(bck).Init(t.owner.bmd); err != nil {
		if cmn.IsErrBckNotFound(err) || cmn.IsErrRemoteBckNotFound(err) {
			const op = "trash-non-existing"
			err = fs.DestroyBucket(op, bck, 0 /*unknown bid*/)
			glog.Infof("%s: %v", op, err)
		}
	}
}
