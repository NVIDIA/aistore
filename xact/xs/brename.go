// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// tunables
const (
	bmvAvgWait  = 2 * time.Minute
	bmvMaxWait  = 2 * time.Hour
	bmvMaxSleep = 30 * time.Second
)

type (
	bckRename struct {
		t       cluster.TargetExt
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		rebID   string
		xact.Base
	}
	bmvFactory struct {
		xreg.RenewBase
		xctn  *bckRename
		cargs *xreg.BckRenameArgs
		phase string
	}
	TestBmvFactory = bmvFactory
)

// interface guard
var (
	_ cluster.Xact   = (*bckRename)(nil)
	_ xreg.Renewable = (*bmvFactory)(nil)
)

////////////////
// bmvFactory //
////////////////

func (*bmvFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	p := &bmvFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, cargs: args.Custom.(*xreg.BckRenameArgs)}
	return p
}

func (*bmvFactory) Kind() string        { return apc.ActMoveBck }
func (p *bmvFactory) Get() cluster.Xact { return p.xctn }

func (p *bmvFactory) Start() error {
	p.xctn = newBckRename(p.UUID(), p.Kind(), p.cargs.RebID, p.Bck, p.cargs.BckFrom, p.cargs.BckTo)
	p.xctn.t = p.cargs.T // cluster.TargetExt
	return nil
}

func (p *bmvFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	if p.phase == apc.ActBegin {
		if !prevEntry.Get().Finished() {
			err = fmt.Errorf("%s: cannot(%s=>%s) older rename still in progress",
				p.Kind(), p.cargs.BckFrom, p.cargs.BckTo)
			return
		}
		// TODO: more checks
	}
	prev := prevEntry.(*bmvFactory)
	bckEq := prev.cargs.BckTo.Equal(p.cargs.BckTo, false /*sameID*/, false /* same backend */)
	if prev.phase == apc.ActBegin && p.phase == apc.ActCommit && bckEq {
		prev.phase = apc.ActCommit // transition
		wpr = xreg.WprUse
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)",
		p.Kind(), prev.cargs.BckFrom, prev.cargs.BckTo, prev.phase, p.phase, p.cargs.BckFrom)
	return
}

///////////////
// bckRename //
///////////////

func newBckRename(uuid, kind, rebID string, bck, bckFrom, bckTo *cluster.Bck) (x *bckRename) {
	// NOTE: `bck` = `bckTo` = (the new name) while `bckFrom` is the existing bucket to be renamed
	debug.Assert(bck.Equal(bckTo, false, true), bck.String()+" vs "+bckTo.String())

	debug.Assert(xact.IsValidRebID(rebID), rebID)
	x = &bckRename{bckFrom: bckFrom, bckTo: bckTo, rebID: rebID}
	x.InitBase(uuid, kind, bck)
	return
}

// NOTE: assuming that rebalance takes longer than resilvering
func (r *bckRename) Run(wg *sync.WaitGroup) {
	var (
		total time.Duration
		flt   = xact.Flt{ID: r.rebID, Kind: apc.ActRebalance}
		sleep = cos.ProbingFrequency(bmvAvgWait)
	)
	glog.Infoln(r.Name())
	wg.Done()
loop:
	for total < bmvMaxWait {
		time.Sleep(sleep)
		total += sleep
		rebStats, err := xreg.GetSnap(flt)
		debug.AssertNoErr(err)
		for _, stat := range rebStats {
			if stat.Finished() || stat.IsAborted() {
				break loop
			}
		}
		if total > bmvAvgWait {
			sleep = cos.MinDuration(sleep+sleep/2, bmvMaxSleep)
		}
	}
	var err error
	if total >= bmvMaxWait {
		err = fmt.Errorf("%s: timeout", r)
		glog.Error(err)
	}
	r.t.BMDVersionFixup(nil, r.bckFrom.Clone()) // piggyback bucket renaming (last step) on getting updated BMD
	r.Finish(err)
}

func (r *bckRename) String() string {
	return fmt.Sprintf("%s <= %s", r.Base.String(), r.bckFrom)
}

func (r *bckRename) Name() string {
	return fmt.Sprintf("%s <= %s", r.Base.Name(), r.bckFrom)
}

func (r *bckRename) FromTo() (*cluster.Bck, *cluster.Bck) { return r.bckFrom, r.bckTo }

func (r *bckRename) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	f, t := r.FromTo()
	snap.SrcBck, snap.DstBck = f.Clone(), t.Clone()
	return
}
