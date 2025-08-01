// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
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
		bckFrom *meta.Bck
		bckTo   *meta.Bck
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
	_ core.Xact      = (*bckRename)(nil)
	_ xreg.Renewable = (*bmvFactory)(nil)
)

////////////////
// bmvFactory //
////////////////

func (*bmvFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &bmvFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, cargs: args.Custom.(*xreg.BckRenameArgs)}
	return p
}

func (*bmvFactory) Kind() string     { return apc.ActMoveBck }
func (p *bmvFactory) Get() core.Xact { return p.xctn }

func (p *bmvFactory) Start() error {
	p.xctn = newBckRename(p.UUID(), p.Kind(), p.cargs.RebID, p.Bck, p.cargs.BckFrom, p.cargs.BckTo)
	return nil
}

func (p *bmvFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	if p.phase == apc.Begin2PC {
		if !prevEntry.Get().Finished() {
			err = fmt.Errorf("%s: cannot(%s=>%s) older rename still in progress",
				p.Kind(), p.cargs.BckFrom, p.cargs.BckTo)
			return
		}
		// TODO: more checks
	}
	prev := prevEntry.(*bmvFactory)
	bckEq := prev.cargs.BckTo.Equal(p.cargs.BckTo, false /*sameID*/, false /* same backend */)
	if prev.phase == apc.Begin2PC && p.phase == apc.Commit2PC && bckEq {
		prev.phase = apc.Commit2PC // transition
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

func newBckRename(uuid, kind, rebID string, bck, bckFrom, bckTo *meta.Bck) (x *bckRename) {
	// NOTE: `bck` = `bckTo` = (the new name) while `bckFrom` is the existing bucket to be renamed
	debug.Assert(bck.Equal(bckTo, false, true), bck.String()+" vs "+bckTo.String())

	debug.Assert(xact.IsValidRebID(rebID), rebID)
	x = &bckRename{bckFrom: bckFrom, bckTo: bckTo, rebID: rebID}
	x.InitBase(uuid, kind, "via "+rebID /*ctlmsg*/, bck)
	return
}

// NOTE: assuming that rebalance takes longer than resilvering
func (r *bckRename) Run(wg *sync.WaitGroup) {
	var (
		total time.Duration
		flt   = xreg.Flt{ID: r.rebID, Kind: apc.ActRebalance}
		sleep = cos.ProbingFrequency(bmvAvgWait)
	)
	nlog.Infoln(r.Name())
	wg.Done()
loop:
	for total < bmvMaxWait {
		time.Sleep(sleep)
		total += sleep
		rebStats, err := xreg.GetSnap(&flt)
		debug.AssertNoErr(err)
		for _, stat := range rebStats {
			if stat.Finished() || stat.IsAborted() {
				break loop
			}
		}
		if total > bmvAvgWait {
			sleep = min(sleep+sleep/2, bmvMaxSleep)
		}
	}
	if total >= bmvMaxWait {
		r.AddErr(fmt.Errorf("timeout %s", total))
	}
	core.T.BMDVersionFixup(nil, r.bckFrom.Clone()) // piggyback bucket renaming (last step) on getting updated BMD
	r.Finish()
}

func (r *bckRename) String() string {
	return fmt.Sprintf("%s <= %s", r.Base.String(), r.bckFrom)
}

func (r *bckRename) Name() string {
	return fmt.Sprintf("%s <= %s", r.Base.Name(), r.bckFrom)
}

func (r *bckRename) FromTo() (*meta.Bck, *meta.Bck) { return r.bckFrom, r.bckTo }

func (r *bckRename) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	f, t := r.FromTo()
	snap.SrcBck, snap.DstBck = f.Clone(), t.Clone()
	return
}
