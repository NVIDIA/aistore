// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	bckRename struct {
		xact.Base
		t       cluster.Target
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		rebID   string
	}
	bmvFactory struct {
		xreg.RenewBase
		xctn  *bckRename
		phase string
		args  *xreg.BckRenameArgs
	}
	TestBmvFactory = bmvFactory
)

// interface guard
var (
	_ cluster.Xact   = (*bckRename)(nil)
	_ xreg.Renewable = (*bmvFactory)(nil)
)

func (*bmvFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	p := &bmvFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, args: args.Custom.(*xreg.BckRenameArgs)}
	return p
}

func (*bmvFactory) Kind() string        { return apc.ActMoveBck }
func (p *bmvFactory) Get() cluster.Xact { return p.xctn }

func (p *bmvFactory) Start() error {
	p.xctn = newBckRename(p.UUID(), p.Kind(), p.Bck, p.T, p.args.BckFrom, p.args.BckTo, p.args.RebID)
	return nil
}

func (p *bmvFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	if p.phase == apc.ActBegin {
		if !prevEntry.Get().Finished() {
			err = fmt.Errorf("%s: cannot(%s=>%s) older rename still in progress",
				p.Kind(), p.args.BckFrom, p.args.BckTo)
			return
		}
		// TODO: more checks
	}
	prev := prevEntry.(*bmvFactory)
	bckEq := prev.args.BckTo.Equal(p.args.BckTo, false /*sameID*/, false /* same backend */)
	if prev.phase == apc.ActBegin && p.phase == apc.ActCommit && bckEq {
		prev.phase = apc.ActCommit // transition
		wpr = xreg.WprUse
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)",
		p.Kind(), prev.args.BckFrom, prev.args.BckTo, prev.phase, p.phase, p.args.BckFrom)
	return
}

func newBckRename(uuid, kind string, bck *cluster.Bck, t cluster.Target, bckFrom, bckTo *cluster.Bck, rebID string) (x *bckRename) {
	// NOTE: `bck` = `bckTo` = (the new name) while `bckFrom` is the existing bucket to be renamed
	debug.AssertMsg(bck.Equal(bckTo, false, true), bck.String()+" vs "+bckTo.String())

	x = &bckRename{t: t, bckFrom: bckFrom, bckTo: bckTo, rebID: rebID}
	x.InitBase(uuid, kind, bck)
	return
}

func (r *bckRename) String() string {
	return fmt.Sprintf("%s <= %s", r.Base.String(), r.bckFrom)
}

func (r *bckRename) Name() string {
	return fmt.Sprintf("%s <= %s", r.Base.Name(), r.bckFrom)
}

func (r *bckRename) FromTo() (*cluster.Bck, *cluster.Bck) { return r.bckFrom, r.bckTo }

// NOTE: assuming that rebalance takes longer than resilvering
func (r *bckRename) Run(wg *sync.WaitGroup) {
	var (
		onlyRunning bool
		finished    bool
		flt         = xreg.XactFilter{ID: r.rebID, Kind: apc.ActRebalance, OnlyRunning: &onlyRunning}
	)
	glog.Infoln(r.Name())
	wg.Done()
	for !finished {
		time.Sleep(10 * time.Second)
		rebStats, err := xreg.GetSnap(flt)
		debug.AssertNoErr(err)
		for _, stat := range rebStats {
			finished = finished || stat.Finished()
		}
	}

	r.t.BMDVersionFixup(nil, r.bckFrom.Bck) // piggyback bucket renaming (last step) on getting updated BMD
	r.Finish(nil)
}
