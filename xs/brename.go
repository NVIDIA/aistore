// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	MovFactory struct {
		xreg.BaseEntry
		xact  *bckRename
		t     cluster.Target
		uuid  string
		phase string
		args  *xreg.BckRenameArgs
	}
	bckRename struct {
		xaction.XactBase
		t       cluster.Target
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		rebID   string
	}
)

// interface guard
var (
	_ cluster.Xact = (*bckRename)(nil)
	_ xreg.Factory = (*MovFactory)(nil)
)

func (*MovFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	p := &MovFactory{t: args.T, uuid: args.UUID, args: args.Custom.(*xreg.BckRenameArgs)}
	p.Bck = bck
	return p
}

func (*MovFactory) Kind() string        { return cmn.ActMoveBck }
func (p *MovFactory) Get() cluster.Xact { return p.xact }

func (p *MovFactory) Start() error {
	p.xact = newBckRename(p.uuid, p.Kind(), p.Bck, p.t, p.args.BckFrom, p.args.BckTo, p.args.RebID)
	return nil
}

func (p *MovFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	if p.phase == cmn.ActBegin {
		if !prevEntry.Get().Finished() {
			err = fmt.Errorf("%s: cannot(%s=>%s) older rename still in progress", p.Kind(), p.args.BckFrom, p.args.BckTo)
			return
		}
		// TODO: more checks
	}
	prev := prevEntry.(*MovFactory)
	bckEq := prev.args.BckTo.Equal(p.args.BckTo, false /*sameID*/, false /* same backend */)
	if prev.phase == cmn.ActBegin && p.phase == cmn.ActCommit && bckEq {
		prev.phase = cmn.ActCommit // transition
		wpr = xreg.WprUse
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)",
		p.Kind(), prev.args.BckFrom, prev.args.BckTo, prev.phase, p.phase, p.args.BckFrom)
	return
}

func newBckRename(uuid, kind string, bck *cluster.Bck, t cluster.Target, bckFrom, bckTo *cluster.Bck, rebID string) (x *bckRename) {
	x = &bckRename{t: t, bckFrom: bckFrom, bckTo: bckTo, rebID: rebID}
	x.InitBase(uuid, kind, bck)
	return
}

func (r *bckRename) String() string { return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.bckFrom) }

// NOTE: assuming that rebalance takes longer than resilvering
func (r *bckRename) Run() {
	var (
		onlyRunning bool
		finished    bool
		flt         = xreg.XactFilter{ID: r.rebID, Kind: cmn.ActRebalance, OnlyRunning: &onlyRunning}
	)
	glog.Infoln(r.String())
	for !finished {
		time.Sleep(10 * time.Second)
		rebStats, err := xreg.GetStats(flt)
		cos.AssertNoErr(err)
		for _, stat := range rebStats {
			finished = finished || stat.Finished()
		}
	}

	r.t.BMDVersionFixup(nil, r.bckFrom.Bck) // piggyback bucket renaming (last step) on getting updated BMD
	r.Finish(nil)
}
