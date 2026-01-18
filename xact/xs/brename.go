// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	BckRename struct {
		*XactTCB
		ctlmsg string
	}
	bmvFactory struct {
		xreg.RenewBase
		xctn  *BckRename
		cargs *xreg.TCBArgs
	}
	TestBmvFactory = bmvFactory
)

// interface guard
var (
	_ core.Xact      = (*BckRename)(nil)
	_ xreg.Renewable = (*bmvFactory)(nil)
)

////////////////
// bmvFactory //
////////////////

func (*bmvFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &bmvFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, cargs: args.Custom.(*xreg.TCBArgs)}
	return p
}

func (*bmvFactory) Kind() string     { return apc.ActMoveBck }
func (p *bmvFactory) Get() core.Xact { return p.xctn }

func (p *bmvFactory) Start() error {
	xctn, err := newBckRename(p.UUID(), p.Kind(), p.cargs)
	if err != nil {
		return err
	}
	p.xctn = xctn
	return nil
}

func (p *bmvFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*bmvFactory)

	if p.UUID() != prev.UUID() {
		return wpr, cmn.NewErrXactUsePrev(prevEntry.Get().String())
	}
	bckEq := prev.xctn.args.BckFrom.Equal(p.xctn.args.BckFrom, true /*same BID*/, true /*same backend*/)
	debug.Assert(bckEq)
	return xreg.WprUse, nil
}

///////////////
// BckRename //
///////////////

// NOTE: `bck` = `bckTo` = (the new name) while `bckFrom` is the existing bucket to be renamed
func newBckRename(uuid, kind string, tcbArgs *xreg.TCBArgs) (xctn *BckRename, err error) {
	xctn = &BckRename{}
	xtcb, err := newXactTCB(uuid, kind, tcbArgs)
	if err != nil {
		return nil, err
	}
	xctn.XactTCB = xtcb
	_ = xctn.CtlMsg()
	return xctn, nil
}

// BckRename xaction is a wrapper around XactTCB that adds the following:
// - if not aborted; call BMDVersionFixup to piggyback bucket renaming to remove bckFrom from BMD (see `whatRenamedLB` in proxy.go)
// -
func (r *BckRename) Run(wg *sync.WaitGroup) {
	r.XactTCB.run(wg)
	if r.IsAborted() {
		nlog.Infoln(r.Name(), "aborted", r.AbortErr())
	} else {
		core.T.BMDVersionFixup(nil, r.XactTCB.args.BckFrom.Clone()) // piggyback bucket renaming (last step) on getting updated BMD
	}
	r.Finish()
}

func (r *BckRename) String() string { return r.CtlMsg() }
func (r *BckRename) Name() string   { return r.CtlMsg() }

func (r *BckRename) FromTo() (*meta.Bck, *meta.Bck) {
	return r.XactTCB.args.BckFrom, r.XactTCB.args.BckTo
}

func (r *BckRename) CtlMsg() string {
	if r.ctlmsg != "" {
		return r.ctlmsg
	}
	r.ctlmsg = r.XactTCB.formatCtlMsg(true /*rename*/)
	return r.ctlmsg
}

func (r *BckRename) Snap() (snap *core.Snap) {
	snap = r.Base.NewSnap(r)

	f, t := r.FromTo()
	snap.SrcBck, snap.DstBck = f.Clone(), t.Clone()
	return
}
