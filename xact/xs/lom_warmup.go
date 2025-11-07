// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"strconv"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	llcFactory struct {
		xreg.RenewBase
		xctn *xactLLC
	}
	xactLLC struct {
		xact.BckJog
	}
)

// interface guard
var (
	_ core.Xact      = (*xactLLC)(nil)
	_ xreg.Renewable = (*llcFactory)(nil)
)

////////////////
// llcFactory //
////////////////

func (*llcFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &llcFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}}
	p.Bck = bck
	return p
}

func (p *llcFactory) Start() error {
	xctn := newXactLLC(p.UUID(), p.Bck)
	p.xctn = xctn
	go xctn.Run(nil)
	return nil
}

func (*llcFactory) Kind() string     { return apc.ActLoadLomCache }
func (p *llcFactory) Get() core.Xact { return p.xctn }

func (*llcFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) { return xreg.WprUse, nil }

/////////////
// xactLLC //
/////////////

func newXactLLC(uuid string, bck *meta.Bck) (r *xactLLC) {
	r = &xactLLC{}
	mpopts := &mpather.JgroupOpts{
		CTs:      []string{fs.ObjCT},
		VisitObj: func(*core.LOM, []byte) error { return nil },
		DoLoad:   mpather.Load,
	}
	mpopts.Bck.Copy(bck.Bucket())
	r.BckJog.Init(uuid, apc.ActLoadLomCache, bck, mpopts, cmn.GCO.Get())
	return
}

func (r *xactLLC) Run(*sync.WaitGroup) {
	r.BckJog.Run()
	nlog.Infoln(r.Name())
	err := r.BckJog.Wait()
	if err != nil {
		r.AddErr(err)
	}
	r.Finish()
}

func (r *xactLLC) ctlmsg() string {
	nv := r.NumVisits()
	if nv == 0 {
		return ""
	}
	var sb strings.Builder
	sb.Grow(16)
	sb.WriteString(", visited:")
	sb.WriteString(strconv.FormatInt(nv, 10))
	return sb.String()
}

func (r *xactLLC) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.AddBaseSnap(snap)

	snap.CtlMsg = r.ctlmsg()
	if snap.CtlMsg != "" {
		nlog.Infoln(r.Name(), "ctlmsg (", snap.CtlMsg, ")")
	}

	snap.IdleX = r.IsIdle()
	return
}
