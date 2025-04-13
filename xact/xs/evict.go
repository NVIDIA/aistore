// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	evdFactory struct {
		xreg.RenewBase
		xctn *evictDelete
		msg  *apc.ListRange
		kind string
	}
	evictDelete struct {
		config *cmn.Config
		lrit
		xact.Base
	}
)

// interface guard
var (
	_ core.Xact      = (*evictDelete)(nil)
	_ xreg.Renewable = (*evdFactory)(nil)
	_ lrwi           = (*evictDelete)(nil)
	_ lrxact         = (*evictDelete)(nil)
)

//
// evict/delete; utilizes mult-object lr-iterator
//

func (p *evdFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	if p.kind == apc.ActEvictRemoteBck {
		return &evdFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: p.kind}
	}
	msg := args.Custom.(*apc.ListRange)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	return &evdFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: p.kind, msg: msg}
}

func (p *evdFactory) Start() (err error) {
	p.xctn, err = newEvictDelete(&p.Args, p.kind, p.Bck, p.msg)
	return err
}

func (p *evdFactory) Kind() string   { return p.kind }
func (p *evdFactory) Get() core.Xact { return p.xctn }

func (*evdFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newEvictDelete(xargs *xreg.Args, kind string, bck *meta.Bck, msg *apc.ListRange) (*evictDelete, error) {
	r := &evictDelete{config: cmn.GCO.Get()}
	if kind == apc.ActEvictRemoteBck {
		r.InitBase(xargs.UUID, kind, "" /*ctlmsg*/, bck)
		r.Finish()
		return r, nil
	}

	// default num-workers hardcoded
	// (currently, always num mountpaths)
	if err := r.lrit.init(r, msg, bck, nwpDflt); err != nil {
		return nil, err
	}

	var sb strings.Builder
	sb.Grow(80)
	msg.Str(&sb, r.lrp == lrpPrefix)
	r.InitBase(xargs.UUID, kind, sb.String() /*ctlmsg*/, bck)

	return r, nil
}

func (r *evictDelete) Run(wg *sync.WaitGroup) {
	wg.Done()
	err := r.lrit.run(r, core.T.Sowner().Get(), false /*prealloc buf*/)
	if err != nil {
		r.AddErr(err, 5, cos.SmoduleXs) // duplicated?
	}
	r.lrit.wait()
	r.Finish()
}

func (r *evictDelete) do(lom *core.LOM, lrit *lrit, _ []byte) {
	ecode, err := core.T.DeleteObject(lom, r.Kind() == apc.ActEvictObjects)
	if err == nil { // done
		r.ObjsAdd(1, lom.Lsize(true))
		return
	}
	if cos.IsNotExist(err, ecode) || cmn.IsErrObjNought(err) {
		if lrit.lrp == lrpList {
			goto eret // unlike range and prefix
		}
		return
	}
eret:
	r.AddErr(err, 5, cos.SmoduleXs)
}

func (r *evictDelete) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
