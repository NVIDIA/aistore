// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
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
		msg  *apc.EvdMsg
		kind string
	}
	evictDelete struct {
		config *cmn.Config
		msg    *apc.EvdMsg
		lrit
		xact.Base
	}
)

// interface guard
var (
	_ core.Xact      = (*evictDelete)(nil)
	_ xreg.Renewable = (*evdFactory)(nil)
	_ lrwi           = (*evictDelete)(nil)
)

//
// evict/delete; utilizes mult-object lr-iterator
//

func (p *evdFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	if p.kind == apc.ActEvictRemoteBck {
		return &evdFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: p.kind}
	}
	msg := args.Custom.(*apc.EvdMsg)
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

func newEvictDelete(xargs *xreg.Args, kind string, bck *meta.Bck, msg *apc.EvdMsg) (*evictDelete, error) {
	r := &evictDelete{config: cmn.GCO.Get(), msg: msg}
	if kind == apc.ActEvictRemoteBck {
		r.InitBase(xargs.UUID, kind, bck)
		r.Finish()
		return r, nil
	}

	var lsflags uint64
	if msg.NonRecurs {
		lsflags = apc.LsNoRecursion
	}
	if err := r.lrit.init(r, &msg.ListRange, bck, lsflags, msg.NumWorkers, 0 /*burst*/); err != nil {
		return nil, err
	}
	r.InitBase(xargs.UUID, kind, bck)

	return r, nil
}

func (r *evictDelete) CtlMsg() string {
	var sb cos.SB
	sb.Init(80)
	r.msg.Str(&sb, r.lrit.lrp == lrpPrefix)
	if r.msg.NonRecurs {
		sb.WriteString(", non-recurs")
	}
	return sb.String()
}

func (r *evictDelete) Run(wg *sync.WaitGroup) {
	wg.Done()
	err := r.lrit.run(r, core.T.Sowner().Get(), false /*prealloc buf*/)
	if err != nil {
		r.AddErr(err, 5, cos.ModXs) // duplicated?
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
	r.AddErr(err, 5, cos.ModXs)
}

func (r *evictDelete) Snap() (snap *core.Snap) {
	snap = r.Base.NewSnap(r)
	snap.Pack(0, len(r.lrit.nwp.workers), r.lrit.nwp.chanFull.Load())
	return
}
