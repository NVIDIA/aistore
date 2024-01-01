// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"net/http"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
		lriterator
		xact.Base
		config *cmn.Config
	}
)

//
// evict/delete; utilizes mult-object lr-iterator
//

func (p *evdFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	msg := args.Custom.(*apc.ListRange)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	np := &evdFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: p.kind, msg: msg}
	return np
}

func (p *evdFactory) Start() error {
	p.xctn = newEvictDelete(&p.Args, p.kind, p.Bck, p.msg)
	return nil
}

func (p *evdFactory) Kind() string   { return p.kind }
func (p *evdFactory) Get() core.Xact { return p.xctn }

func (*evdFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newEvictDelete(xargs *xreg.Args, kind string, bck *meta.Bck, msg *apc.ListRange) (ed *evictDelete) {
	ed = &evictDelete{config: cmn.GCO.Get()}
	ed.lriterator.init(ed, msg)
	ed.InitBase(xargs.UUID, kind, bck)
	return
}

func (r *evictDelete) Run(wg *sync.WaitGroup) {
	wg.Done()
	smap := core.T.Sowner().Get()
	if r.msg.IsList() {
		_ = r.iterList(r, smap)
	} else {
		_ = r.rangeOrPref(r, smap)
	}
	r.Finish()
}

func (r *evictDelete) do(lom *core.LOM, lrit *lriterator) {
	errCode, err := core.T.DeleteObject(lom, r.Kind() == apc.ActEvictObjects)
	if err == nil { // done
		r.ObjsAdd(1, lom.SizeBytes(true))
		return
	}
	if errCode == http.StatusNotFound || cmn.IsErrObjNought(err) {
		if lrit.lrp == lrpList {
			goto eret // unlike range and prefix
		}
		return
	}
eret:
	r.AddErr(err)
	if r.config.FastV(5, cos.SmoduleXs) {
		nlog.Warningln(err)
	}
}

func (r *evictDelete) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
