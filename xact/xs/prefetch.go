// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"fmt"
	"sync"
	"time"

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

// utilizes mult-object lr-iterator

type (
	prfFactory struct {
		xreg.RenewBase
		xctn *prefetch
		msg  *apc.PrefetchMsg
	}
	prefetch struct {
		config *cmn.Config
		msg    *apc.PrefetchMsg
		lriterator
		xact.Base
		latestVer bool
	}
)

func (*prfFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	msg := args.Custom.(*apc.PrefetchMsg)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	np := &prfFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, msg: msg}
	return np
}

func (p *prfFactory) Start() error {
	b := p.Bck
	if err := b.Init(core.T.Bowner()); err != nil {
		return err
	}
	if b.IsAIS() {
		return fmt.Errorf("bucket %s is not _remote_ (can only prefetch remote buckets)", b)
	}
	p.xctn = newPrefetch(&p.Args, p.Kind(), b, p.msg)
	return nil
}

func (*prfFactory) Kind() string     { return apc.ActPrefetchObjects }
func (p *prfFactory) Get() core.Xact { return p.xctn }

func (*prfFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newPrefetch(xargs *xreg.Args, kind string, bck *meta.Bck, msg *apc.PrefetchMsg) *prefetch {
	r := &prefetch{config: cmn.GCO.Get(), msg: msg}

	r.lriterator.init(r, &msg.ListRange)
	r.InitBase(xargs.UUID, kind, bck)
	r.lriterator.xctn = r

	r.latestVer = bck.VersionConf().ValidateWarmGet || msg.LatestVer
	return r
}

func (r *prefetch) Run(wg *sync.WaitGroup) {
	wg.Done()
	smap := core.T.Sowner().Get()
	if r.msg.IsList() {
		_ = r.iterList(r, smap)
	} else {
		_ = r.rangeOrPref(r, smap)
	}
	r.Finish()
}

func (r *prefetch) do(lom *core.LOM, lrit *lriterator) {
	var (
		err     error
		errCode int
	)
	lom.Lock(false)
	if err = lom.Load(true /*cache it*/, true /*locked*/); err != nil {
		if !cmn.IsErrObjNought(err) {
			lom.Unlock(false)
			goto eret
		}
	} else {
		if !r.latestVer {
			lom.Unlock(false)
			return // nothing to do
		}
		var eq bool
		if eq, errCode, err = lom.CheckRemoteMD(true /*rlocked*/, false /*synchronize*/); eq {
			lom.Unlock(false)
			return // nothing to do
		}
		if err != nil {
			lom.Unlock(false)
			if cos.IsNotExist(err, errCode) && lrit.lrp != lrpList {
				return // not found, prefix or range
			}
			goto eret
		}
	}
	lom.Unlock(false)

	// Minimal locking, optimistic concurrency ====================================================
	// Not setting atime (a.k.a. access time) as prefetching != actual access.
	//
	// On the other hand, zero atime makes the object's lifespan in the cache too short - the first
	// housekeeping traversal will remove it. Using neative `-now` value for subsequent correction
	// (see core/lcache.go).                                             ==========================
	lom.SetAtimeUnix(-time.Now().UnixNano())

	errCode, err = core.T.GetCold(context.Background(), lom, cmn.OwtGetPrefetchLock)
	if err == nil { // done
		r.ObjsAdd(1, lom.SizeBytes())
		return
	}
	if cos.IsNotExist(err, errCode) && lrit.lrp != lrpList {
		return // not found, prefix or range
	}
eret:
	r.AddErr(err)
	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln("Warning:", err)
	}
}

func (r *prefetch) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
