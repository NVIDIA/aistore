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

func (p *prfFactory) Start() (err error) {
	if p.msg.BlobThreshold > 0 {
		if a := int64(minChunkSize << 2); p.msg.BlobThreshold < a {
			return fmt.Errorf("blob-threshold (size) is too small: must be at least %s", cos.ToSizeIEC(a, 0))
		}
	}

	b := p.Bck
	if err = b.Init(core.T.Bowner()); err != nil {
		return err
	}
	if b.IsAIS() {
		return fmt.Errorf("bucket %s is not _remote_ (can only prefetch remote buckets)", b)
	}
	p.xctn, err = newPrefetch(&p.Args, p.Kind(), b, p.msg)
	return err
}

func (*prfFactory) Kind() string     { return apc.ActPrefetchObjects }
func (p *prfFactory) Get() core.Xact { return p.xctn }

func (*prfFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newPrefetch(xargs *xreg.Args, kind string, bck *meta.Bck, msg *apc.PrefetchMsg) (r *prefetch, err error) {
	r = &prefetch{config: cmn.GCO.Get(), msg: msg}

	err = r.lriterator.init(r, &msg.ListRange, bck)
	if err != nil {
		return nil, err
	}
	r.InitBase(xargs.UUID, kind, bck)
	r.latestVer = bck.VersionConf().ValidateWarmGet || msg.LatestVer
	return r, nil
}

func (r *prefetch) Run(wg *sync.WaitGroup) {
	wg.Done()
	err := r.lriterator.run(r, core.T.Sowner().Get())
	if err != nil {
		r.AddErr(err, 5, cos.SmoduleXs) // duplicated?
	}
	r.Finish()
}

func (r *prefetch) do(lom *core.LOM, lrit *lriterator) {
	var (
		err   error
		size  int64
		ecode int
	)

	lom.Lock(false)
	oa, deleted, err := lom.LoadLatest(r.latestVer || r.msg.BlobThreshold > 0) // NOTE: shortcut to find size
	lom.Unlock(false)

	// handle assorted returns
	switch {
	case deleted: // remotely
		debug.Assert(r.latestVer && err != nil)
		if lrit.lrp != lrpList {
			return // deleted or not found remotely, prefix or range
		}
		goto eret
	case oa != nil:
		// not latest
		size = oa.Size
	case err == nil:
		return // nothing to do
	case !cmn.IsErrObjNought(err):
		goto eret
	}

	// Minimal locking, optimistic concurrency ====================================================
	// Not setting atime (a.k.a. access time) as prefetching != actual access.
	//
	// On the other hand, zero atime makes the object's lifespan in the cache too short - the first
	// housekeeping traversal will remove it. Using negative `-now` value for subsequent correction
	// (see core/lcache.go).                                             ==========================
	lom.SetAtimeUnix(-time.Now().UnixNano())

	if r.msg.BlobThreshold > 0 && size >= r.msg.BlobThreshold {
		err = _prefetchBlob(lom, oa)
	} else {
		ecode, err = core.T.GetCold(context.Background(), lom, cmn.OwtGetPrefetchLock)
		if err == nil { // done
			r.ObjsAdd(1, lom.SizeBytes())
		}
	}

	if err == nil { // done
		return
	}
	if cos.IsNotExist(err, ecode) && lrit.lrp != lrpList {
		return // not found, prefix or range
	}
eret:
	r.AddErr(err, 5, cos.SmoduleXs)
}

// TODO -- FIXME: initial, hardcoded, and simplified
func _prefetchBlob(lom *core.LOM, oa *cmn.ObjAttrs) error {
	var (
		total time.Duration
		sleep = 5 * time.Second
		lom2  = core.AllocLOM(lom.ObjName)
	)
	if err := lom2.InitBck(lom.Bucket()); err != nil {
		return err
	}
	xctn, err := core.T.GetColdBlob(lom2, oa)
	if err != nil {
		return err
	}
	for !xctn.Finished() {
		time.Sleep(sleep)
		total += sleep
		if total >= time.Minute {
			break
		}
	}
	if xctn.Finished() {
		return xctn.AbortErr()
	}
	nlog.Warningln(xctn.Name(), "- is taking more than 1 minute to complete")
	return nil // (leaving x-blob dangling)
}

func (r *prefetch) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
