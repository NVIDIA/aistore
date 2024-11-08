// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO:
// - user-assigned (configurable) num-workers
// - jogger(s) per mountpath type concurrency
// - blob downloading (when msg.BlobThreshold > 0):
//   - configurable num concurrent x-blob
//   - configurable chunk-size and num-workers

const maxNumBlobDls = 16

type (
	prfFactory struct {
		xreg.RenewBase
		xctn *prefetch
		msg  *apc.PrefetchMsg
	}
	prefetch struct {
		config *cmn.Config
		msg    *apc.PrefetchMsg
		lrit
		xact.Base
		blob struct {
			pending []core.Xact
			num     atomic.Int32
			mu      sync.Mutex
		}
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

	err = r.lrit.init(r, &msg.ListRange, bck, msg.NumWorkers)
	if err != nil {
		return nil, err
	}
	r.InitBase(xargs.UUID, kind, bck)
	r.latestVer = bck.VersionConf().ValidateWarmGet || msg.LatestVer

	if r.msg.BlobThreshold > 0 {
		r.blob.pending = make([]core.Xact, 0, min(maxNumBlobDls, 8))
	}
	return r, nil
}

func (r *prefetch) Run(wg *sync.WaitGroup) {
	nlog.Infoln(r.Name())

	wg.Done()

	err := r.lrit.run(r, core.T.Sowner().Get())
	if err != nil {
		r.AddErr(err, 5, cos.SmoduleXs) // duplicated?
	}
	r.lrit.wait()

	// pending blob-downloads
	if r.blob.num.Load() > 0 {
		if r.IsAborted() {
			for _, xctn := range r.blob.pending {
				xctn.Abort(r.AbortErr())
			}
		} else {
			r._wait1h()
		}
	}

	r.Finish()
}

func (r *prefetch) do(lom *core.LOM, lrit *lrit) {
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

	// NOTE ref 6735188: _not_ setting negative atime, flushing lom metadata

	if r.msg.BlobThreshold > 0 && size >= r.msg.BlobThreshold && r.blob.num.Load() < maxNumBlobDls {
		err = r.blobdl(lom, oa)
	} else {
		// OwtGetPrefetchLock: minimal locking, optimistic concurrency
		ecode, err = core.T.GetCold(context.Background(), lom, cmn.OwtGetPrefetchLock)
		if err == nil { // done
			r.ObjsAdd(1, lom.Lsize())
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

func (r *prefetch) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}

//
// async, via blob-downloader --------------------------
//

func (r *prefetch) blobdl(lom *core.LOM, oa *cmn.ObjAttrs) error {
	params := &core.BlobParams{
		Lom: core.AllocLOM(lom.ObjName),
		Msg: &apc.BlobMsg{},
	}
	if err := params.Lom.InitBck(lom.Bucket()); err != nil {
		return err
	}
	notif := &xact.NotifXact{
		Base: nl.Base{
			When: core.UponTerm,
			F:    r._done,
		},
	}
	xctn, err := core.T.GetColdBlob(params, oa)
	if err != nil {
		return err
	}
	notif.Xact = xctn
	xctn.AddNotif(notif)

	if xctn.Finished() {
		return nil
	}
	r.blob.mu.Lock()
	r.blob.num.Inc()
	r.blob.pending = append(r.blob.pending, xctn)
	r.blob.mu.Unlock()
	return nil
}

func (r *prefetch) _done(n core.Notif, err error, aborted bool) {
	msg := n.ToNotifMsg(aborted)
	r.blob.mu.Lock()
repeat:
	for {
		for i, xctn := range r.blob.pending {
			if xctn.ID() != msg.UUID && !xctn.Finished() {
				continue
			}
			// shift left
			l := len(r.blob.pending)
			copy(r.blob.pending[i:], r.blob.pending[i+1:])
			r.blob.pending = r.blob.pending[:l-1]
			r.blob.num.Dec()
			continue repeat
		}
		break
	}
	r.blob.mu.Unlock()
	if err != nil {
		nlog.Warningf("%s: %s finished w/ err: %v", r.Name(), msg.String(), err)
	} else if aborted {
		nlog.Warningf("%s: %s aborted", r.Name(), msg.String())
	} else if cmn.Rom.FastV(4, cos.SmoduleXs) {
		var s string
		if num := int(r.blob.num.Load()); num > 0 {
			s = " (num-pending " + strconv.Itoa(num) + ")"
		}
		nlog.Infof("%s: %s done%s", r.Name(), msg.String(), s)
	}
}

func (r *prefetch) _wait1h() {
	const c = " waiting for blob downloads:"
	var (
		sleep = 4 * time.Second
		total time.Duration
	)
	for {
		time.Sleep(sleep)
		if r.blob.num.Load() <= 0 {
			break
		}
		total += sleep
		if total == 15*sleep {
			nlog.Warningln(r.Name()+": still"+c, r._pending2S())
			sleep *= 2
		} else if total == 30*sleep {
			nlog.Warningln(r.Name()+": still"+c, r._pending2S())
			sleep *= 2
		} else if total >= time.Hour {
			nlog.Warningln(r.Name()+": timed-out"+c, r._pending2S())
			break
		}
	}
}

func (r *prefetch) _pending2S() (s string) {
	r.blob.mu.Lock()
	for _, xctn := range r.blob.pending {
		s += xctn.ID() + ", "
	}
	r.blob.mu.Unlock()
	return s[:len(s)-2]
}
