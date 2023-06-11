// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// sparse logging
const (
	logPending = 256
	logError   = 32
)

type (
	putFactory struct {
		xreg.RenewBase
		xctn *XactPut
		lom  *cluster.LOM
	}
	XactPut struct {
		// implements cluster.Xact interface
		xact.DemandBase
		// runtime
		workers *mpather.WorkerGroup
		workCh  chan cluster.LIF
		// init
		mirror cmn.MirrorConf
		total  atomic.Int64
		config *cmn.Config
	}
)

// interface guard
var (
	_ cluster.Xact   = (*XactPut)(nil)
	_ xreg.Renewable = (*putFactory)(nil)
)

////////////////
// putFactory //
////////////////

func (*putFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &putFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, lom: args.Custom.(*cluster.LOM)}
	return p
}

func (p *putFactory) Start() error {
	slab, err := p.T.PageMM().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	debug.AssertNoErr(err)
	xctn, err := runXactPut(p.lom, slab, p.T)
	if err != nil {
		glog.Error(err)
		return err
	}
	p.xctn = xctn
	return nil
}

func (*putFactory) Kind() string        { return apc.ActPutCopies }
func (p *putFactory) Get() cluster.Xact { return p.xctn }

func (p *putFactory) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

// main
func runXactPut(lom *cluster.LOM, slab *memsys.Slab, t cluster.Target) (r *XactPut, err error) {
	mirror := *lom.MirrorConf()
	if !mirror.Enabled {
		return nil, errors.New("mirroring disabled, nothing to do")
	}
	tname := t.String()
	if err = fs.ValidateNCopies(tname, int(mirror.Copies)); err != nil {
		return
	}
	bck := lom.Bck()
	r = &XactPut{mirror: mirror, workCh: make(chan cluster.LIF, mirror.Burst)}
	r.DemandBase.Init(cos.GenUUID(), apc.ActPutCopies, bck, 0 /*use default*/)
	r.workers = mpather.NewWorkerGroup(&mpather.WorkerGroupOpts{
		Callback:  r.workCb,
		Slab:      slab,
		QueueSize: mirror.Burst,
	})
	// Run
	go r.Run(nil)
	return
}

/////////////
// XactPut //
/////////////

// mpather/worker callback (one worker per mountpath)
func (r *XactPut) workCb(lom *cluster.LOM, buf []byte) {
	copies := int(lom.Bprops().Mirror.Copies)
	size, err := addCopies(lom, copies, buf)
	if (err != nil && (r.total.Load()%logError) == 0) || r.config.FastV(5, glog.SmoduleMirror) {
		var s string
		if err != nil {
			s = "Error: " + err.Error() + ": "
		}
		glog.Infof("%s: %s%s, copies=%d, size=%d", r.Base.Name(), s, lom.Cname(), copies, size)
	}
	r.DecPending() // to support action renewal on-demand
	cluster.FreeLOM(lom)
}

// control logic: stop and idle timer
// (LOMs get dispatched directly to workers)
func (r *XactPut) Run(*sync.WaitGroup) {
	glog.Infoln(r.Name())
	r.config = cmn.GCO.Get()
	r.workers.Run()
	for {
		select {
		case <-r.IdleTimer():
			err := r.stop()
			r.Finish(err)
			return
		case errCause := <-r.ChanAbort():
			if err := r.stop(); err != nil {
				glog.Errorf("%s aborted (cause %v), traversal err %v", r, errCause, err)
			} else {
				glog.Infof("%s aborted (cause %v)", r, errCause)
			}
			r.Finish(cmn.NewErrAborted(r.Name(), "", errCause))
			return
		}
	}
}

// main method: replicate onto a given (and different) mountpath
func (r *XactPut) Repl(lom *cluster.LOM) {
	debug.Assert(!r.Finished(), r.String())
	total := r.total.Inc()

	pending, max := int(r.Pending()), r.mirror.Burst
	if pending > 1 && pending >= max && (total%logPending) == 0 {
		glog.Warningf("%s: pending=%d exceeded %d=burst (total=%d)", r, pending, max, total)
	}
	r.IncPending() // ref-count via base to support on-demand action

	if ok := r.workers.Do(lom); !ok {
		err := fmt.Errorf("%s: failed to post %s work", r, lom)
		debug.AssertNoErr(err)
		return
	}

	// [throttle]
	// a bit of back-pressure when approaching the fixed boundary
	if pending > 1 && max > 10 {
		// increase the chances for the burst of PUTs to subside
		// but only to a point
		if pending > max/2 && pending < max-max/8 {
			time.Sleep(mpather.ThrottleAvgDur)
		}
	}
}

func (r *XactPut) stop() (err error) {
	r.DemandBase.Stop()
	n := r.workers.Stop()
	if nn := drainWorkCh(r.workCh); nn > 0 {
		n += nn
	}
	if n > 0 {
		r.SubPending(n)
		err = fmt.Errorf("%s: dropped (ie., failed to mirror) %d object%s", r, n, cos.Plural(n))
	}
	return err
}

func (r *XactPut) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
