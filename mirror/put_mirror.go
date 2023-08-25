// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"errors"
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
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
		nlog.Errorln(err)
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
		Callback:  r.do,
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

// (one worker per mountpath)
func (r *XactPut) do(lom *cluster.LOM, buf []byte) {
	copies := int(lom.Bprops().Mirror.Copies)

	lom.Lock(true)
	size, err := addCopies(lom, copies, buf)
	lom.Unlock(true)

	if err != nil {
		r.AddErr(err)
		if r.config.FastV(5, cos.SmoduleMirror) {
			nlog.Infof("Error: %v", err)
		}
	} else {
		r.ObjsAdd(1, size)
	}
	r.DecPending() // (see IncPending below)
	cluster.FreeLOM(lom)
}

// control logic: stop and idle timer
// (LOMs get dispatched directly to workers)
func (r *XactPut) Run(*sync.WaitGroup) {
	var err error
	nlog.Infoln(r.Name())
	r.config = cmn.GCO.Get()
	r.workers.Run()
loop:
	for {
		select {
		case <-r.IdleTimer():
			break loop
		case <-r.ChanAbort():
			break loop
		}
	}

	err = r.stop()
	r.AddErr(err)
	r.Finish()
}

// main method
func (r *XactPut) Repl(lom *cluster.LOM) {
	debug.Assert(!r.Finished(), r.String())

	// ref-count on-demand, decrement via worker.Callback = r.do
	r.IncPending()
	if err := r.workers.PostLIF(lom, r.String()); err != nil {
		r.DecPending()
		r.Abort(fmt.Errorf("%s: %v", r, err))
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
		err = fmt.Errorf("%s: dropped %d object%s", r, n, cos.Plural(n))
	}
	return
}

func (r *XactPut) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
