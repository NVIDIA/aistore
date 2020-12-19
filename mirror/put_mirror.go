// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

const (
	logNumProcessed = 256 // unit of house-keeping
)

type (
	putMirrorProvider struct {
		xreg.BaseBckEntry
		xact *XactPut

		t    cluster.Target
		uuid string
		lom  *cluster.LOM
	}
	XactPut struct {
		// implements cluster.Xact interface
		xaction.XactDemandBase
		// runtime
		workers *mpather.WorkerGroup
		workCh  chan *cluster.LOM
		// init
		mirror  cmn.MirrorConf
		total   atomic.Int64
		dropped int64
	}
)

// interface guard
var _ cluster.Xact = (*XactPut)(nil)

func (*putMirrorProvider) New(args xreg.XactArgs) xreg.BucketEntry {
	return &putMirrorProvider{t: args.T, uuid: args.UUID, lom: args.Custom.(*cluster.LOM)}
}

func (p *putMirrorProvider) Start(_ cmn.Bck) error {
	slab, err := p.t.MMSA().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	cmn.AssertNoErr(err)
	xact, err := RunXactPut(p.lom, slab)
	if err != nil {
		glog.Error(err)
		return err
	}
	p.xact = xact
	return nil
}
func (*putMirrorProvider) Kind() string        { return cmn.ActPutCopies }
func (p *putMirrorProvider) Get() cluster.Xact { return p.xact }

//
// public methods
//

func RunXactPut(lom *cluster.LOM, slab *memsys.Slab) (r *XactPut, err error) {
	var (
		availablePaths, _ = fs.Get()
		mpathCount        = len(availablePaths)
	)
	if mpathCount < 2 {
		return nil, fmt.Errorf("number of mountpaths (%d) is insufficient for local mirroring, exiting", mpathCount)
	}

	mirror := *lom.MirrorConf()
	r = &XactPut{
		XactDemandBase: *xaction.NewXactDemandBaseBck(cmn.ActPutCopies, lom.Bck().Bck),
		mirror:         mirror,
		workCh:         make(chan *cluster.LOM, mirror.Burst),
		workers: mpather.NewWorkerGroup(&mpather.WorkerGroupOpts{
			Slab:      slab,
			QueueSize: mirror.Burst,
			Callback: func(lom *cluster.LOM, buf []byte) {
				copies := int(lom.Bprops().Mirror.Copies)
				if _, err := addCopies(lom, copies, buf); err != nil {
					glog.Error(err)
				} else {
					r.ObjectsAdd(int64(copies))
					r.BytesAdd(lom.Size() * int64(copies))
				}
				r.DecPending() // to support action renewal on-demand
			},
		}),
	}
	r.InitIdle()

	// Run
	go r.Run()
	return
}

func (r *XactPut) Run() {
	glog.Infoln(r.String())

	r.workers.Run()

	for {
		select {
		case src := <-r.workCh:
			lom := src.Clone(src.FQN)
			if err := lom.Load(); err != nil {
				glog.Error(err)
				break
			}
			if ok := r.workers.Do(lom); !ok {
				glog.Errorf("failed to get post with path: %s", lom)
			}
		case <-r.IdleTimer():
			err := r.stop()
			r.Finish(err)
			return
		case <-r.ChanAbort():
			if err := r.stop(); err != nil {
				r.Finish(err)
			}
			r.Finish(cmn.NewAbortedError(r.String()))
			return
		}
	}
}

// main method: replicate a given locally stored object
func (r *XactPut) Repl(lom *cluster.LOM) (err error) {
	if r.Finished() {
		err = xaction.NewErrXactExpired("Cannot replicate: " + r.String())
		return
	}
	r.total.Inc()

	// [throttle]
	// when the optimization objective is write perf,
	// we start dropping requests to make sure callers don't block
	pending, max := int(r.Pending()), r.mirror.Burst
	if r.mirror.OptimizePUT {
		if pending > 1 && pending >= max {
			r.dropped++
			if (r.dropped % logNumProcessed) == 0 {
				glog.Errorf("%s: pending=%d, total=%d, dropped=%d", r, pending, r.total.Load(), r.dropped)
			}
			return
		}
	}
	r.IncPending() // ref-count via base to support on-demand action
	r.workCh <- lom

	// [throttle]
	// a bit of back-pressure when approaching the fixed boundary
	if pending > 1 && max > 10 {
		// increase the chances for the burst of PUTs to subside
		// but only to a point
		if pending > max/2 && pending < max-max/8 {
			time.Sleep(cmn.ThrottleAvg)
		}
	}
	return
}

func (r *XactPut) stop() (err error) {
	r.XactDemandBase.Stop()
	n := r.workers.Stop()
	if nn := drainWorkCh(r.workCh); nn > 0 {
		n += nn
	}
	if n > 0 {
		r.SubPending(n)
		err = fmt.Errorf("%s: dropped %d object(s)", r, n)
	}
	return err
}

func (r *XactPut) Stats() cluster.XactStats {
	baseStats := r.XactDemandBase.Stats().(*xaction.BaseXactStatsExt)
	baseStats.Ext = &xaction.BaseXactDemandStatsExt{IsIdle: r.IsIdle()}
	return baseStats
}
