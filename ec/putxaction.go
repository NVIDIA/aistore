// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
* Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	putFactory struct {
		xreg.RenewBase
		xctn *XactPut
	}
	// Erasure coding runner: accepts requests and dispatches them to
	// a correct mountpath runner. Runner uses dedicated to EC memory manager
	// inherited by dependent mountpath runners
	XactPut struct {
		xactECBase
		xactReqBase
		putJoggers map[string]*putJogger // mountpath joggers for PUT/DEL
	}
	// extended x-ec-put statistics
	ExtECPutStats struct {
		AvgEncodeTime  cos.Duration `json:"ec.encode.ns"`
		AvgDeleteTime  cos.Duration `json:"ec.delete.ns"`
		EncodeCount    int64        `json:"ec.encode.n,string"`
		DeleteCount    int64        `json:"ec.delete.n,string"`
		EncodeSize     int64        `json:"ec.encode.size,string"`
		EncodeErrCount int64        `json:"ec.encode.err.n,string"`
		DeleteErrCount int64        `json:"ec.delete.err.n,string"`
		AvgObjTime     cos.Duration `json:"ec.obj.process.ns"`
		AvgQueueLen    float64      `json:"ec.queue.len.f"`
		IsIdle         bool         `json:"is_idle"`
	}
)

// interface guard
var (
	_ xact.Demand    = (*XactPut)(nil)
	_ xreg.Renewable = (*putFactory)(nil)
)

////////////////
// putFactory //
////////////////

func (*putFactory) New(_ xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &putFactory{RenewBase: xreg.RenewBase{Bck: bck}}
	return p
}

func (p *putFactory) Start() error {
	xec := ECM.NewPutXact(p.Bck.Bucket())
	xec.DemandBase.Init(cos.GenUUID(), p.Kind(), p.Bck, 0 /*use default*/)
	p.xctn = xec
	go xec.Run(nil)
	return nil
}

func (*putFactory) Kind() string        { return apc.ActECPut }
func (p *putFactory) Get() cluster.Xact { return p.xctn }

func (p *putFactory) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

/////////////
// XactPut //
/////////////

func NewPutXact(t cluster.Target, bck *cmn.Bck, mgr *Manager) *XactPut {
	availablePaths, disabledPaths := fs.Get()
	totalPaths := len(availablePaths) + len(disabledPaths)
	smap, si := t.Sowner(), t.Snode()
	runner := &XactPut{
		putJoggers:  make(map[string]*putJogger, totalPaths),
		xactECBase:  newXactECBase(t, smap, si, bck, mgr),
		xactReqBase: newXactReqECBase(),
	}

	// create all runners but do not start them until Run is called
	for mpath := range availablePaths {
		putJog := runner.newPutJogger(mpath)
		runner.putJoggers[mpath] = putJog
	}
	for mpath := range disabledPaths {
		putJog := runner.newPutJogger(mpath)
		runner.putJoggers[mpath] = putJog
	}

	return runner
}

func (r *XactPut) newPutJogger(mpath string) *putJogger {
	j := &putJogger{
		parent: r,
		mpath:  mpath,
		putCh:  make(chan *request, requestBufSizeFS),
		xactCh: make(chan *request, requestBufSizeEncode),
	}
	j.stopCh.Init()
	return j
}

func (r *XactPut) dispatchRequest(req *request, lom *cluster.LOM) error {
	debug.Assert(req.Action == ActDelete || req.Action == ActSplit, req.Action)
	debug.Assert(req.ErrCh == nil, "ec-put does not support ErrCh")
	if !r.ecRequestsEnabled() {
		return ErrorECDisabled
	}
	switch req.Action {
	case ActSplit:
		r.stats.updateEncode(lom.SizeBytes())
	case ActDelete:
		r.stats.updateDelete()
	default:
		return fmt.Errorf("invalid request's action %s for putxaction", req.Action)
	}

	jogger, ok := r.putJoggers[lom.Mountpath().Path]
	if !ok {
		debug.Assert(false, "invalid "+lom.Mountpath().String())
	}
	if glog.FastV(4, glog.SmoduleEC) {
		glog.Infof("ECPUT (bg queue = %d): dispatching object %s....", len(jogger.putCh), lom)
	}
	if req.rebuild {
		jogger.xactCh <- req
	} else {
		r.stats.updateQueue(len(jogger.putCh))
		jogger.putCh <- req
	}
	return nil
}

func (r *XactPut) Run(*sync.WaitGroup) {
	glog.Infoln(r.Name())

	var wg sync.WaitGroup
	for _, jog := range r.putJoggers {
		wg.Add(1)
		go jog.run(&wg)
	}

	r.mainLoop()
	wg.Wait()
	// Don't close bundles, they are shared between different EC xactions
	r.Finish(nil)
}

func (r *XactPut) mainLoop() {
	var (
		cfg    = cmn.GCO.Get()
		ticker = time.NewTicker(cfg.Periodic.StatsTime.D())
	)
	defer ticker.Stop()

	// as of now all requests are equal. Some may get throttling later
	for {
		select {
		case <-ticker.C:
			if glog.FastV(4, glog.SmoduleEC) {
				if s := fmt.Sprintf("%v", r.Snap()); s != "" {
					glog.Info(s)
				}
			}
		case <-r.IdleTimer():
			// It's OK not to notify ecmanager, it'll just have stopped xctn in a map.
			r.stop()
			return
		case msg := <-r.controlCh:
			if msg.Action == ActEnableRequests {
				r.setEcRequestsEnabled()
				break
			}
			debug.Assert(msg.Action == ActClearRequests, msg.Action)

			r.setEcRequestsDisabled()
			r.stop()
			return
		case <-r.ChanAbort():
			r.stop()
			return
		}
	}
}

func (r *XactPut) Stop(err error) { r.Abort(err) }

func (r *XactPut) stop() {
	r.DemandBase.Stop()
	for _, jog := range r.putJoggers {
		jog.stop()
	}

	// Don't close bundles, they are shared between bucket's EC actions
	r.Finish(nil)
}

// Encode schedules FQN for erasure coding process
func (r *XactPut) encode(req *request, lom *cluster.LOM) {
	req.putTime = time.Now()
	req.tm = time.Now()
	if err := r.dispatchRequest(req, lom); err != nil {
		glog.Errorf("Failed to encode %s: %v", lom, err)
		freeReq(req)
	}
}

// Cleanup deletes all object slices or copies after the main object is removed
func (r *XactPut) cleanup(req *request, lom *cluster.LOM) {
	req.putTime = time.Now()
	req.tm = time.Now()

	if err := r.dispatchRequest(req, lom); err != nil {
		glog.Errorf("Failed to cleanup %s: %v", lom, err)
		freeReq(req)
	}
}

func (r *XactPut) Snap() (snap *cluster.Snap) {
	snap = r.baseSnap()
	st := r.stats.stats()
	snap.Ext = &ExtECPutStats{
		AvgEncodeTime:  cos.Duration(st.EncodeTime),
		EncodeSize:     st.EncodeSize,
		EncodeCount:    st.PutReq,
		EncodeErrCount: st.EncodeErr,
		AvgDeleteTime:  cos.Duration(st.DeleteTime),
		DeleteErrCount: st.DeleteErr,
		DeleteCount:    st.DelReq,
		AvgObjTime:     cos.Duration(st.ObjTime),
		AvgQueueLen:    st.QueueLen,
		IsIdle:         r.Pending() == 0,
	}

	snap.Stats.Objs = st.PutReq + st.DelReq // TODO: support in and out
	snap.Stats.Bytes = st.EncodeSize
	return
}
