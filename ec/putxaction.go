// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
* Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	// Implements `xreg.BucketEntryProvider` and `xreg.BucketEntry` interface.
	xactPutProvider struct {
		xreg.BaseBckEntry
		xact *XactPut
	}

	// Erasure coding runner: accepts requests and dispatches them to
	// a correct mountpath runner. Runner uses dedicated to EC memory manager
	// inherited by dependent mountpath runners
	XactPut struct {
		xactECBase
		xactReqBase
		putJoggers map[string]*putJogger // mountpath joggers for PUT/DEL
	}
)

// interface guard
var _ xaction.XactDemand = (*XactPut)(nil)

func (*xactPutProvider) New(_ xreg.XactArgs) xreg.BucketEntry { return &xactPutProvider{} }
func (p *xactPutProvider) Start(bck cmn.Bck) error {
	var (
		xec      = ECM.NewPutXact(bck)
		idleTime = cmn.GCO.Get().Timeout.SendFile
	)
	xec.XactDemandBase = *xaction.NewXactDemandBaseBck(p.Kind(), bck, idleTime)
	xec.InitIdle()
	p.xact = xec
	go xec.Run()
	return nil
}
func (*xactPutProvider) Kind() string        { return cmn.ActECPut }
func (p *xactPutProvider) Get() cluster.Xact { return p.xact }

//
// XactPut
//

func NewPutXact(t cluster.Target, bck cmn.Bck, mgr *Manager) *XactPut {
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
	return &putJogger{
		parent: r,
		mpath:  mpath,
		putCh:  make(chan *Request, requestBufSizeFS),
		xactCh: make(chan *Request, requestBufSizeEncode),
		stopCh: make(chan struct{}, 1),
	}
}

func (r *XactPut) Do(req *Request, lom *cluster.LOM) error {
	switch req.Action {
	case ActSplit:
		r.stats.updateEncode(lom.Size())
	case ActDelete:
		r.stats.updateDelete()
	default:
		return fmt.Errorf("invalid request's action %s for putxaction", req.Action)
	}
	return r.dispatchReqest(req, lom)
}

func (r *XactPut) dispatchReqest(req *Request, lom *cluster.LOM) error {
	r.IncPending()
	if !r.ecRequestsEnabled() {
		r.DecPending()
		err := fmt.Errorf("EC on bucket %s is being disabled, no EC requests accepted", r.bck)
		if req.ErrCh != nil {
			req.ErrCh <- err
			close(req.ErrCh)
		}
		return err
	}

	debug.Assert(req.Action == ActDelete || req.Action == ActSplit)

	jogger, ok := r.putJoggers[lom.MpathInfo().Path]
	cmn.AssertMsg(ok, "Invalid mountpath given in EC request")
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

func (r *XactPut) Run() {
	glog.Infoln(r.String())

	for _, jog := range r.putJoggers {
		go jog.run()
	}

	var (
		cfg    = cmn.GCO.Get()
		ticker = time.NewTicker(cfg.Periodic.StatsTime)
	)
	defer ticker.Stop()

	// as of now all requests are equal. Some may get throttling later
	for {
		// Favor aborting the process, otherwise because of random choice
		// `select` can choose a task from `r.ecCh` while `abort` awaits
		// for execution.
		// NOTE: next select should include the same `case`, too. Because
		// it does not have `default` branch and may stuck
		select {
		case <-r.ChanAbort():
			r.stop()
			return
		default:
		}

		select {
		case <-ticker.C:
			if glog.FastV(4, glog.SmoduleEC) {
				if s := fmt.Sprintf("%v", r.Stats()); s != "" {
					glog.Info(s)
				}
			}
		case <-r.IdleTimer():
			// It's OK not to notify ecmanager, it'll just have stopped xact in a map.
			r.stop()
			return
		case msg := <-r.controlCh:
			if msg.Action == ActEnableRequests {
				r.setEcRequestsEnabled()
				break
			}
			cmn.Assert(msg.Action == ActClearRequests)

			r.setEcRequestsDisabled()
			r.stop()
			return
		case <-r.ChanAbort():
			r.stop()
			return
		}
	}
}

func (r *XactPut) abortECRequestWhenDisabled(req *Request) {
	if req.ErrCh != nil {
		req.ErrCh <- fmt.Errorf("EC disabled, can't procced with the request on bucket %s", r.bck)
		close(req.ErrCh)
	}
}

func (r *XactPut) Stop(error) { r.Abort() }

func (r *XactPut) stop() {
	r.XactDemandBase.Stop()
	for _, jog := range r.putJoggers {
		jog.stop()
	}

	// Don't close bundles, they are shared between different EC xactions

	r.Finish(nil)
}

// Encode schedules FQN for erasure coding process
func (r *XactPut) Encode(req *Request, lom *cluster.LOM) {
	req.putTime = time.Now()
	req.tm = time.Now()
	if err := r.Do(req, lom); err != nil {
		glog.Errorf("Failed to encode %s: %v", lom, err)
	}
}

// Cleanup deletes all object slices or copies after the main object is removed
func (r *XactPut) Cleanup(req *Request, lom *cluster.LOM) {
	req.putTime = time.Now()
	req.tm = time.Now()

	r.dispatchDecodingRequest(req, lom)
}

func (r *XactPut) dispatchDecodingRequest(req *Request, lom *cluster.LOM) {
	if !r.ecRequestsEnabled() {
		r.abortECRequestWhenDisabled(req)
		return
	}

	r.dispatchReqest(req, lom)
}

type ExtECPutStats struct {
	AvgEncodeTime  cmn.DurationJSON `json:"ec.encode.time"`
	AvgDeleteTime  cmn.DurationJSON `json:"ec.delete.time"`
	EncodeCount    int64            `json:"ec.encode.n,string"`
	DeleteCount    int64            `json:"ec.delete.n,string"`
	EncodeSize     cmn.SizeJSON     `json:"ec.encode.size,string"`
	EncodeErrCount int64            `json:"ec.encode.err.n,string"`
	DeleteErrCount int64            `json:"ec.delete.err.n,string"`
	AvgObjTime     cmn.DurationJSON `json:"ec.obj.process.time"`
	AvgQueueLen    float64          `json:"ec.queue.len.n"`
	IsIdle         bool             `json:"is_idle"`
}

func (r *XactPut) Stats() cluster.XactStats {
	baseStats := r.XactDemandBase.Stats().(*xaction.BaseXactStatsExt)
	st := r.stats.stats()
	baseStats.Ext = &ExtECPutStats{
		AvgEncodeTime:  cmn.DurationJSON(st.EncodeTime.Nanoseconds()),
		EncodeSize:     cmn.SizeJSON(st.EncodeSize),
		EncodeCount:    st.PutReq,
		EncodeErrCount: st.EncodeErr,
		AvgDeleteTime:  cmn.DurationJSON(st.DeleteTime.Nanoseconds()),
		DeleteErrCount: st.DeleteErr,
		DeleteCount:    st.DelReq,
		AvgObjTime:     cmn.DurationJSON(st.ObjTime.Nanoseconds()),
		AvgQueueLen:    st.QueueLen,
		IsIdle:         r.IsIdle(),
	}

	baseStats.ObjCountX = st.PutReq + st.DelReq
	baseStats.BytesCountX = st.EncodeSize
	return baseStats
}
