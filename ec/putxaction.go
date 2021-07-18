// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
* Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	putFactory struct {
		xreg.BaseEntry
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
	// Runtime EC statistics for PUT xaction
	ExtECPutStats struct {
		AvgEncodeTime  cos.Duration `json:"ec.encode.time"`
		AvgDeleteTime  cos.Duration `json:"ec.delete.time"`
		EncodeCount    int64        `json:"ec.encode.n,string"`
		DeleteCount    int64        `json:"ec.delete.n,string"`
		EncodeSize     int64        `json:"ec.encode.size,string"`
		EncodeErrCount int64        `json:"ec.encode.err.n,string"`
		DeleteErrCount int64        `json:"ec.delete.err.n,string"`
		AvgObjTime     cos.Duration `json:"ec.obj.process.time"`
		AvgQueueLen    float64      `json:"ec.queue.len.n"`
		IsIdle         bool         `json:"is_idle"`
	}
)

// interface guard
var (
	_ xaction.XactDemand = (*XactPut)(nil)
	_ xreg.Factory       = (*putFactory)(nil)
)

////////////////
// putFactory //
////////////////

func (*putFactory) New(xreg.Args) xreg.Renewable { return &putFactory{} }

func (p *putFactory) Start(bck cmn.Bck) error {
	var (
		xec         = ECM.NewPutXact(bck)
		config      = cmn.GCO.Get()
		totallyIdle = config.Timeout.SendFile.D()
		likelyIdle  = config.Timeout.MaxKeepalive.D()
	)
	xec.XactDemandBase = *xaction.NewXDB(cos.GenUUID(), p.Kind(), &bck, totallyIdle, likelyIdle)
	xec.InitIdle()
	p.xact = xec
	go xec.Run()
	return nil
}

func (*putFactory) Kind() string        { return cmn.ActECPut }
func (p *putFactory) Get() cluster.Xact { return p.xact }

/////////////
// XactPut //
/////////////

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
		putCh:  make(chan *request, requestBufSizeFS),
		xactCh: make(chan *request, requestBufSizeEncode),
		stopCh: cos.NewStopCh(),
	}
}

func (r *XactPut) dispatchRequest(req *request, lom *cluster.LOM) error {
	debug.AssertMsg(req.Action == ActDelete || req.Action == ActSplit, req.Action)
	debug.AssertMsg(req.ErrCh == nil, "ecput does not support ErrCh")
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

	jogger, ok := r.putJoggers[lom.MpathInfo().Path]
	cos.AssertMsg(ok, "Invalid mountpath given in EC request")
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
			cos.Assert(msg.Action == ActClearRequests)

			r.setEcRequestsDisabled()
			r.stop()
			return
		case <-r.ChanAbort():
			r.stop()
			return
		}
	}
}

func (r *XactPut) Stop(error) { r.Abort() }

func (r *XactPut) stop() {
	r.XactDemandBase.Stop()
	for _, jog := range r.putJoggers {
		jog.stop()
	}
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

func (r *XactPut) Stats() cluster.XactStats {
	baseStats := r.XactDemandBase.Stats().(*xaction.BaseXactStatsExt)
	st := r.stats.stats()
	baseStats.Ext = &ExtECPutStats{
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

	baseStats.ObjCountX = st.PutReq + st.DelReq
	baseStats.BytesCountX = st.EncodeSize
	return baseStats
}
