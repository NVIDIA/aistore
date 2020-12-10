// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
* Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"io"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	// Implements `xreg.BucketEntry` and `xreg.BucketEntryProvider` interface.
	xactGetProvider struct {
		xreg.BaseBckEntry
		xact *XactGet
	}

	// Erasure coding runner: accepts requests and dispatches them to
	// a correct mountpath runner. Runner uses dedicated to EC memory manager
	// inherited by dependent mountpath runners
	XactGet struct {
		xactECBase
		xactReqBase
		getJoggers map[string]*getJogger // mountpath joggers for GET
	}

	bgProcess = func(req *Request, toDisk bool, cb func(error))
)

// interface guard
var _ xaction.XactDemand = (*XactGet)(nil)

func (*xactGetProvider) New(_ xreg.XactArgs) xreg.BucketEntry { return &xactGetProvider{} }
func (p *xactGetProvider) Start(bck cmn.Bck) error {
	var (
		xec      = ECM.NewGetXact(bck)
		idleTime = cmn.GCO.Get().Timeout.SendFile
	)
	xec.XactDemandBase = *xaction.NewXactDemandBaseBck(p.Kind(), bck, idleTime)
	xec.InitIdle()
	p.xact = xec
	go xec.Run()
	return nil
}
func (*xactGetProvider) Kind() string        { return cmn.ActECGet }
func (p *xactGetProvider) Get() cluster.Xact { return p.xact }

//
// XactGet
//

func NewGetXact(t cluster.Target, bck cmn.Bck, mgr *Manager) *XactGet {
	availablePaths, disabledPaths := fs.Get()
	totalPaths := len(availablePaths) + len(disabledPaths)
	smap, si := t.Sowner(), t.Snode()

	runner := &XactGet{
		getJoggers:  make(map[string]*getJogger, totalPaths),
		xactECBase:  newXactECBase(t, smap, si, bck, mgr),
		xactReqBase: newXactReqECBase(),
	}

	// create all runners but do not start them until Run is called
	for mpath := range availablePaths {
		getJog := runner.newGetJogger(mpath)
		runner.getJoggers[mpath] = getJog
	}
	for mpath := range disabledPaths {
		getJog := runner.newGetJogger(mpath)
		runner.getJoggers[mpath] = getJog
	}

	return runner
}

func (r *XactGet) DispatchResp(iReq intraReq, bck *cluster.Bck, objName string, objAttrs transport.ObjectAttrs,
	object io.Reader) {
	uname := unique(iReq.sender, bck, objName)

	switch iReq.act {
	// It is response to slice/replica request by an object
	// restoration process. In this case there should exists
	// a slice waiting for the data to come(registered with `regWriter`.
	// Read the data into the slice writer and notify the slice when
	// the transfer is completed
	case respPut:
		if glog.V(4) {
			glog.Infof("Response from %s, %s", iReq.sender, uname)
		}
		r.dOwner.mtx.Lock()
		writer, ok := r.dOwner.slices[uname]
		r.dOwner.mtx.Unlock()

		if !ok {
			glog.Errorf("No writer for %s/%s", bck.Name, objName)
			return
		}

		if err := r.writerReceive(writer, iReq.exists, objAttrs, object); err != nil {
			glog.Errorf("Failed to read replica: %v", err)
		}
	default:
		// should be unreachable
		glog.Errorf("Invalid request: %d", iReq.act)
	}
}

func (r *XactGet) newGetJogger(mpath string) *getJogger {
	config := cmn.GCO.Get()
	client := cmn.NewClient(cmn.TransportArgs{
		Timeout:    config.Client.Timeout,
		UseHTTPS:   config.Net.HTTP.UseHTTPS,
		SkipVerify: config.Net.HTTP.SkipVerify,
	})
	return &getJogger{
		parent: r,
		mpath:  mpath,
		client: client,
		workCh: make(chan *Request, requestBufSizeFS),
		stopCh: make(chan struct{}, 1),
		jobs:   make(map[uint64]bgProcess, 4),
		sema:   make(chan struct{}, maxBgJobsPerJogger),
	}
}

func (r *XactGet) Run() (err error) {
	glog.Infoln(r.String())

	for _, jog := range r.getJoggers {
		go jog.run()
	}

	var (
		cfg    = cmn.GCO.Get()
		ticker = time.NewTicker(cfg.Periodic.StatsTime)
	)
	defer ticker.Stop()

	// as of now all requests are equal. Some may get throttling later
	for {
		select {
		case <-ticker.C:
			if glog.FastV(4, glog.SmoduleEC) {
				if s := fmt.Sprintf("%v", r.ECStats()); s != "" {
					glog.Info(s)
				}
			}
		case req := <-r.ecCh:
			if req.Action != ActRestore {
				glog.Errorf("Invalid request's action %s for getxaction", req.Action)
				break
			}

			r.stats.updateDecode()
			r.dispatchRequest(req)
		case mpathRequest := <-r.mpathReqCh:
			switch mpathRequest.action {
			case cmn.ActMountpathAdd:
				r.addMpath(mpathRequest.mpath)
			case cmn.ActMountpathRemove:
				r.removeMpath(mpathRequest.mpath)
			}
		case <-r.IdleTimer():
			// It's OK not to notify ecmanager, it'll just have stopped xact in a map.
			r.stop()
			return nil
		case msg := <-r.controlCh:
			if msg.Action == ActEnableRequests {
				r.setEcRequestsEnabled()
				break
			}
			cmn.Assert(msg.Action == ActClearRequests)

			r.setEcRequestsDisabled()

			// Drain pending bucket's EC requests, return them with an error.
			// NOTE: loop can't be replaced with channel range, as the channel is never closed.
			for {
				select {
				case req := <-r.ecCh:
					r.abortECRequestWhenDisabled(req)
				default:
					r.stop()
					return nil
				}
			}
		case <-r.ChanAbort():
			r.stop()
			return cmn.NewAbortedError(r.String())
		}
	}
}

func (r *XactGet) abortECRequestWhenDisabled(req *Request) {
	if req.ErrCh != nil {
		req.ErrCh <- fmt.Errorf("EC disabled, can't procced with the request on bucket %s", r.bck)
		close(req.ErrCh)
	}
}

func (r *XactGet) Stop(error) { r.Abort() }

func (r *XactGet) stop() {
	r.XactDemandBase.Stop()
	for _, jog := range r.getJoggers {
		jog.stop()
	}

	// Don't close bundles, they are shared between bucket's EC actions

	r.Finish()
}

// Decode schedules an object to be restored from existing slices.
// A caller should wait for the main object restoration is completed. When
// ecrunner finishes main object restoration process it puts into request.ErrCh
// channel the error or nil. The caller may read the object after receiving
// a nil value from channel but ecrunner keeps working - it reuploads all missing
// slices or copies
func (r *XactGet) Decode(req *Request) {
	req.putTime = time.Now()
	req.tm = time.Now()

	r.dispatchEncodingRequest(req)
}

// Cleanup deletes all object slices or copies after the main object is removed
func (r *XactGet) Cleanup(req *Request) {
	req.putTime = time.Now()
	req.tm = time.Now()

	r.dispatchEncodingRequest(req)
}

// ClearRequests disables receiving new EC requests, they will be terminated with error
// Then it starts draining a channel from pending EC requests
// It does not enable receiving new EC requests, it has to be done explicitly, when EC is enabled again
func (r *XactGet) ClearRequests() {
	msg := RequestsControlMsg{
		Action: ActClearRequests,
	}

	r.controlCh <- msg
}

func (r *XactGet) EnableRequests() {
	msg := RequestsControlMsg{
		Action: ActEnableRequests,
	}

	r.controlCh <- msg
}

func (r *XactGet) dispatchEncodingRequest(req *Request) {
	if !r.ecRequestsEnabled() {
		r.abortECRequestWhenDisabled(req)
		return
	}

	r.ecCh <- req
}

func (r *XactGet) dispatchRequest(req *Request) {
	r.IncPending()
	if !r.ecRequestsEnabled() {
		if req.ErrCh != nil {
			req.ErrCh <- fmt.Errorf("EC on bucket %s is being disabled, no EC requests accepted", r.bck)
			close(req.ErrCh)
		}
		r.DecPending()
		return
	}

	cmn.Assert(req.Action == ActRestore)

	jogger, ok := r.getJoggers[req.LOM.MpathInfo().Path]
	cmn.AssertMsg(ok, "Invalid mountpath given in EC request")
	r.stats.updateQueue(len(jogger.workCh))
	jogger.workCh <- req
}

//
// fsprunner methods
//
func (r *XactGet) addMpath(mpath string) {
	jogger, ok := r.getJoggers[mpath]
	if ok && jogger != nil {
		glog.Warningf("Attempted to add already existing mountpath: %s", mpath)
		return
	}
	getJog := r.newGetJogger(mpath)
	r.getJoggers[mpath] = getJog
	go getJog.run()
}

func (r *XactGet) removeMpath(mpath string) {
	getJog, ok := r.getJoggers[mpath]
	cmn.AssertMsg(ok, "Mountpath unregister handler for EC called with invalid mountpath")
	getJog.stop()
	delete(r.getJoggers, mpath)
}

type GetTargetStats struct {
	xaction.BaseXactStats
	Ext ExtECGetStats `json:"ext"`
}

type ExtECGetStats struct {
	AvgTime     cmn.DurationJSON `json:"ec.decode.time"`
	ErrCount    int64            `json:"ec.decode.err.n,string"`
	AvgObjTime  cmn.DurationJSON `json:"ec.obj.process.time"`
	AvgQueueLen float64          `json:"ec.queue.len.n"`
}

// interface guard
var _ cluster.XactStats = (*GetTargetStats)(nil)

func (r *XactGet) Stats() cluster.XactStats {
	baseStats := r.XactBase.Stats().(*xaction.BaseXactStats)
	getStats := GetTargetStats{BaseXactStats: *baseStats}
	st := r.stats.stats()
	getStats.Ext.AvgTime = cmn.DurationJSON(st.DecodeTime.Nanoseconds())
	getStats.Ext.ErrCount = st.DecodeErr
	getStats.ObjCountX = st.GetReq
	getStats.Ext.AvgObjTime = cmn.DurationJSON(st.ObjTime.Nanoseconds())
	getStats.Ext.AvgQueueLen = st.QueueLen
	return &getStats
}
