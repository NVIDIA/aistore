// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
* Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"io"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	getFactory struct {
		xreg.RenewBase
		xctn *XactGet
	}

	// Erasure coding runner: accepts requests and dispatches them to
	// a correct mountpath runner. Runner uses dedicated to EC memory manager
	// inherited by dependent mountpath runners
	XactGet struct {
		xactECBase
		xactReqBase
		getJoggers map[string]*getJogger // mountpath joggers for GET
	}

	// extended x-ec-get statistics
	ExtECGetStats struct {
		AvgTime     cos.Duration `json:"ec.decode.ns"`
		ErrCount    int64        `json:"ec.decode.err.n,string"`
		AvgObjTime  cos.Duration `json:"ec.obj.process.ns"`
		AvgQueueLen float64      `json:"ec.queue.len.f"`
		IsIdle      bool         `json:"is_idle"`
	}
)

// interface guard
var (
	_ xact.Demand    = (*XactGet)(nil)
	_ xreg.Renewable = (*getFactory)(nil)
)

////////////////
// getFactory //
////////////////

func (*getFactory) New(_ xreg.Args, bck *cluster.Bck) xreg.Renewable {
	p := &getFactory{RenewBase: xreg.RenewBase{Bck: bck}}
	return p
}

func (p *getFactory) Start() error {
	xec := ECM.NewGetXact(p.Bck.Bucket())
	xec.DemandBase.Init(cos.GenUUID(), p.Kind(), p.Bck, 0 /*use default*/)
	p.xctn = xec
	go xec.Run(nil)
	return nil
}
func (*getFactory) Kind() string        { return apc.ActECGet }
func (p *getFactory) Get() cluster.Xact { return p.xctn }

func (p *getFactory) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

/////////////
// XactGet //
/////////////

func NewGetXact(t cluster.Target, bck *cmn.Bck, mgr *Manager) *XactGet {
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

func (r *XactGet) DispatchResp(iReq intraReq, hdr *transport.ObjHdr, bck *cluster.Bck, reader io.Reader) {
	objName, objAttrs := hdr.ObjName, hdr.ObjAttrs
	uname := unique(hdr.SID, bck, objName)
	switch hdr.Opcode {
	// It is response to slice/replica request by an object
	// restoration process. In this case there should exists
	// a slice waiting for the data to come(registered with `regWriter`.
	// Read the data into the slice writer and notify the slice when
	// the transfer is completed
	case respPut:
		if glog.V(4) {
			glog.Infof("Response from %s, %s", hdr.SID, uname)
		}
		r.dOwner.mtx.Lock()
		writer, ok := r.dOwner.slices[uname]
		r.dOwner.mtx.Unlock()

		if !ok {
			glog.Errorf("No writer for %s/%s", bck.Name, objName)
			return
		}

		if err := _writerReceive(writer, iReq.exists, objAttrs, reader); err != nil {
			glog.Errorf("Failed to read replica: %v", err)
		}
	default:
		// should be unreachable
		glog.Errorf("Invalid request: %d", hdr.Opcode)
	}
}

func (r *XactGet) newGetJogger(mpath string) *getJogger {
	config := cmn.GCO.Get()
	client := cmn.NewClient(cmn.TransportArgs{
		Timeout:    config.Client.Timeout.D(),
		UseHTTPS:   config.Net.HTTP.UseHTTPS,
		SkipVerify: config.Net.HTTP.SkipVerify,
	})
	j := &getJogger{
		parent: r,
		mpath:  mpath,
		client: client,
		workCh: make(chan *request, requestBufSizeFS),
	}
	j.stopCh.Init()
	return j
}

func (r *XactGet) dispatchRequest(req *request, lom *cluster.LOM) error {
	if !r.ecRequestsEnabled() {
		if req.ErrCh != nil {
			req.ErrCh <- ErrorECDisabled
			close(req.ErrCh)
		}
		return ErrorECDisabled
	}

	debug.Assert(req.Action == ActRestore)

	jogger, ok := r.getJoggers[lom.MpathInfo().Path]
	if !ok {
		cos.AssertMsg(false, "invalid "+lom.MpathInfo().String())
	}
	r.stats.updateQueue(len(jogger.workCh))
	jogger.workCh <- req
	return nil
}

func (r *XactGet) Run(*sync.WaitGroup) {
	glog.Infoln(r.Name())

	for _, jog := range r.getJoggers {
		go jog.run()
	}

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
				if s := r.ECStats().String(); s != "" {
					glog.Info(s)
				}
			}
		case mpathRequest := <-r.mpathReqCh:
			switch mpathRequest.action {
			case apc.ActMountpathAttach:
				r.addMpath(mpathRequest.mpath)
			case apc.ActMountpathDetach:
				r.removeMpath(mpathRequest.mpath)
			}
		case <-r.IdleTimer():
			// It's OK not to notify ecmanager, it'll just have stopped xctn in a map.
			r.stop(nil)
			return
		case msg := <-r.controlCh:
			if msg.Action == ActEnableRequests {
				r.setEcRequestsEnabled()
				break
			}
			debug.Assert(msg.Action == ActClearRequests)

			r.setEcRequestsDisabled()
			r.stop(nil)
			return
		case errCause := <-r.ChanAbort():
			r.stop(errCause)
			return
		}
	}
}

func (r *XactGet) Stop(err error) { r.Abort(err) }

func (r *XactGet) stop(err error) {
	r.DemandBase.Stop()
	for _, jog := range r.getJoggers {
		jog.stop()
	}

	// Don't close bundles, they are shared between bucket's EC actions
	r.Finish(err)
}

// Decode schedules an object to be restored from existing slices.
// A caller should wait for the main object restoration is completed. When
// ecrunner finishes main object restoration process it puts into request.ErrCh
// channel the error or nil. The caller may read the object after receiving
// a nil value from channel but ecrunner keeps working - it reuploads all missing
// slices or copies
func (r *XactGet) decode(req *request, lom *cluster.LOM) {
	debug.Assert(req.Action == ActRestore, "invalid action for restore: "+req.Action)
	r.stats.updateDecode()
	req.putTime = time.Now()
	req.tm = time.Now()

	if err := r.dispatchRequest(req, lom); err != nil {
		glog.Errorf("Failed to restore %s: %v", lom, err)
		freeReq(req)
	}
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
	if !ok {
		cos.AssertMsg(false, "invalid mountpath: "+mpath)
	}
	getJog.stop()
	delete(r.getJoggers, mpath)
}

func (r *XactGet) Snap() (snap *cluster.Snap) {
	snap = r.baseSnap()
	st := r.stats.stats()
	snap.Ext = &ExtECGetStats{
		AvgTime:     cos.Duration(st.DecodeTime),
		ErrCount:    st.DecodeErr,
		AvgObjTime:  cos.Duration(st.ObjTime),
		AvgQueueLen: st.QueueLen,
		IsIdle:      r.Pending() == 0,
	}
	snap.Stats.Objs = st.GetReq
	return
}
