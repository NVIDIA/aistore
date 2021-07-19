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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	// Implements `xreg.Renewable` and `xreg.Factory` interface.
	getFactory struct {
		xreg.BaseEntry
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

	// Runtime EC statistics for restore xaction
	ExtECGetStats struct {
		AvgTime     cos.Duration `json:"ec.decode.time"`
		ErrCount    int64        `json:"ec.decode.err.n,string"`
		AvgObjTime  cos.Duration `json:"ec.obj.process.time"`
		AvgQueueLen float64      `json:"ec.queue.len.n"`
		IsIdle      bool         `json:"is_idle"`
	}
)

// interface guard
var (
	_ xaction.Demand = (*XactGet)(nil)
	_ xreg.Factory   = (*getFactory)(nil)
)

////////////////
// getFactory //
////////////////

func (*getFactory) New(_ xreg.Args, bck *cluster.Bck) xreg.Renewable {
	p := &getFactory{}
	p.Bck = bck
	return p
}

func (p *getFactory) Start() error {
	var (
		xec         = ECM.NewGetXact(p.Bck.Bck)
		config      = cmn.GCO.Get()
		totallyIdle = config.Timeout.SendFile.D()
		likelyIdle  = config.Timeout.MaxKeepalive.D()
	)
	xec.DemandBase = *xaction.NewXDB(cos.GenUUID(), p.Kind(), &p.Bck.Bck, totallyIdle, likelyIdle)
	xec.InitIdle()
	p.xact = xec
	go xec.Run()
	return nil
}
func (*getFactory) Kind() string        { return cmn.ActECGet }
func (p *getFactory) Get() cluster.Xact { return p.xact }

/////////////
// XactGet //
/////////////

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

func (r *XactGet) DispatchResp(iReq intraReq, hdr *transport.ObjHdr, bck *cluster.Bck, reader io.Reader) {
	objName, objAttrs := hdr.ObjName, hdr.ObjAttrs
	uname := unique(hdr.SID, bck, objName)
	switch iReq.act {
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
		glog.Errorf("Invalid request: %d", iReq.act)
	}
}

func (r *XactGet) newGetJogger(mpath string) *getJogger {
	config := cmn.GCO.Get()
	client := cmn.NewClient(cmn.TransportArgs{
		Timeout:    config.Client.Timeout.D(),
		UseHTTPS:   config.Net.HTTP.UseHTTPS,
		SkipVerify: config.Net.HTTP.SkipVerify,
	})
	return &getJogger{
		parent: r,
		mpath:  mpath,
		client: client,
		workCh: make(chan *request, requestBufSizeFS),
		stopCh: cos.NewStopCh(),
	}
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
	cos.AssertMsg(ok, "Invalid mountpath given in EC request")
	r.stats.updateQueue(len(jogger.workCh))
	jogger.workCh <- req
	return nil
}

func (r *XactGet) Run() {
	glog.Infoln(r.String())

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
				if s := fmt.Sprintf("%v", r.ECStats()); s != "" {
					glog.Info(s)
				}
			}
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
			return
		case msg := <-r.controlCh:
			if msg.Action == ActEnableRequests {
				r.setEcRequestsEnabled()
				break
			}
			debug.Assert(msg.Action == ActClearRequests)

			r.setEcRequestsDisabled()
			r.stop()
			return
		case <-r.ChanAbort():
			r.stop()
			return
		}
	}
}

func (r *XactGet) Stop(error) { r.Abort() }

func (r *XactGet) stop() {
	r.DemandBase.Stop()
	for _, jog := range r.getJoggers {
		jog.stop()
	}

	// Don't close bundles, they are shared between bucket's EC actions
	r.Finish(nil)
}

// Decode schedules an object to be restored from existing slices.
// A caller should wait for the main object restoration is completed. When
// ecrunner finishes main object restoration process it puts into request.ErrCh
// channel the error or nil. The caller may read the object after receiving
// a nil value from channel but ecrunner keeps working - it reuploads all missing
// slices or copies
func (r *XactGet) decode(req *request, lom *cluster.LOM) {
	debug.AssertMsg(req.Action == ActRestore, "invalid action for restore: "+req.Action)
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
	cos.AssertMsg(ok, "Mountpath unregister handler for EC called with invalid mountpath")
	getJog.stop()
	delete(r.getJoggers, mpath)
}

func (r *XactGet) Stats() cluster.XactStats {
	baseStats := r.DemandBase.Stats().(*xaction.BaseXactStatsExt)
	st := r.stats.stats()
	baseStats.Ext = &ExtECGetStats{
		AvgTime:     cos.Duration(st.DecodeTime),
		ErrCount:    st.DecodeErr,
		AvgObjTime:  cos.Duration(st.ObjTime),
		AvgQueueLen: st.QueueLen,
		IsIdle:      r.Pending() == 0,
	}
	baseStats.ObjCountX = st.GetReq
	return baseStats
}
