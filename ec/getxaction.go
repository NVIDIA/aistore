// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
* Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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
)

type (
	// Erasure coding runner: accepts requests and dispatches them to
	// a correct mountpath runner. Runner uses dedicated to EC memory manager
	// inherited by dependent mountpath runners
	XactGet struct {
		xactECBase
		xactReqBase
		getJoggers map[string]*getJogger // mountpath joggers for GET
	}
)

type (
	bgProcess = func(req *Request, toDisk bool, buffer []byte, cb func(error))
)

//
// XactGet
//

func NewGetXact(t cluster.Target, smap cluster.Sowner,
	si *cluster.Snode, bucket string, reqBundle, respBundle *transport.StreamBundle) *XactGet {
	XactCount.Inc()
	availablePaths, disabledPaths := fs.Mountpaths.Get()
	totalPaths := len(availablePaths) + len(disabledPaths)

	runner := &XactGet{
		getJoggers:  make(map[string]*getJogger, totalPaths),
		xactECBase:  newXactECBase(t, smap, si, bucket, reqBundle, respBundle),
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

func (r *XactGet) DispatchResp(iReq IntraReq, bck *cluster.Bck, objName string, objAttrs transport.ObjectAttrs,
	object io.Reader) {
	uname := unique(iReq.Sender, bck, objName)

	switch iReq.Act {
	// It is response to slice/replica request by an object
	// restoration process. In this case there should exists
	// a slice waiting for the data to come(registered with `regWriter`.
	// Read the data into the slice writer and notify the slice when
	// the transfer is completed
	case RespPut:
		if glog.V(4) {
			glog.Infof("Response from %s, %s", iReq.Sender, uname)
		}
		r.dOwner.mtx.Lock()
		writer, ok := r.dOwner.slices[uname]
		r.dOwner.mtx.Unlock()

		if !ok {
			glog.Errorf("No writer for %s/%s", bck.Name, objName)
			return
		}

		if err := r.writerReceive(writer, iReq.Exists, objAttrs, object); err != nil {
			glog.Errorf("Failed to read replica: %v", err)
		}
	default:
		// should be unreachable
		glog.Errorf("Invalid request: %d", iReq.Act)
	}
}

func (r *XactGet) newGetJogger(mpath string) *getJogger {
	return &getJogger{
		parent: r,
		mpath:  mpath,
		workCh: make(chan *Request, requestBufSizeFS),
		stopCh: make(chan struct{}, 1),
		jobs:   make(map[uint64]bgProcess, 4),
		sema:   make(chan struct{}, maxBgJobsPerJogger),
		diskCh: make(chan struct{}, 1),
	}
}

func (r *XactGet) Run() (err error) {
	glog.Infof("Starting %s", r.GetRunName())

	for _, jog := range r.getJoggers {
		go jog.run()
	}

	conf := cmn.GCO.Get()
	tck := time.NewTicker(conf.Periodic.StatsTime)
	lastAction := time.Now()
	idleTimeout := conf.Timeout.SendFile * 3

	// as of now all requests are equal. Some may get throttling later
	for {
		select {
		case <-tck.C:
			if s := fmt.Sprintf("%v", r.Stats()); s != "" {
				glog.Info(s)
			}
		case req := <-r.ecCh:
			lastAction = time.Now()
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
		case <-r.ChanCheckTimeout():
			idleEnds := lastAction.Add(idleTimeout)
			if idleEnds.Before(time.Now()) && r.Timeout() {
				if glog.V(4) {
					glog.Infof("Idle time is over: %v. Last action at: %v",
						time.Now(), lastAction)
				}
				// it's ok not to notify ecmanager, he'll just have stoped xact in a map
				r.stop()

				return nil
			}
		case msg := <-r.controlCh:

			if msg.Action == ActEnableRequests {
				r.setEcRequestsEnabled()
				break
			}
			cmn.Assert(msg.Action == ActClearRequests)

			r.setEcRequestsDisabled()

			// drain pending bucket's EC requests, return them with an error
			// note: loop can't be replaced with channel range, as the channel is never closed
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
		req.ErrCh <- fmt.Errorf("EC disabled, can't procced with the request on bucket %s", r.bckName)
		close(req.ErrCh)
	}
}

func (r *XactGet) Stop(error) { r.Abort() }

func (r *XactGet) stop() {
	if r.Finished() {
		glog.Warningf("%s - not running, nothing to do", r)
		return
	}

	XactCount.Dec()
	r.XactDemandBase.Stop()
	for _, jog := range r.getJoggers {
		jog.stop()
	}

	// Don't close bundles, they are shared between bucket's EC actions

	r.EndTime(time.Now())
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
	if !r.ecRequestsEnabled() {
		if req.ErrCh != nil {
			req.ErrCh <- fmt.Errorf("EC on bucket %s is being disabled, no EC requests accepted", r.bckName)
			close(req.ErrCh)
		}
		return
	}

	r.IncPending()
	if req.Action == ActRestore {
		jogger, ok := r.getJoggers[req.LOM.ParsedFQN.MpathInfo.Path]
		cmn.AssertMsg(ok, "Invalid mountpath given in EC request")
		r.stats.updateQueue(len(jogger.workCh))
		jogger.workCh <- req
	}
}

func (r *XactGet) Description() string {
	return "restore (recover) erasure coded objects"
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
