// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
* Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
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
	XactPut struct {
		xactECBase
		xactReqBase
		putJoggers map[string]*putJogger // mountpath joggers for PUT/DEL
	}
)

//
// XactGet
//

func NewPutXact(t cluster.Target, smap cluster.Sowner,
	si *cluster.Snode, bucket string, reqBundle, respBundle *transport.StreamBundle) *XactPut {
	XactCount.Inc()
	availablePaths, disabledPaths := fs.Mountpaths.Get()
	totalPaths := len(availablePaths) + len(disabledPaths)
	runner := &XactPut{
		putJoggers:  make(map[string]*putJogger, totalPaths),
		xactECBase:  newXactECBase(t, smap, si, bucket, reqBundle, respBundle),
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
		workCh: make(chan *Request, requestBufSizeFS),
		stopCh: make(chan struct{}, 1),
	}
}

func (r *XactPut) Run() (err error) {
	glog.Infof("Starting %s", r.GetRunName())

	for _, jog := range r.putJoggers {
		go jog.run()
	}
	conf := cmn.GCO.Get()
	tck := time.NewTicker(conf.Periodic.StatsTime)
	lastAction := time.Now()
	idleTimeout := conf.Timeout.SendFile * 3

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
			return fmt.Errorf("%s aborted, exiting", r)
		default:
		}

		select {
		case <-tck.C:
			if s := fmt.Sprintf("%v", r.Stats()); s != "" {
				glog.Info(s)
			}
		case req := <-r.ecCh:
			lastAction = time.Now()
			switch req.Action {
			case ActSplit:
				r.stats.updateEncode(req.LOM.Size())
			case ActDelete:
				r.stats.updateDelete()
			default:
				glog.Errorf("Invalid request's action %s for putxaction", req.Action)
			}
			r.dispatchRequest(req)
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
			return fmt.Errorf("%s aborted, exiting", r)
		}
	}
}

func (r *XactPut) abortECRequestWhenDisabled(req *Request) {
	if req.ErrCh != nil {
		req.ErrCh <- fmt.Errorf("EC disabled, can't procced with the request on bucket %s", r.bckName)
		close(req.ErrCh)
	}
}

func (r *XactPut) Stop(error) { r.Abort() }

func (r *XactPut) stop() {
	if r.Finished() {
		glog.Warningf("%s - not running, nothing to do", r)
		return
	}

	XactCount.Dec()
	r.XactDemandBase.Stop()
	for _, jog := range r.putJoggers {
		jog.stop()
	}

	// Don't close bundles, they are shared between different EC xactions

	r.EndTime(time.Now())
}

// Encode schedules FQN for erasure coding process
func (r *XactPut) Encode(req *Request) {
	req.putTime = time.Now()
	req.tm = time.Now()
	if glog.V(4) {
		glog.Infof("ECXAction for bucket %s (queue = %d): encode object %s",
			r.bckName, len(r.ecCh), req.LOM.Uname())
	}

	r.dispatchDecodingRequest(req)
}

// Cleanup deletes all object slices or copies after the main object is removed
func (r *XactPut) Cleanup(req *Request) {
	req.putTime = time.Now()
	req.tm = time.Now()

	r.dispatchDecodingRequest(req)
}

func (r *XactPut) dispatchDecodingRequest(req *Request) {
	if !r.ecRequestsEnabled() {
		r.abortECRequestWhenDisabled(req)
		return
	}

	r.ecCh <- req
}

func (r *XactPut) dispatchRequest(req *Request) {
	if !r.ecRequestsEnabled() {
		if req.ErrCh != nil {
			req.ErrCh <- fmt.Errorf("EC on bucket %s is being disabled, no EC requests accepted", r.bckName)
			close(req.ErrCh)
		}
		return
	}

	if req.Action == ActDelete || req.Action == ActSplit {
		r.IncPending()
		jogger, ok := r.putJoggers[req.LOM.ParsedFQN.MpathInfo.Path]
		cmn.AssertMsg(ok, "Invalid mountpath given in EC request")
		if glog.V(4) {
			glog.Infof("ECXAction (bg queue = %d): dispatching object %s....", len(jogger.workCh), req.LOM.Uname())
		}
		r.stats.updateQueue(len(jogger.workCh))
		jogger.workCh <- req
	}
}

func (r *XactPut) Description() string {
	return "encode objects upon PUT into an EC-enabled bucket"
}
