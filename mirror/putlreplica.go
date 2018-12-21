// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

const (
	workChanCap = 256
)

type (
	XactPut struct {
		// implements cmn.Xact a cmn.Runner interfaces
		cmn.XactDemandBase
		cmn.Named
		// runtime
		workCh  chan *cluster.LOM
		joggers map[string]*putmp
		// init
		Bucket   string
		Bislocal bool
	}
	putmp struct {
		parent    *XactPut
		mpathInfo *fs.MountpathInfo
		workCh    chan *cluster.LOM
		stopCh    chan struct{}
	}
)

/*
 * implements fs.PathRunner interface
 */
var _ fs.PathRunner = &XactPut{}

func (r *XactPut) SetID(id int64) { cmn.Assert(false) }

func (r *XactPut) ReqAddMountpath(mpath string)     { cmn.Assert(false, "NIY") } // TODO
func (r *XactPut) ReqRemoveMountpath(mpath string)  { cmn.Assert(false, "NIY") }
func (r *XactPut) ReqEnableMountpath(mpath string)  { cmn.Assert(false, "NIY") }
func (r *XactPut) ReqDisableMountpath(mpath string) { cmn.Assert(false, "NIY") }

//
// public methods
//

// - runs on a per- mirrored bucket basis
// - dispatches replication requests for execution by one of the dedicated "joggers"
// - ref-counts pending requests and self-terminates when idle for a while
func (r *XactPut) Run() error {
	// init
	r.init()

	// start mpath joggers
	availablePaths, _ := fs.Mountpaths.Get()
	for mpath, mpathInfo := range availablePaths {
		var mpathplus string
		if r.Bislocal {
			mpathplus = fs.Mountpaths.MakePathLocal(mpath, fs.ObjectType)
		} else {
			mpathplus = fs.Mountpaths.MakePathCloud(mpath, fs.ObjectType)
		}
		putmp := &putmp{parent: r, mpathInfo: mpathInfo}
		r.joggers[mpathplus] = putmp
		putmp.jog()
	}

	// control loop
	for {
		select {
		case lom := <-r.workCh:
			glog.Infof("new lom %s", lom) // FIXME: remove
			cmn.Assert(lom.Bucket == r.Bucket && lom.Bislocal == r.Bislocal)

			r.dispatch(lom) // DISPATCH ==================> to the least loaded
		case <-r.ChanCheckTimeout():
			if r.Timeout() {
				r.stop()
				return nil
			}
		case <-r.ChanAbort():
			r.stop()
			return fmt.Errorf("%s aborted, exiting", r)
		}
	}
}

// main method: replicate a given locally stored object
func (r *XactPut) Copy(lom *cluster.LOM) (err error) {
	r.IncPending() // ref-count via base
	if r.Finished() {
		err = cmn.NewErrXpired("Cannot replicate: " + r.String())
		return
	}
	r.workCh <- lom
	return
}

func (r *XactPut) Stop(error) { r.Abort() } // call base method

//
// private methods
//

func (r *XactPut) init() { r.workCh = make(chan *cluster.LOM, workChanCap) }

func (r *XactPut) dispatch(lom *cluster.LOM) {
	var jogger *putmp
	for _, j := range r.joggers {
		// TODO: sort by LOAD(jogger.mpathInfo.FileSystem)
		fs := jogger.mpathInfo.FileSystem
		_ = fs
		if true {
			jogger = j
			break
		}
	}
	jogger.workCh <- lom
}

func (r *XactPut) stop() {
	if r.Finished() {
		glog.Warningf("%s - not running, nothing to do", r)
		return
	}
	r.XactDemandBase.Stop()
	for _, jogger := range r.joggers {
		jogger.stop()
	}
	r.EndTime(time.Now())
}

//
// mpath jogger
//

func (j *putmp) init() {
	j.workCh = make(chan *cluster.LOM, workChanCap)
	j.stopCh = make(chan struct{}, 1)
}

func (j *putmp) jog() {
	j.init()
	for {
		select {
		case lom := <-j.workCh:
			glog.Infof("NIY lom %s", lom) // TODO do the throttling and the copying
			j._copy(lom)
			j.parent.DecPending()
		case <-j.stopCh:
			return
		}
	}
}

func (r *putmp) _copy(lom *cluster.LOM) { /* TODO */ }

func (r *putmp) stop() { r.stopCh <- struct{}{}; close(r.stopCh) }
