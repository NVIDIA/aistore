// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

type (
	XactBck interface {
		cmn.Xact
		BckIsLocal() bool
		DoneCh() chan struct{}
		Target() cluster.Target
		Mpathers() map[string]mpather
	}
	xactBckBase struct {
		// implements cmn.Xact a cmn.Runner interfaces
		cmn.XactBase
		cmn.MountpathXact
		// runtime
		doneCh   chan struct{}
		mpathers map[string]mpather
		// init
		t cluster.Target
	}
	joggerBckBase struct { // per mountpath
		parent    XactBck
		mpathInfo *fs.MountpathInfo
		config    *cmn.Config
		num, size int64
		stopCh    chan struct{}
		callback  func(lom *cluster.LOM) error
	}
)

func newXactBckBase(id int64, kind, bucket string, t cluster.Target, blocal bool) *xactBckBase {
	return &xactBckBase{
		XactBase: *cmn.NewXactBaseWithBucket(id, kind, bucket, blocal),
		t:        t,
	}
}

//
// as XactBck interface
//
func (r *xactBckBase) DoneCh() chan struct{}        { return r.doneCh }
func (r *xactBckBase) Target() cluster.Target       { return r.t }
func (r *xactBckBase) Mpathers() map[string]mpather { return r.mpathers }
func (r *xactBckBase) Description() string          { return "base bucket xaction implementation" }

// init and stop
func (r *xactBckBase) Stop(error) { r.Abort() } // call base method

func (r *xactBckBase) init(availablePaths map[string]*fs.MountpathInfo) {
	numjs := len(availablePaths)
	r.doneCh = make(chan struct{}, numjs)
	r.mpathers = make(map[string]mpather, numjs)
}

// control loop
func (r *xactBckBase) run(numjs int) error {
	for {
		select {
		case <-r.ChanAbort():
			r.stop()
			return fmt.Errorf("%s aborted, exiting", r)
		case <-r.doneCh:
			numjs--
			if numjs == 0 {
				glog.Infof("%s: all done", r)
				r.mpathers = nil
				r.stop()
				return nil
			}
		}
	}
}

func (r *xactBckBase) stop() {
	if r.Finished() {
		glog.Warningf("%s is (already) not running", r)
		return
	}
	for _, mpather := range r.mpathers {
		mpather.stop()
	}
	r.EndTime(time.Now())
}

//
// mpath jogger - as mpather
//

func (j *joggerBckBase) mountpathInfo() *fs.MountpathInfo { return j.mpathInfo }
func (j *joggerBckBase) post(*cluster.LOM)                { cmn.Assert(false) }
func (j *joggerBckBase) stop()                            { j.stopCh <- struct{}{}; close(j.stopCh) }

//
// mpath joggerBckBase - main
//
func (j *joggerBckBase) jog() {
	j.stopCh = make(chan struct{}, 1)
	dir := j.mpathInfo.MakePathBucket(fs.ObjectType, j.parent.Bucket(), j.parent.BckIsLocal())
	if err := filepath.Walk(dir, j.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("%s: stopping traversal: %s", dir, s)
		} else {
			glog.Errorln(err)
		}
	}
	j.parent.DoneCh() <- struct{}{}
}

func (j *joggerBckBase) walk(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		if errstr := cmn.PathWalkErr(err); errstr != "" {
			glog.Errorf(errstr)
			return err
		}
		return nil
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	lom, errstr := cluster.LOM{T: j.parent.Target(), FQN: fqn}.Init(j.config)
	if errstr != "" {
		return nil
	}
	if _, errstr := lom.Load(true); errstr != "" || !lom.Exists() {
		return nil
	}
	if lom.IsCopy() {
		return nil
	}
	cmn.Assert(j.parent.BckIsLocal() == lom.BckIsLocal)
	return j.callback(lom)
}

// [throttle]
func (j *joggerBckBase) yieldTerm() error {
	diskConf := &j.config.Disk
	select {
	case <-j.stopCh:
		return fmt.Errorf("jogger[%s/%s] aborted, exiting", j.mpathInfo, j.parent.Bucket())
	default:
		curr := fs.Mountpaths.Iostats.GetDiskUtil(j.mpathInfo.Path)
		if curr >= diskConf.DiskUtilHighWM {
			time.Sleep(cmn.ThrottleSleepMin)
		}
		break
	}
	return nil
}
