// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
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
		DoneCh() chan struct{}
		Target() cluster.Target
		Mpathers() map[string]mpather
	}
	xactBckBase struct {
		// implements cmn.Xact and cmn.Runner interfaces
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
		stopCh    cmn.StopCh
		callback  func(lom *cluster.LOM) error
		skipLoad  bool // true: skip lom.Load() and further checks (e.g. done in callback under lock)
	}
)

func newXactBckBase(id, kind string, bck *cluster.Bck, t cluster.Target) *xactBckBase {
	return &xactBckBase{XactBase: *cmn.NewXactBaseWithBucket(id, kind, bck.Bck), t: t}
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

func (r *xactBckBase) init(mpathCount int) {
	r.doneCh = make(chan struct{}, 1)
	r.mpathers = make(map[string]mpather, mpathCount)
}

// control loop
func (r *xactBckBase) run(mpathersCount int) error {
	for {
		select {
		case <-r.ChanAbort():
			r.stop()
			return fmt.Errorf("%s aborted, exiting", r)
		case <-r.doneCh:
			mpathersCount--
			if mpathersCount == 0 {
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
// mpath joggerBckBase - main
//

func (j *joggerBckBase) jog() {
	j.stopCh = cmn.NewStopCh()

	opts := &fs.Options{
		Mpath:    j.mpathInfo,
		Bck:      j.parent.Bck(),
		CTs:      []string{fs.ObjectType},
		Callback: j.walk,
		Sorted:   false,
	}
	if err := fs.Walk(opts); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("stopping traversal: %s", s)
		} else {
			glog.Errorln(err)
		}
	}
	j.parent.DoneCh() <- struct{}{}
}

func (j *joggerBckBase) walk(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		return nil
	}
	lom := &cluster.LOM{T: j.parent.Target(), FQN: fqn}
	err := lom.Init(j.parent.Bck(), j.config)
	if err != nil {
		return nil
	}
	if !j.skipLoad {
		if err := lom.Load(); err != nil {
			return nil
		}
		if lom.IsCopy() {
			return nil
		}
	}
	return j.callback(lom)
}

// [throttle]
func (j *joggerBckBase) yieldTerm() error {
	diskConf := &j.config.Disk
	select {
	case <-j.stopCh.Listen():
		return fmt.Errorf("jogger[%s/%s] aborted, exiting", j.mpathInfo, j.parent.Bck())
	default:
		curr := fs.Mountpaths.GetMpathUtil(j.mpathInfo.Path, time.Now())
		if curr >= diskConf.DiskUtilHighWM {
			time.Sleep(cmn.ThrottleSleepMin)
		}
		break
	}
	return nil
}

//
// mpath jogger - as mpather
//

func (j *joggerBckBase) mountpathInfo() *fs.MountpathInfo { return j.mpathInfo }
func (j *joggerBckBase) post(*cluster.LOM)                { cmn.Assert(false) }
func (j *joggerBckBase) stop()                            { j.stopCh.Close() }
