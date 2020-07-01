// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"errors"
	"fmt"
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
		// runtime
		doneCh   chan struct{}
		mpathers map[string]mpather
		// init
		t cluster.Target
	}
	joggerBckBase struct { // per mountpath
		parent XactBck
		// TODO: this duplicates `bck` from `parent.Bck()` but for now it is
		//  required because CopyBucket uses different bucket (`bck=bckFrom`)
		//  for jogging and different (`parent.Bck()=bckTo`) for checking
		//  if xaction runs on given bucket or getting stats.
		bck       cmn.Bck
		mpathInfo *fs.MountpathInfo
		config    *cmn.Config
		num, size int64
		stopCh    *cmn.StopCh
		callback  func(lom *cluster.LOM) error
		skipLoad  bool // true: skip lom.Load() and further checks (e.g. done in callback under lock)
	}
)

func newXactBckBase(id, kind string, bck cmn.Bck, t cluster.Target) *xactBckBase {
	return &xactBckBase{XactBase: *cmn.NewXactBaseWithBucket(id, kind, bck), t: t}
}

//
// as XactBck interface
//
func (r *xactBckBase) IsMountpathXact() bool        { return true }
func (r *xactBckBase) DoneCh() chan struct{}        { return r.doneCh }
func (r *xactBckBase) Target() cluster.Target       { return r.t }
func (r *xactBckBase) Mpathers() map[string]mpather { return r.mpathers }

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
			return cmn.NewAbortedError(r.String())
		case <-r.doneCh:
			mpathersCount--
			if mpathersCount == 0 {
				glog.Infof("%s: all done", r)
				r.mpathers = nil
				return r.stop()
			}
		}
	}
}

func (r *xactBckBase) stop() (err error) {
	var n int
	for _, mpather := range r.mpathers {
		n += mpather.stop()
	}
	if n > 0 {
		err = fmt.Errorf("%s: dropped %d object(s)", r, n)
	}
	return
}

//
// mpath joggerBckBase - main
//

func (j *joggerBckBase) jog() {
	cmn.Assert(j.bck.HasProvider())

	j.stopCh = cmn.NewStopCh()
	opts := &fs.Options{
		Mpath:    j.mpathInfo,
		Bck:      j.bck,
		CTs:      []string{fs.ObjectType},
		Callback: j.walk,
		Sorted:   false,
	}
	if err := fs.Walk(opts); err != nil {
		if errors.As(err, &cmn.AbortedError{}) {
			glog.Infof("stopping traversal: %v", err)
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
	err := lom.Init(j.bck, j.config)
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
		return fmt.Errorf("jogger[%s/%s] aborted, exiting", j.mpathInfo, j.bck)
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
func (j *joggerBckBase) stop() int                        { j.stopCh.Close(); return 0 }
