// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

const (
	throttleNumErased = 16                     // unit of self-throttling
	logNumErased      = throttleNumErased * 16 // unit of house-keeping
	logNumCopied      = logNumErased
	logNumDropped     = logNumErased
)

// XactErase (extended action) reduces data redundancy of a given bucket to 1 (single copy)
// It runs in a background and traverses all local mountpaths to do the job.

type (
	XactErase struct {
		// implements cmn.Xact a cmn.Runner interfaces
		cmn.XactBase
		// runtime
		doneCh  chan struct{}
		erasers map[string]*eraser
		// init
		T          cluster.Target
		Namelocker cluster.NameLocker
		Bislocal   bool
	}
	eraser struct { // one per mountpath
		parent    *XactErase
		mpathInfo *fs.MountpathInfo
		config    *cmn.Config
		num       int64
		stopCh    chan struct{}
	}
)

//
// public methods
//

func (r *XactErase) Run() (err error) {
	var numjs int
	if numjs, err = r.init(); err != nil {
		return err
	}
	glog.Infoln(r.String())
	// control loop
	for {
		select {
		case <-r.ChanAbort():
			r.stop()
			return fmt.Errorf("%s aborted, exiting", r)
		case <-r.doneCh:
			numjs--
			if numjs == 0 {
				glog.Infof("%s: all erasers completed", r)
				r.erasers = nil
				r.stop()
				return
			}
		}
	}
}

func (r *XactErase) Stop(error) { r.Abort() } // call base method

//
// private methods
//

func (r *XactErase) init() (numjs int, err error) {
	availablePaths, _ := fs.Mountpaths.Get()
	numjs = len(availablePaths)
	if err = checkErrNumMp(r, numjs); err != nil {
		return
	}
	r.doneCh = make(chan struct{}, numjs)
	r.erasers = make(map[string]*eraser, numjs)
	config := cmn.GCO.Get()
	for _, mpathInfo := range availablePaths {
		eraser := &eraser{parent: r, mpathInfo: mpathInfo, config: config}
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.Bislocal)
		r.erasers[mpathLC] = eraser
		go eraser.jog()
	}
	return
}

func (r *XactErase) stop() {
	if r.Finished() {
		glog.Warningf("%s is (already) not running", r)
		return
	}
	for _, eraser := range r.erasers {
		eraser.stop()
	}
	r.EndTime(time.Now())
}

//
// mpath eraser
//
func (j *eraser) stop() { j.stopCh <- struct{}{}; close(j.stopCh) }

func (j *eraser) jog() {
	glog.Infof("eraser[%s/%s] started", j.mpathInfo, j.parent.Bucket())
	j.stopCh = make(chan struct{}, 1)
	dir := j.mpathInfo.MakePathBucket(fs.ObjectType, j.parent.Bucket(), j.parent.Bislocal)
	if err := filepath.Walk(dir, j.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("%s: stopping traversal: %s", dir, s)
		} else {
			glog.Errorf("%s: failed to traverse, err: %v", dir, err)
		}
	}
	j.parent.doneCh <- struct{}{}
}

func (j *eraser) walk(fqn string, osfi os.FileInfo, err error) error {
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
	lom := &cluster.LOM{T: j.parent.T, FQN: fqn}
	if errstr := lom.Fill("", cluster.LomFstat|cluster.LomCopy, j.config); errstr != "" || !lom.Exists() {
		if glog.V(4) {
			glog.Infof("Warning: %s", errstr)
		}
		return nil
	}
	if !lom.HasCopy() {
		return nil
	}

	j.parent.Namelocker.Lock(lom.Uname, true)
	defer j.parent.Namelocker.Unlock(lom.Uname, true)

	if errstr := lom.DelCopy(); errstr != "" {
		return errors.New(errstr)
	}
	j.num++
	if (j.num % throttleNumErased) == 0 {
		if err = j.yieldTerm(); err != nil {
			return err
		}
		if (j.num % logNumErased) == 0 {
			glog.Infof("eraser[%s/%s] erased %d copies...", j.mpathInfo, j.parent.Bucket(), j.num)
			j.config = cmn.GCO.Get()
		}
	} else {
		runtime.Gosched()
	}
	return nil
}

// [throttle]
func (j *eraser) yieldTerm() error {
	xaction := &j.config.Xaction
	select {
	case <-j.stopCh:
		return fmt.Errorf("eraser[%s/%s] aborted, exiting", j.mpathInfo, j.parent.Bucket())
	default:
		_, curr := j.mpathInfo.GetIOstats(fs.StatDiskUtil)
		if curr.Max >= float32(xaction.DiskUtilHighWM) && curr.Min > float32(xaction.DiskUtilLowWM) {
			time.Sleep(cmn.ThrottleSleepAvg)
		} else {
			time.Sleep(cmn.ThrottleSleepMin)
		}
		break
	}
	return nil
}

// common helper
func checkErrNumMp(xx cmn.Xact, l int) error {
	if l < 2 {
		return fmt.Errorf("%s: number of mountpaths (%d) is insufficient for local mirroring, exiting", xx, l)
	}
	return nil
}
