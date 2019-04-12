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
	"github.com/NVIDIA/aistore/memsys"
)

const (
	throttleNumObjects = 16                      // unit of self-throttling
	logNumProcessed    = throttleNumObjects * 16 // unit of house-keeping
	//
	MaxNCopies = 16
)

// XactBckMakeNCopies (extended action) reduces data redundancy of a given bucket to 1 (single copy)
// It runs in a background and traverses all local mountpaths to do the job.

type (
	XactBckMakeNCopies struct {
		// implements cmn.Xact a cmn.Runner interfaces
		cmn.XactBase
		// runtime
		doneCh   chan struct{}
		mpathers map[string]mpather
		// init
		T          cluster.Target
		Namelocker cluster.NameLocker
		Slab       *memsys.Slab2
		Copies     int
		BckIsLocal bool
	}
	jogger struct { // one per mountpath
		parent    *XactBckMakeNCopies
		mpathInfo *fs.MountpathInfo
		config    *cmn.Config
		num       int64
		stopCh    chan struct{}
		buf       []byte
	}
)

//
// public methods
//

func (r *XactBckMakeNCopies) Run() (err error) {
	var numjs int
	if numjs, err = r.init(); err != nil {
		return err
	}
	glog.Infoln(r.String(), "copies=", r.Copies)
	// control loop
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
				return
			}
		}
	}
}

func (r *XactBckMakeNCopies) Stop(error) { r.Abort() } // call base method

func ValidateNCopies(copies int) error {
	const s = "number of local copies"
	if copies <= 0 {
		return fmt.Errorf("%s cannot be negative (%d)", s, copies)
	}
	if copies > MaxNCopies {
		return fmt.Errorf("%s (%d) exceeds %d and is likely invalid", s, copies, MaxNCopies)
	}
	availablePaths, _ := fs.Mountpaths.Get()
	nmp := len(availablePaths)
	if copies > nmp {
		return fmt.Errorf("%s (%d) exceeds the number of mountpaths (%d)", s, copies, nmp)
	}
	return nil
}

//
// private methods
//

func (r *XactBckMakeNCopies) init() (numjs int, err error) {
	if err = ValidateNCopies(r.Copies); err != nil {
		return
	}
	availablePaths, _ := fs.Mountpaths.Get()
	numjs = len(availablePaths)
	if err = checkErrNumMp(r, numjs); err != nil {
		return
	}
	r.doneCh = make(chan struct{}, numjs)
	r.mpathers = make(map[string]mpather, numjs)
	config := cmn.GCO.Get()
	for _, mpathInfo := range availablePaths {
		jogger := &jogger{parent: r, mpathInfo: mpathInfo, config: config}
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.BckIsLocal)
		r.mpathers[mpathLC] = jogger
		go jogger.jog()
	}
	return
}

func (r *XactBckMakeNCopies) stop() {
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

func (j *jogger) mountpathInfo() *fs.MountpathInfo { return j.mpathInfo }
func (j *jogger) post(lom *cluster.LOM)            { cmn.Assert(false) }
func (j *jogger) stop()                            { j.stopCh <- struct{}{}; close(j.stopCh) }

//
// mpath jogger - main
//
func (j *jogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.Bucket())
	j.stopCh = make(chan struct{}, 1)
	j.buf = j.parent.Slab.Alloc()
	dir := j.mpathInfo.MakePathBucket(fs.ObjectType, j.parent.Bucket(), j.parent.BckIsLocal)
	if err := filepath.Walk(dir, j.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("%s: stopping traversal: %s", dir, s)
		} else {
			glog.Errorln(err)
		}
	}
	j.parent.doneCh <- struct{}{}
	j.parent.Slab.Free(j.buf)
}

func (j *jogger) walk(fqn string, osfi os.FileInfo, err error) error {
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
	lom, errstr := cluster.LOM{T: j.parent.T, FQN: fqn}.Init(j.config)
	if errstr != "" {
		return nil
	}
	if errstr := lom.Load(true); errstr != "" || !lom.Exists() {
		return nil
	}
	if lom.IsCopy() {
		return nil
	}
	cmn.Assert(j.parent.BckIsLocal == lom.BckIsLocal)
	if n := lom.NumCopies(); n == j.parent.Copies {
		return nil
	} else if n > j.parent.Copies {
		err = j.delCopies(lom)
	} else {
		err = j.addCopies(lom)
	}
	j.num++
	if (j.num % throttleNumObjects) == 0 {
		if err = j.yieldTerm(); err != nil {
			return err
		}
		if (j.num % logNumProcessed) == 0 {
			glog.Infof("jogger[%s/%s] erased %d copies...", j.mpathInfo, j.parent.Bucket(), j.num)
			j.config = cmn.GCO.Get()
		}
	} else {
		runtime.Gosched()
	}
	return err
}

func (j *jogger) delCopies(lom *cluster.LOM) (err error) {
	j.parent.Namelocker.Lock(lom.Uname(), true)
	if j.parent.Copies == 1 {
		if errstr := lom.DelAllCopies(); errstr != "" {
			err = errors.New(errstr)
		}
	} else {
		copies := lom.CopyFQN()
		for i := len(copies) - 1; i >= j.parent.Copies-1; i-- {
			cpyfqn := copies[i]
			if errstr := lom.DelCopy(cpyfqn); errstr != "" {
				err = errors.New(errstr)
				break
			}
		}
	}
	lom.ReCache()
	j.parent.Namelocker.Unlock(lom.Uname(), true)
	return
}

func (j *jogger) addCopies(lom *cluster.LOM) (err error) {
	for i := lom.NumCopies() + 1; i <= j.parent.Copies; i++ {
		if mpather := findLeastUtilized(lom, j.parent.mpathers); mpather != nil {
			if err = copyTo(lom, mpather.mountpathInfo(), j.buf); err != nil {
				glog.Errorln(err)
				return
			}
			if glog.V(4) {
				glog.Infof("%s: %s=>%s", lom, lom.ParsedFQN.MpathInfo, mpather.mountpathInfo())
			}
		} else {
			err = fmt.Errorf("%s (copies=%d): cannot find dst mountpath", lom, lom.NumCopies())
			return
		}
	}
	return nil
}

// [throttle]
func (j *jogger) yieldTerm() error {
	diskConf := &j.config.Disk
	select {
	case <-j.stopCh:
		return fmt.Errorf("jogger[%s/%s] aborted, exiting", j.mpathInfo, j.parent.Bucket())
	default:
		curr := j.mpathInfo.Iostat.GetDiskUtil()
		if curr >= diskConf.DiskUtilHighWM {
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
