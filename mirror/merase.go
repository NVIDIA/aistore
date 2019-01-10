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

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/memsys"
)

const throttleNumErased = 16

type (
	XactErase struct {
		// implements cmn.Xact a cmn.Runner interfaces
		cmn.XactBase
		cmn.Named
		// runtime
		mpathChangeCh chan struct{}
		erasers       map[string]*eraser
		config        *cmn.Config
		// init
		Bucket   string
		Mirror   cmn.MirrorConf
		Slab     *memsys.Slab2
		T        cluster.Target
		Bislocal bool
	}
	eraser struct { // one per mountpath
		parent    *XactErase
		mpathInfo *fs.MountpathInfo
		num       int64
		stopCh    chan struct{}
	}
)

/*
 * implements fs.PathRunner interface
 */
var _ fs.PathRunner = &XactErase{}

func (r *XactErase) SetID(id int64) { cmn.Assert(false) }

func (r *XactErase) ReqAddMountpath(mpath string)     { r.mpathChangeCh <- struct{}{} } // TODO: same for other "erasers"
func (r *XactErase) ReqRemoveMountpath(mpath string)  { r.mpathChangeCh <- struct{}{} }
func (r *XactErase) ReqEnableMountpath(mpath string)  { r.mpathChangeCh <- struct{}{} }
func (r *XactErase) ReqDisableMountpath(mpath string) { r.mpathChangeCh <- struct{}{} }

//
// public methods
//

func (r *XactErase) Run() error {
	// init
	availablePaths, _ := fs.Mountpaths.Get()
	r.erasers = make(map[string]*eraser, len(availablePaths))
	r.config = cmn.GCO.Get()
init:
	// start mpath erasers
	for _, mpathInfo := range availablePaths {
		eraser := &eraser{parent: r, mpathInfo: mpathInfo}
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.Bislocal)
		r.erasers[mpathLC] = eraser
		go eraser.jog()
	}
	// control loop
	for {
		select {
		case <-r.ChanAbort():
			r.stop()
			return fmt.Errorf("%s aborted, exiting", r)
		case <-r.mpathChangeCh:
			r.config = cmn.GCO.Get()
			for _, eraser := range r.erasers {
				eraser.stop()
			}
			availablePaths, _ = fs.Mountpaths.Get()
			l := len(availablePaths)
			r.erasers = make(map[string]*eraser, l) // new erasers map
			if l == 0 {
				r.stop()
				return fmt.Errorf("%s no mountpaths, exiting", r)
			}
			goto init // reinitialize and keep running
		}
	}
}

func (r *XactErase) Stop(error) { r.Abort() } // call base method

//
// private methods
//

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
	j.stopCh = make(chan struct{}, 1)
	dir := j.mpathInfo.MakePathBucket(fs.ObjectType, j.parent.Bucket, j.parent.Bislocal)
	if err := filepath.Walk(dir, j.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("%s: stopping traversal: %s", dir, s)
		} else {
			glog.Errorf("%s: failed to traverse, err: %v", dir, err)
		}
		return
	}
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
	lom := &cluster.LOM{T: j.parent.T, Fqn: fqn}
	if errstr := lom.Fill(cluster.LomFstat|cluster.LomCopy, j.parent.config); errstr != "" || lom.Doesnotexist {
		if glog.V(4) {
			glog.Infof("Warning: %s", errstr)
		}
		return nil
	}
	if !lom.HasCopy() {
		return nil
	}
	if errstr := lom.DelCopy(); errstr != "" {
		return errors.New(errstr)
	}
	j.num++
	if j.num >= throttleNumErased {
		j.num = 0
		if err = j.yieldTerm(); err != nil {
			return err
		}
	} else {
		runtime.Gosched()
	}
	return nil
}

func (j *eraser) yieldTerm() error {
	select {
	case <-j.stopCh:
		return fmt.Errorf("%s aborted, exiting", j)
	default:
		_, curr := j.mpathInfo.GetIOstats(fs.StatDiskUtil)
		j.num = 0
		if curr.Max >= float32(j.parent.config.Xaction.DiskUtilHighWM) && curr.Min > float32(j.parent.config.Xaction.DiskUtilLowWM) {
			time.Sleep(cmn.ThrottleSleepOut)
		} else {
			time.Sleep(cmn.ThrottleSleepIn)
		}
		break
	}
	return nil
}
