// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
)

type (
	xactECEncode struct {
		cmn.XactBase
		cmn.MountpathXact
		doneCh   chan struct{}
		mpathers map[string]*joggerECEncode
		t        cluster.Target
		bck      *cluster.Bck
		wg       *sync.WaitGroup // to wait for EC finishes all objects
	}
	joggerECEncode struct { // per mountpath
		parent    *xactECEncode
		mpathInfo *fs.MountpathInfo
		config    *cmn.Config
		stopCh    cmn.StopCh

		// to cache some info for quick access
		provider string
		smap     *cluster.Smap
		daemonID string
	}
)

func newXactECEncode(id int64, bck *cluster.Bck, t cluster.Target) *xactECEncode {
	return &xactECEncode{
		XactBase: *cmn.NewXactBaseWithBucket(id, cmn.ActECEncode, bck.Name, bck.IsAIS()),
		t:        t,
		bck:      bck,
		wg:       &sync.WaitGroup{},
	}
}

func (r *xactECEncode) done()                      { r.doneCh <- struct{}{} }
func (r *xactECEncode) waitGroup() *sync.WaitGroup { return r.wg }
func (r *xactECEncode) target() cluster.Target     { return r.t }
func (r *xactECEncode) Description() string        { return "erasure code all objects in a bucket" }

func (r *xactECEncode) Run() (err error) {
	var numjs int
	if !r.bck.Props.EC.Enabled {
		return fmt.Errorf("Bucket %q does not have EC enabled", r.bck.Name)
	}
	if numjs, err = r.init(); err != nil {
		return
	}
	return r.run(numjs)
}

func (r *xactECEncode) init() (int, error) {
	availablePaths, _ := fs.Mountpaths.Get()
	numjs := len(availablePaths)
	r.doneCh = make(chan struct{}, numjs)
	r.mpathers = make(map[string]*joggerECEncode, numjs)
	config := cmn.GCO.Get()
	for _, mpathInfo := range availablePaths {
		jogger := &joggerECEncode{
			parent:    r,
			mpathInfo: mpathInfo,
			config:    config,
			smap:      r.t.GetSmap(),
			daemonID:  r.t.Snode().DaemonID,
			stopCh:    cmn.NewStopCh(),
		}
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.Provider())
		r.mpathers[mpathLC] = jogger
	}
	for _, mpather := range r.mpathers {
		go mpather.jog()
	}
	return numjs, nil
}

func (r *xactECEncode) Stop(error) { r.Abort() }

func (r *xactECEncode) run(numjs int) error {
	for {
		select {
		case <-r.ChanAbort():
			r.stop()
			return fmt.Errorf("%s aborted, exiting", r)
		case <-r.doneCh:
			numjs--
			if numjs == 0 {
				glog.Infof("%s: all done. Waiting for EC finishes", r)
				r.wg.Wait()
				r.mpathers = nil
				r.stop()
				return nil
			}
		}
	}
}

func (r *xactECEncode) stop() {
	if r.Finished() {
		glog.Warningf("%s is (already) not running", r)
		return
	}
	for _, mpather := range r.mpathers {
		mpather.stop()
	}
	r.EndTime(time.Now())
}

func (j *joggerECEncode) stop() { j.stopCh.Close() }

func (j *joggerECEncode) jog() {
	dir := j.mpathInfo.MakePathBucket(fs.ObjectType, j.parent.Bucket(), j.parent.Provider())
	j.provider = j.parent.Provider()
	opts := &fs.Options{
		Callback: j.walk,
		Sorted:   false,
	}
	if err := fs.Walk(dir, opts); err != nil {
		glog.Errorln(err)
	}
	j.parent.done()
}

// Walks through all files in 'obj' directory, and calls EC.Encode for every
// file whose HRW points to this file and the file does not have corresponding
// metadata file in 'meta' directory
func (j *joggerECEncode) walk(fqn string, de fs.DirEntry) error {
	select {
	case <-j.stopCh.Listen():
		return fmt.Errorf("jogger[%s/%s] aborted, exiting", j.mpathInfo, j.parent.Bucket())
	default:
	}

	if de.IsDir() {
		return nil
	}
	lom := &cluster.LOM{T: j.parent.target(), FQN: fqn}
	err := lom.Init("", j.provider, j.config)
	if err != nil {
		return nil
	}
	if err := lom.Load(); err != nil || !lom.Exists() {
		return nil
	}

	// a mirror of the object - skip EC
	if !lom.IsHRW() {
		return nil
	}

	si, err := cluster.HrwTarget(lom.Bck(), lom.Objname, j.smap)
	if err != nil {
		glog.Errorf("%s: %s", lom, err)
		return nil
	}
	// an object replica - skip EC
	if j.daemonID != si.DaemonID {
		return nil
	}

	mdFQN, _, err := cluster.HrwFQN(ec.MetaType, lom.Bck(), lom.Objname)
	if err != nil {
		glog.Warningf("Metadata FQN generation failed %q: %v", fqn, err)
		return nil
	}
	_, err = os.Stat(mdFQN)
	// metadata file exists - the object was already EC'ed before
	if err == nil {
		return nil
	}
	if !os.IsNotExist(err) {
		glog.Warningf("Failed to stat %q: %v", mdFQN, err)
		return nil
	}

	// EC PUT jogger decreases 'waitGroup' after EC has processed the object
	j.parent.waitGroup().Add(1)
	if err = ECM.EncodeObject(lom, j.parent.waitGroup()); err != nil {
		// something wrong with EC, interrupt file walk - it is critical
		return fmt.Errorf("Failed to EC object %q: %v", fqn, err)
	}

	return nil
}
