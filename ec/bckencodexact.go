// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

type (
	XactBckEncode struct {
		cmn.XactBase
		cmn.MountpathXact
		doneCh   chan struct{}
		mpathers map[string]*joggerBckEncode
		t        cluster.Target
		bck      *cluster.Bck
		wg       *sync.WaitGroup // to wait for EC finishes all objects
	}
	joggerBckEncode struct { // per mountpath
		parent    *XactBckEncode
		mpathInfo *fs.MountpathInfo
		config    *cmn.Config
		stopCh    cmn.StopCh

		// to cache some info for quick access
		provider string
		smap     *cluster.Smap
		daemonID string
		ecm      cluster.ECManager
	}
)

func NewXactBckEncode(id int64, bck *cluster.Bck, t cluster.Target) *XactBckEncode {
	return &XactBckEncode{
		XactBase: *cmn.NewXactBaseWithBucket(id, cmn.ActECEncode, bck.Name, bck.IsAIS()),
		t:        t,
		bck:      bck,
		wg:       &sync.WaitGroup{},
	}
}

func (r *XactBckEncode) done()                  { r.doneCh <- struct{}{} }
func (r *XactBckEncode) target() cluster.Target { return r.t }
func (r *XactBckEncode) Description() string    { return "erasure code all objects in a bucket" }

func (r *XactBckEncode) beforeECObj() { r.wg.Add(1) }
func (r *XactBckEncode) afterECObj(lom *cluster.LOM, err error) {
	if err == nil {
		r.ObjectsInc()
		r.BytesAdd(lom.Size())
	} else {
		glog.Errorf("Failed to EC object %s/%s: %v", lom.Bck().Name, lom.Objname, err)
	}

	r.wg.Done()
}

func (r *XactBckEncode) Run() (err error) {
	var numjs int
	if !r.bck.Props.EC.Enabled {
		return fmt.Errorf("Bucket %q does not have EC enabled", r.bck.Name)
	}
	if numjs, err = r.init(); err != nil {
		return
	}
	return r.run(numjs)
}

func (r *XactBckEncode) init() (int, error) {
	availablePaths, _ := fs.Mountpaths.Get()
	numjs := len(availablePaths)
	r.doneCh = make(chan struct{}, numjs)
	r.mpathers = make(map[string]*joggerBckEncode, numjs)
	config := cmn.GCO.Get()
	for _, mpathInfo := range availablePaths {
		jogger := &joggerBckEncode{
			parent:    r,
			mpathInfo: mpathInfo,
			config:    config,
			smap:      r.t.GetSowner().Get(),
			daemonID:  r.t.Snode().DaemonID,
			stopCh:    cmn.NewStopCh(),
			ecm:       r.t.ECM(),
		}
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.Provider())
		r.mpathers[mpathLC] = jogger
	}
	for _, mpather := range r.mpathers {
		go mpather.jog()
	}
	return numjs, nil
}

func (r *XactBckEncode) Stop(error) { r.Abort() }

func (r *XactBckEncode) run(numjs int) error {
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

func (r *XactBckEncode) stop() {
	if r.Finished() {
		glog.Warningf("%s is (already) not running", r)
		return
	}
	for _, mpather := range r.mpathers {
		mpather.stop()
	}
	r.EndTime(time.Now())
}

func (j *joggerBckEncode) stop() { j.stopCh.Close() }

func (j *joggerBckEncode) jog() {
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
func (j *joggerBckEncode) walk(fqn string, de fs.DirEntry) error {
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
	if err := lom.Load(); err != nil {
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

	mdFQN, _, err := cluster.HrwFQN(MetaType, lom.Bck(), lom.Objname)
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

	// beforeECObj increases a counter, and callback afterECObj decreases it.
	// After Walk finishes, the xaction waits until counter drops to zero.
	// That means all objects have been processed and xaction can finalize.
	j.parent.beforeECObj()
	if err = j.ecm.EncodeObject(lom, j.parent.afterECObj); err != nil {
		// something wrong with EC, interrupt file walk - it is critical
		return fmt.Errorf("Failed to EC object %q: %v", fqn, err)
	}

	return nil
}
