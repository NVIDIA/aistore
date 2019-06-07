// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	XactPutLRepl struct {
		// implements cmn.Xact a cmn.Runner interfaces
		cmn.XactDemandBase
		cmn.MountpathXact
		// runtime
		workCh   chan *cluster.LOM
		mpathers map[string]mpather
		// init
		mirror         cmn.MirrorConf
		slab           *memsys.Slab2
		wg             *sync.WaitGroup
		total, dropped int64
	}
	xputJogger struct { // one per mountpath
		parent    *XactPutLRepl
		mpathInfo *fs.MountpathInfo
		workCh    chan *cluster.LOM
		stopCh    chan struct{}
		buf       []byte
	}
)

//
// public methods
//

func RunXactPutLRepl(id int64, lom *cluster.LOM, slab *memsys.Slab2) (r *XactPutLRepl, err error) {
	r = &XactPutLRepl{
		XactDemandBase: *cmn.NewXactDemandBase(id, cmn.ActPutCopies, lom.Bucket, lom.BckIsLocal),
		slab:           slab,
		mirror:         *lom.MirrorConf(),
	}
	availablePaths, _ := fs.Mountpaths.Get()
	l := len(availablePaths)
	if err = checkErrNumMp(r, l); err != nil {
		r = nil
		return
	}
	r.workCh = make(chan *cluster.LOM, r.mirror.Burst)
	r.mpathers = make(map[string]mpather, l)
	//
	// RUN
	//
	r.wg = &sync.WaitGroup{}
	r.wg.Add(1)
	go r.Run()
	for _, mpathInfo := range availablePaths {
		xputJogger := &xputJogger{parent: r, mpathInfo: mpathInfo}
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.BckIsLocal())
		r.mpathers[mpathLC] = xputJogger
		r.wg.Add(1)
		go xputJogger.jog()
	}
	r.wg.Wait() // wait for all to start
	return
}

func (r *XactPutLRepl) Run() error {
	glog.Infoln(r.String())
	r.wg.Done()
	for {
		select {
		case lom := <-r.workCh:
			if _, errstr := lom.Load(true); errstr != "" {
				glog.Errorln(errstr)
				break
			}
			cmn.Assert(r.BckIsLocal() == lom.BckIsLocal)
			if mpather := findLeastUtilized(lom, r.mpathers); mpather != nil {
				mpather.post(lom)
			} else {
				glog.Errorf("%s: cannot find destination mountpath", lom)
			}
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

// TODO: move elsewhere and use bucket ID
func (r *XactPutLRepl) SameBucket(lom *cluster.LOM) bool {
	return r.BckIsLocal() == lom.BckIsLocal && r.Bucket() == lom.Bucket
}

// main method: replicate a given locally stored object
func (r *XactPutLRepl) Repl(lom *cluster.LOM) (err error) {
	if r.Finished() {
		err = cmn.NewErrXpired("Cannot replicate: " + r.String())
		return
	}
	r.total++
	// [throttle]
	// when the optimization objective is write perf,
	// we start dropping requests to make sure callers don't block
	pending, max := r.Pending(), r.mirror.Burst
	if r.mirror.OptimizePUT {
		if pending > 1 && pending >= max {
			r.dropped++
			if (r.dropped % logNumProcessed) == 0 {
				glog.Errorf("%s: pending=%d, total=%d, dropped=%d", r, pending, r.total, r.dropped)
			}
			return
		}
	}
	r.IncPending() // ref-count via base to support on-demand action
	r.workCh <- lom

	// [throttle]
	// a bit of back-pressure when approaching the fixed boundary
	if pending > 1 && max > 10 {
		// increase the chances for the burst of PUTs to subside
		// but only to a point
		if pending > max/2 && pending < max-max/8 {
			time.Sleep(cmn.ThrottleSleepAvg)
		}
	}
	return
}

func (r *XactPutLRepl) Stop(error) { r.Abort() } // call base method
//
// private methods
//

// =================== load balancing and self-throttling ========================
// Generally,
// load balancing decision must (... TODO ...) be configurable and a function of:
// - current utilization (%) of the filesystem's disks;
// - current disk queue lengths and their respective minimums and maximums during
//   the reporting period (config.Periodic.IostatTimeLong);
// - previous values of the same, and their corresponding averages.
//
// Further, load balancers must take into account relative priorities of
// other workloads that are simultaneously present in the system -
// and self-throttle accordingly. E.g., in most cases we'd want GET to have the
// top (default, configurable) priority which would mean that the filesystems that
// serve GETs are even less available for other extended actions than otherwise, etc.
// =================== load balancing and self-throttling ========================

func (r *XactPutLRepl) stop() {
	if r.Finished() {
		glog.Warningf("%s is (already) not running", r)
		return
	}
	r.XactDemandBase.Stop()
	for _, mpather := range r.mpathers {
		mpather.stop()
	}
	r.EndTime(time.Now())
	for lom := range r.workCh {
		glog.Infof("Stopping, not copying %s", lom)
		r.DecPending()
	}
}

func (r *XactPutLRepl) Description() string {
	return "responsible for creating local object replicas upon PUT into a mirrored bucket"
}

//
// xputJogger - as mpather
//

func (j *xputJogger) mountpathInfo() *fs.MountpathInfo { return j.mpathInfo }
func (j *xputJogger) post(lom *cluster.LOM)            { j.workCh <- lom }

func (j *xputJogger) stop() {
	for lom := range j.workCh {
		glog.Infof("Stopping, not copying %s", lom)
		j.parent.DecPending()
	}
	j.stopCh <- struct{}{}
	close(j.stopCh)
}

//
// xputJogger - main
//
func (j *xputJogger) jog() {
	j.workCh = make(chan *cluster.LOM, j.parent.mirror.Burst)
	j.stopCh = make(chan struct{}, 1)
	j.buf = j.parent.slab.Alloc()
	j.parent.wg.Done()
	glog.Infof("xputJogger[%s] started", j.mpathInfo)
loop:
	for {
		select {
		case lom := <-j.workCh:
			j.addCopy(lom)
			j.parent.DecPending() // to support action renewal on-demand
		case <-j.stopCh:
			break loop
		}
	}
	j.parent.slab.Free(j.buf)
}

func (j *xputJogger) addCopy(lom *cluster.LOM) {
	cluster.ObjectLocker.Lock(lom.Uname(), false)
	defer cluster.ObjectLocker.Unlock(lom.Uname(), false)

	if err := copyTo(lom, j.mpathInfo, j.buf); err != nil {
		glog.Errorln(err)
	} else {
		if glog.V(4) {
			glog.Infof("copied %s/%s %s=>%s", lom.Bucket, lom.Objname, lom.ParsedFQN.MpathInfo, j.mpathInfo)
		}
		if v := j.parent.ObjectsInc(); (v % logNumProcessed) == 0 {
			glog.Infof("%s: total~=%d, copied=%d", j.parent.String(), j.parent.total, v)
		}
		j.parent.BytesAdd(lom.Size())
	}
}
