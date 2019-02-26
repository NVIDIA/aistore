// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	XactCopy struct {
		// implements cmn.Xact a cmn.Runner interfaces
		cmn.XactDemandBase
		// runtime
		workCh  chan *cluster.LOM
		copiers map[string]*copier
		// init
		Mirror                 cmn.MirrorConf
		Slab                   *memsys.Slab2
		T                      cluster.Target
		Namelocker             cluster.NameLocker
		wg                     *sync.WaitGroup
		total, dropped, copied int64
		BckIsLocal             bool
	}
	copier struct { // one per mountpath
		parent    *XactCopy
		mpathInfo *fs.MountpathInfo
		workCh    chan *cluster.LOM
		stopCh    chan struct{}
		buf       []byte
	}
)

//
// public methods
//

// - runs on a per-mirrored bucket basis
// - dispatches replication requests to a dedicated mountpath copier
// - ref-counts pending requests and self-terminates when idle for a while
func (r *XactCopy) InitAndRun() error {
	availablePaths, _ := fs.Mountpaths.Get()
	l := len(availablePaths)
	if err := checkErrNumMp(r, l); err != nil {
		return err
	}
	r.workCh = make(chan *cluster.LOM, r.Mirror.Burst)
	r.copiers = make(map[string]*copier, l)
	r.wg = &sync.WaitGroup{}
	r.wg.Add(1)
	go r.Run()
	for _, mpathInfo := range availablePaths {
		copier := &copier{parent: r, mpathInfo: mpathInfo}
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.BckIsLocal)
		r.copiers[mpathLC] = copier
		r.wg.Add(1)
		go copier.jog()
	}
	r.wg.Wait() // wait for all to start
	return nil
}

func (r *XactCopy) Run() error {
	glog.Infoln(r.String())
	r.wg.Done()
	for {
		select {
		case lom := <-r.workCh:
			// load balance
			if copier := r.loadBalance(lom); copier != nil {
				if glog.V(4) {
					glog.Infof("%s=>%s", lom.ParsedFQN.MpathInfo, copier.mpathInfo)
				}
				copier.workCh <- lom
			} else {
				glog.Errorf("%s: failed to load balance (drop type 1)", lom)
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

// main method: replicate a given locally stored object
func (r *XactCopy) Copy(lom *cluster.LOM) (err error) {
	if r.Finished() {
		err = cmn.NewErrXpired("Cannot replicate: " + r.String())
		return
	}
	r.total++
	// [throttle]
	// when the optimization objective is write perf,
	// we start dropping requests to make sure callers don't block
	pending, max := r.Pending(), r.Mirror.Burst
	if r.Mirror.OptimizePUT {
		if pending > 1 && pending >= max {
			r.dropped++
			if (r.dropped % logNumDropped) == 0 {
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
		if pending >= max-max/8 && !r.Mirror.OptimizePUT {
			time.Sleep(cmn.ThrottleSleepMax)
		} else if pending > max-max/4 {
			time.Sleep((cmn.ThrottleSleepAvg + cmn.ThrottleSleepMax) / 2)
		} else if pending > max/2 {
			time.Sleep(cmn.ThrottleSleepAvg)
		}
	}
	return
}

func (r *XactCopy) Stop(error) { r.Abort() } // call base method

//
// private methods
//
// =================== load balancing and self-throttling ========================
// Generally,
// load balancing decision must (... TODO ...) be configurable and a function of:
// - current utilization (%) of the filesystem's disks;
// - current disk queue lengths and their respective minimums and maximums during
//   the reporting period (config.Periodic.IostatTime);
// - previous values of the same, and their corresponding averages.
//
// Further, load balancers must take into account relative priorities of
// other workloads that are simultaneously present in the system -
// and self-throttle accordingly. E.g., in most cases we'd want GET to have the
// top (default, configurable) priority which would mean that the filesystems that
// serve GETs are even less available for other extended actions than otherwise, etc.
// =================== load balancing and self-throttling ========================

func (r *XactCopy) loadBalance(lom *cluster.LOM) (copier *copier) {
	var util = cmn.PairF32{101, 101}
	for _, j := range r.copiers {
		if j.mpathInfo.Path == lom.ParsedFQN.MpathInfo.Path {
			continue
		}
		if _, curr := j.mpathInfo.GetIOstats(fs.StatDiskUtil); curr.Max < util.Max {
			copier = j
			util = curr
		}
	}
	return
}

func (r *XactCopy) stop() {
	if r.Finished() {
		glog.Warningf("%s is (already) not running", r)
		return
	}
	r.XactDemandBase.Stop()
	for _, copier := range r.copiers {
		copier.stop()
	}
	r.EndTime(time.Now())
	for lom := range r.workCh {
		glog.Infof("Stopping, not copying %s", lom)
		r.DecPending()
	}
}

//
// mpath copier
//
func (j *copier) stop() {
	for lom := range j.workCh {
		glog.Infof("Stopping, not copying %s", lom)
		j.parent.DecPending()
	}
	j.stopCh <- struct{}{}
	close(j.stopCh)
}

func (j *copier) jog() {
	glog.Infof("copier[%s] started", j.mpathInfo)
	j.parent.wg.Done()
	j.workCh = make(chan *cluster.LOM, j.parent.Mirror.Burst)
	j.stopCh = make(chan struct{}, 1)
	j.buf = j.parent.Slab.Alloc()
loop:
	for {
		select {
		case lom := <-j.workCh:
			j.mirror(lom)
			j.parent.DecPending() // to support action renewal on-demand
		case <-j.stopCh:
			break loop
		}
	}
	j.parent.Slab.Free(j.buf)
}

func (j *copier) mirror(lom *cluster.LOM) {
	// copy
	j.parent.Namelocker.Lock(lom.Uname, false)
	defer j.parent.Namelocker.Unlock(lom.Uname, false)

	lom.ParsedFQN.MpathInfo = j.mpathInfo
	workFQN := lom.GenFQN(fs.WorkfileType, fs.WorkfilePut)
	if err := lom.CopyObject(workFQN, j.buf); err != nil {
		return
	}
	cpyFQN := fs.CSM.FQN(j.mpathInfo, lom.ParsedFQN.ContentType, lom.BckIsLocal, lom.Bucket, lom.Objname)
	if glog.V(4) {
		glog.Infof("Copied %s => workfile %s, cpyFQN %s", lom, workFQN, cpyFQN)
	}
	if err := cmn.MvFile(workFQN, cpyFQN); err != nil {
		glog.Errorln(err)
		goto fail
	}
	if errstr := lom.SetXcopy(cpyFQN); errstr != "" {
		glog.Errorln(errstr)
	} else {
		if glog.V(4) {
			glog.Infof("copied %s/%s %s=>%s", lom.Bucket, lom.Objname, lom.ParsedFQN.MpathInfo, j.mpathInfo)
		}
		if v := atomic.AddInt64(&j.parent.copied, 1); (v % logNumCopied) == 0 {
			glog.Infof("%s: total~=%d, copied=%d", j.parent.String(), j.parent.total, v)
		}
	}
	return
fail:
	if errRemove := os.Remove(workFQN); errRemove != nil {
		glog.Errorf("Failed to remove %s, err: %v", workFQN, errRemove)
		j.parent.T.FSHC(errRemove, workFQN)
	}
}
