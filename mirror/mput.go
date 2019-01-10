// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"os"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/memsys"
)

type (
	XactCopy struct {
		// implements cmn.Xact a cmn.Runner interfaces
		cmn.XactDemandBase
		cmn.Named
		// runtime
		workCh        chan *cluster.LOM
		mpathChangeCh chan struct{}
		copiers       map[string]*copier
		// init
		Bucket   string
		Mirror   cmn.MirrorConf
		Slab     *memsys.Slab2
		T        cluster.Target
		Bislocal bool
	}
	copier struct { // one per mountpath
		parent    *XactCopy
		mpathInfo *fs.MountpathInfo
		workCh    chan *cluster.LOM
		stopCh    chan struct{}
		buf       []byte
	}
)

/*
 * implements fs.PathRunner interface
 */
var _ fs.PathRunner = &XactCopy{}

func (r *XactCopy) SetID(id int64) { cmn.Assert(false) }

func (r *XactCopy) ReqAddMountpath(mpath string)     { r.mpathChangeCh <- struct{}{} } // TODO: same for other "copiers"
func (r *XactCopy) ReqRemoveMountpath(mpath string)  { r.mpathChangeCh <- struct{}{} }
func (r *XactCopy) ReqEnableMountpath(mpath string)  { r.mpathChangeCh <- struct{}{} }
func (r *XactCopy) ReqDisableMountpath(mpath string) { r.mpathChangeCh <- struct{}{} }

//
// public methods
//

// - runs on a per-mirrored bucket basis
// - dispatches replication requests to a dedicated mountpath copier
// - ref-counts pending requests and self-terminates when idle for a while
func (r *XactCopy) Init() {
	availablePaths, _ := fs.Mountpaths.Get()
	r.workCh = make(chan *cluster.LOM, r.Mirror.MirrorBurst)
	r.mpathChangeCh = make(chan struct{}, 1)
	r.copiers = make(map[string]*copier, len(availablePaths))
}

func (r *XactCopy) Run() error {
	availablePaths, _ := fs.Mountpaths.Get()
outer:
	for {
		// start mpath copiers
		for _, mpathInfo := range availablePaths {
			copier := &copier{parent: r, mpathInfo: mpathInfo}
			mpathLC := mpathInfo.MakePath(fs.ObjectType, r.Bislocal)
			r.copiers[mpathLC] = copier
			go copier.jog()
		}
		// control loop
		for {
			select {
			case lom := <-r.workCh:
				cmn.Assert(r.Mirror.MirrorOptimizeRead, cmn.NotSupported)
				// [throttle] when the optimization objective is read load balancing (rather than
				// data redundancy), we drop to make sure senders won't block on the workCh
				if pending := r.Pending(); r.Mirror.MirrorBurst > 1 && pending >= r.Mirror.MirrorBurst {
					glog.Errorf("pending=%d, burst=%d - dropping %s", pending, r.Mirror.MirrorBurst, lom)
					break
				}
				// load balance
				if copier := r.loadBalance(lom); copier != nil {
					copier.workCh <- lom
				}
			case <-r.ChanCheckTimeout():
				if r.Timeout() {
					r.stop()
					return nil
				}
			case <-r.ChanAbort():
				r.stop()
				return fmt.Errorf("%s aborted, exiting", r)
			case <-r.mpathChangeCh:
				for _, copier := range r.copiers {
					copier.stop()
				}
				availablePaths, _ = fs.Mountpaths.Get()
				l := len(availablePaths)
				if l == 0 {
					r.stop()
					return fmt.Errorf("%s no mountpaths, exiting", r)
				}
				r.copiers = make(map[string]*copier, l) // new copiers map
				continue outer                          // reinitialize and keep running
			}
		}
	}
}

// main method: replicate a given locally stored object
func (r *XactCopy) Copy(lom *cluster.LOM) (err error) {
	r.IncPending() // ref-count via base to support on-demand action
	if r.Finished() {
		err = cmn.NewErrXpired("Cannot replicate: " + r.String())
		return
	}
	r.workCh <- lom
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
	var util = cmn.PairF32{100, 100}
	for _, j := range r.copiers {
		if j.mpathInfo.Path == lom.ParsedFQN.MpathInfo.Path {
			continue
		}
		// TODO: minimize or eliminate the cases when replication
		//       destination happens to be busier than the source;
		//       throttle via delay or rescheduling
		if _, curr := j.mpathInfo.GetIOstats(fs.StatDiskUtil); curr.Max < util.Max {
			copier = j
			util = curr
		}
	}
	// [throttle] when the optimization objective is read load balancing (rather than
	// data redundancy), we start dropping requests at high utilizations of local filesystems
	if util.Max >= float32(lom.Config.Xaction.DiskUtilHighWM) && util.Min > float32(lom.Config.Xaction.DiskUtilLowWM) {
		glog.Errorf("utilization %s - dropping %s", util, lom)
		copier = nil
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
	j.workCh = make(chan *cluster.LOM, j.parent.Mirror.MirrorBurst)
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

// TODO: - versioned updates
//       - disable active mirror via bucket props
func (j *copier) mirror(lom *cluster.LOM) {
	// copy
	var (
		cpyfqn       string
		parsedCpyFQN = lom.ParsedFQN
	)
	parsedCpyFQN.MpathInfo = j.mpathInfo
	workfqn := fs.CSM.GenContentParsedFQN(parsedCpyFQN, fs.WorkfileType, fs.WorkfilePut)
	if err := lom.CopyObject(workfqn, j.buf); err != nil {
		return
	}
	if glog.V(4) {
		glog.Infof("Copied %s => workfile %s", lom, workfqn)
	}
	cpyfqn = fs.CSM.FQN(j.mpathInfo, lom.ParsedFQN.ContentType, lom.Bislocal, lom.Bucket, lom.Objname)
	if err := cmn.MvFile(workfqn, cpyfqn); err != nil {
		glog.Errorln(err)
		goto fail
	}
	if errstr := lom.SetXcopy(cpyfqn); errstr != "" {
		glog.Errorln(errstr)
	} else if glog.V(3) {
		glog.Infof("Copied %s => %s", lom, j.mpathInfo)
	}
	return
fail:
	if errRemove := os.Remove(workfqn); errRemove != nil {
		glog.Errorf("Failed to remove %s, err: %v", workfqn, errRemove)
		j.parent.T.FSHC(errRemove, workfqn)
	}
}
