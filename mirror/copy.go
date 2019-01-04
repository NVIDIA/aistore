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

const (
	workChanCap = 256
)

type (
	XactCopy struct {
		// implements cmn.Xact a cmn.Runner interfaces
		cmn.XactDemandBase
		cmn.Named
		// runtime
		workCh  chan *cluster.LOM
		joggers map[string]*jogger
		// init
		Bucket   string
		Slab     *memsys.Slab2
		T        cluster.Target
		Bislocal bool
	}
	jogger struct { // one per mountpath
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

func (r *XactCopy) ReqAddMountpath(mpath string)     { cmn.Assert(false, "NIY") } // TODO
func (r *XactCopy) ReqRemoveMountpath(mpath string)  { cmn.Assert(false, "NIY") }
func (r *XactCopy) ReqEnableMountpath(mpath string)  { cmn.Assert(false, "NIY") }
func (r *XactCopy) ReqDisableMountpath(mpath string) { cmn.Assert(false, "NIY") }

//
// public methods
//

// - runs on a per- mirrored bucket basis
// - dispatches replication requests for execution by one of the dedicated "joggers"
// - ref-counts pending requests and self-terminates when idle for a while
func (r *XactCopy) Run() error {
	// init
	availablePaths, _ := fs.Mountpaths.Get()
	r.init(len(availablePaths))

	// start mpath joggers
	for mpath, mpathInfo := range availablePaths {
		var (
			mpathLC string
			jogger  = &jogger{parent: r, mpathInfo: mpathInfo}
		)
		if r.Bislocal {
			mpathLC = fs.Mountpaths.MakePathLocal(mpath, fs.ObjectType)
		} else {
			mpathLC = fs.Mountpaths.MakePathCloud(mpath, fs.ObjectType)
		}
		r.joggers[mpathLC] = jogger
		go jogger.jog()
	}

	// control loop
	for {
		select {
		case lom := <-r.workCh:
			if jogger := r.loadBalance(lom); jogger != nil {
				jogger.workCh <- lom
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

func (r *XactCopy) init(l int) {
	r.workCh = make(chan *cluster.LOM, workChanCap)
	r.joggers = make(map[string]*jogger, l)
}

// =================== load balancing and self-throttling ========================
// Load balancing decision must (... TODO ...) be configurable and a function of:
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

func (r *XactCopy) loadBalance(lom *cluster.LOM) (jogger *jogger) {
	var max float32 = 100
	for _, j := range r.joggers {
		if j.mpathInfo.Path == lom.ParsedFQN.MpathInfo.Path {
			continue
		}
		// TODO: minimize or eliminate the cases when replication
		//       destination happens to be busier than the source;
		//       throttle via delay or rescheduling
		if _, curr := j.mpathInfo.GetIOstats(fs.StatDiskUtil); curr.Max < max {
			jogger = j
			max = curr.Max
		}
	}
	return
}

func (r *XactCopy) stop() {
	if r.Finished() {
		glog.Warningf("%s is (already) not running", r)
		return
	}
	r.EndTime(time.Now())
	r.XactDemandBase.Stop()
	for _, jogger := range r.joggers {
		jogger.stop()
	}
	for lom := range r.workCh {
		glog.Infof("Stopping, not copying %s", lom)
		r.DecPending()
	}
}

//
// mpath jogger
//
func (j *jogger) stop() {
	for lom := range j.workCh {
		glog.Infof("Stopping, not copying %s", lom)
		j.parent.DecPending()
	}
	j.stopCh <- struct{}{}
	close(j.stopCh)
}

func (j *jogger) jog() {
	j.workCh = make(chan *cluster.LOM, workChanCap)
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

// TODO: 1) handle errors
//       4) throttle
//       5) versioned updates vs outdated replicas
//       6) elsewhere: fix LRU from evicting; support reduction num replicas for symmetry
//       7) verbose log
//       8) target's removeBuckets() won't work
//       9) forbid setting bucket.copies back to zero - see proxy.go
func (j *jogger) mirror(lom *cluster.LOM) {
	var (
		cpyfqn       string
		parsedCpyFQN = lom.ParsedFQN
	)
	parsedCpyFQN.MpathInfo = j.mpathInfo
	workfqn := fs.CSM.GenContentParsedFQN(parsedCpyFQN, fs.WorkfileType, fs.WorkfilePut)
	if err := cmn.CopyFile(lom.Fqn, workfqn, j.buf); err != nil {
		return
	}
	if glog.V(4) {
		glog.Infof("Copied %s => workfile %s", lom, workfqn)
	}
	cpyfqn = fs.CSM.FQN(j.mpathInfo.Path, lom.ParsedFQN.ContentType, lom.Bislocal, lom.Bucket, lom.Objname)
	if err := cmn.MvFile(workfqn, cpyfqn); err != nil {
		glog.Errorln(err)
		goto fail
	}
	if errstr := fs.SetXattr(lom.Fqn, cmn.XattrCopies, []byte(cpyfqn)); errstr != "" {
		glog.Errorln(errstr)
		goto fail
	}
	lom.CopyFQN = cpyfqn
	if errstr := fs.SetXattr(lom.CopyFQN, cmn.XattrCopies, []byte(lom.Fqn)); errstr != "" {
		glog.Errorln(errstr)
	}
	if glog.V(3) {
		glog.Infof("Copied %s => %s", lom, j.mpathInfo)
	}
	return
fail:
	if errRemove := os.Remove(workfqn); errRemove != nil {
		glog.Errorf("Failed to remove %s, err: %v", workfqn, errRemove)
		j.parent.T.FSHC(errRemove, workfqn)
	}
}
