// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"golang.org/x/sync/errgroup"
)

// Dispatcher serves as middle layer between receiving download requests
// and serving them to joggers which actually download objects from a cloud.

type (
	dispatcher struct {
		parent *Downloader

		joggers  map[string]*jogger     // mpath -> jogger
		abortJob map[string]*cmn.StopCh // jobID -> abort job chan

		dispatchDownloadCh chan DlJob

		stopCh *cmn.StopCh
		sync.RWMutex
	}
)

func newDispatcher(parent *Downloader) *dispatcher {
	initInfoStore() // it will be initialized only once
	return &dispatcher{
		parent:  parent,
		joggers: make(map[string]*jogger, 8),

		dispatchDownloadCh: make(chan DlJob, jobsChSize),
		stopCh:             cmn.NewStopCh(),
		abortJob:           make(map[string]*cmn.StopCh, jobsChSize),
	}
}

func (d *dispatcher) init() {
	availablePaths, _ := d.parent.mountpaths.Get()
	for mpath := range availablePaths {
		d.addJogger(mpath)
	}

	go d.run()
}

func (d *dispatcher) run() {
	var (
		// Number of concurrent job dispatches - it basically limits the number
		// of goroutines so they won't go out of hand.
		sema       = cmn.NewDynSemaphore(5 * fs.Mountpaths.NumAvail())
		group, ctx = errgroup.WithContext(context.Background())
	)
	for {
		select {
		case <-ctx.Done():
			d.stop()
			group.Wait()
			return
		case job := <-d.dispatchDownloadCh:
			// Start dispatching each job in new goroutine to make sure that
			// all joggers are busy downloading the tasks (jobs with limits
			// may not saturate the full downloader throughput).
			sema.Acquire()
			group.Go(func() error {
				defer sema.Release()
				if !d.dispatchDownload(job) {
					return cmn.NewAbortedError("dispatcher")
				}
				return nil
			})
		case <-d.stopCh.Listen():
			d.stop()
			return
		}
	}
}

func (d *dispatcher) Abort() {
	d.stopCh.Close()
}

// stop running joggers
// no need to cleanup maps, dispatcher should not be used after stop()
func (d *dispatcher) stop() {
	for _, jogger := range d.joggers {
		jogger.stop()
	}
}

func (d *dispatcher) addJogger(mpath string) {
	if _, ok := d.joggers[mpath]; ok {
		glog.Warningf("Attempted to add an already existing mountpath %q", mpath)
		return
	}
	mpathInfo, _ := d.parent.mountpaths.Path2MpathInfo(mpath)
	if mpathInfo == nil {
		glog.Errorf("Attempted to add a mountpath %q with no corresponding filesystem", mpath)
		return
	}
	j := newJogger(d, mpath)
	go j.jog()
	d.joggers[mpath] = j
}

func (d *dispatcher) cleanUpAborted(jobID string) {
	d.Lock()
	if ch, exists := d.abortJob[jobID]; exists {
		ch.Close()
		delete(d.abortJob, jobID)
	}
	d.Unlock()
}

func (d *dispatcher) ScheduleForDownload(job DlJob) {
	d.Lock()
	d.abortJob[job.ID()] = cmn.NewStopCh()
	d.Unlock()

	d.dispatchDownloadCh <- job
}

/*
 * dispatcher's dispatch methods (forwards request to jogger)
 */

func (d *dispatcher) dispatchDownload(job DlJob) (ok bool) {
	defer func() {
		if err := dlStore.markFinished(job.ID()); err != nil {
			glog.Error(err)
		}
		dlStore.flush(job.ID())
		d.cleanUpAborted(job.ID())
	}()

	if aborted := d.checkAborted(); aborted || d.checkAbortedJob(job) {
		return !aborted
	}

	for {
		objs, ok := job.genNext()
		if !ok {
			if err := dlStore.setAllDispatched(job.ID(), true); err != nil {
				glog.Error(err)
			}
			return true
		}

		for _, obj := range objs {
			if aborted := d.checkAborted(); aborted || d.checkAbortedJob(job) {
				return !aborted
			}

			err, ok := d.blockingDispatchDownloadSingle(job, obj)
			if err != nil {
				glog.Errorf("Download job %s failed, couldn't download object %s, aborting; %s", job.ID(), obj.link, err.Error())
				cmn.AssertNoErr(dlStore.setAborted(job.ID()))
				return ok
			}
			if !ok {
				return false
			}
		}
	}
}

func (d *dispatcher) jobAbortedCh(jobID string) *cmn.StopCh {
	d.RLock()
	defer d.RUnlock()
	if abCh, ok := d.abortJob[jobID]; ok {
		return abCh
	}

	// Channel always sending something if entry in the map is missing.
	abCh := cmn.NewStopCh()
	abCh.Close()
	return abCh
}

func (d *dispatcher) checkAbortedJob(job DlJob) bool {
	select {
	case <-d.jobAbortedCh(job.ID()).Listen():
		return true
	default:
		return false
	}
}

func (d *dispatcher) checkAborted() bool {
	select {
	case <-d.stopCh.Listen():
		return true
	default:
		return false
	}
}

func (d *dispatcher) createTasksLom(job DlJob, obj dlObj) (*cluster.LOM, error) {
	lom := &cluster.LOM{T: d.parent.t, ObjName: obj.objName}
	err := lom.Init(job.Bck())
	if err == nil {
		err = lom.Load()
	}
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		equal, err := compareObjects(obj, lom)
		if err != nil {
			return nil, err
		}
		if equal {
			if glog.V(4) {
				glog.Infof("object %q already exists and seems to match remote - skipping", obj.objName)
			}
			return nil, nil
		}
		glog.Warningf("object %q already exists but does not match with remote - overriding", obj.objName)
	}

	if lom.ParsedFQN.MpathInfo == nil {
		err = fmt.Errorf("download task for %s failed. Failed to get mountpath for the request's fqn %s", obj.link, lom.FQN)
		glog.Error(err)
		return nil, err
	}
	return lom, nil
}

func (d *dispatcher) prepareTask(job DlJob, obj dlObj) (*singleObjectTask, *jogger, error) {
	t := &singleObjectTask{
		parent: d.parent,
		obj:    obj,
		job:    job,
	}

	lom, err := d.createTasksLom(job, obj)
	if err != nil {
		glog.Warningf("error in handling downloader request: %v", err)
		d.parent.statsT.Add(stats.ErrDownloadCount, 1)

		dlStore.persistError(job.ID(), obj.objName, err.Error())
		// NOTE: do not propagate single task errors
		return nil, nil, nil
	}

	if lom == nil {
		// object already exists
		err = dlStore.incFinished(job.ID())
		return nil, nil, err
	}

	j, ok := d.joggers[lom.ParsedFQN.MpathInfo.Path]
	cmn.AssertMsg(ok, fmt.Sprintf("no jogger for mpath %s exists for %v", lom.ParsedFQN.MpathInfo.Path, t))
	return t, j, nil
}

// returns false if dispatcher was aborted in the meantime, true otherwise
func (d *dispatcher) blockingDispatchDownloadSingle(job DlJob, obj dlObj) (err error, ok bool) {
	if err := dlStore.incScheduled(job.ID()); err != nil {
		glog.Error(err)
	}
	task, jogger, err := d.prepareTask(job, obj)
	if err != nil {
		return err, true
	}
	if task == nil || jogger == nil {
		return nil, true
	}

	// NOTE: Throttle job before making jogger busy - we don't want to clog the
	//  jogger as other tasks from other jobs can be already ready to download.
	job.throttler().acquire()

	// Firstly, check if the job was aborted when we were sleeping.
	if d.checkAbortedJob(job) {
		return nil, true
	}

	// Secondly, try to push the new task into queue.
	select {
	// FIXME: if this particular jogger is full, but others are available, dispatcher
	//  will wait with dispatching all of the requests anyway
	case jogger.putCh(task) <- task:
		return nil, true
	case <-d.jobAbortedCh(job.ID()).Listen():
		return nil, true
	case <-d.stopCh.Listen():
		return nil, false
	}
}

func (d *dispatcher) dispatchRemove(req *request) {
	jInfo, err := d.parent.checkJob(req)
	if err != nil {
		return
	}

	// There's a slight chance this doesn't happen if target rejoins after target checks for download not running
	dlInfo := jInfo.ToDlJobInfo()
	if dlInfo.JobRunning() {
		req.writeErrResp(fmt.Errorf("download job with id = %s is still running", jInfo.ID), http.StatusBadRequest)
		return
	}

	dlStore.delJob(req.id)
	req.writeResp(nil)
}

func (d *dispatcher) dispatchAbort(req *request) {
	_, err := d.parent.checkJob(req)
	if err != nil {
		return
	}

	d.jobAbortedCh(req.id).Close()

	for _, j := range d.joggers {
		j.abortJob(req.id)
	}

	err = dlStore.setAborted(req.id)
	cmn.AssertNoErr(err) // Everything should be okay since getReqFromDB
	req.writeResp(nil)
}

func (d *dispatcher) dispatchStatus(req *request) {
	jInfo, err := d.parent.checkJob(req)
	if err != nil || jInfo == nil {
		return
	}

	currentTasks := d.parent.activeTasks(req.id)
	finishedTasks, err := dlStore.getTasks(req.id)
	if err != nil {
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}

	dlErrors, err := dlStore.getErrors(req.id)
	if err != nil {
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}
	sort.Sort(TaskErrByName(dlErrors))

	req.writeResp(DlStatusResp{
		DlJobInfo:     jInfo.ToDlJobInfo(),
		CurrentTasks:  currentTasks,
		FinishedTasks: finishedTasks,
		Errs:          dlErrors,
	})
}

func (d *dispatcher) dispatchList(req *request) {
	records := dlStore.getList(req.regex)
	respMap := make(map[string]DlJobInfo)
	for _, r := range records {
		respMap[r.ID] = r.ToDlJobInfo()
	}

	req.writeResp(respMap)
}
