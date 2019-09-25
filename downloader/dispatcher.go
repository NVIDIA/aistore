// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"fmt"
	"net/http"
	"sort"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

// Dispatcher serves as middle layer between receiving download requests
// and serving them to joggers which actually download objects from a cloud.

type (
	dispatcher struct {
		parent *Downloader

		joggers  map[string]*jogger       // mpath -> jogger
		abortJob map[string]chan struct{} //jobID -> abort job chan

		dispatchDownloadCh chan DlJob

		stopCh cmn.StopCh
		sync.RWMutex
	}
)

func newDispatcher(parent *Downloader) *dispatcher {
	return &dispatcher{
		parent:  parent,
		joggers: make(map[string]*jogger, 8),

		dispatchDownloadCh: make(chan DlJob, jobsChSize),
		stopCh:             cmn.NewStopCh(),
		abortJob:           make(map[string]chan struct{}, jobsChSize),
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
	for {
		select {
		case job := <-d.dispatchDownloadCh:
			if !d.dispatchDownload(job) {
				// stop dispatcher if aborted
				d.stop()
				return
			}
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
	delete(d.abortJob, jobID)
	d.Unlock()
}

func (d *dispatcher) ScheduleForDownload(job DlJob) {
	d.Lock()
	d.abortJob[job.ID()] = make(chan struct{}, 1)
	d.Unlock()

	d.dispatchDownloadCh <- job
}

/*
 * dispatcher's dispatch methods (forwards request to jogger)
 */

func (d *dispatcher) dispatchDownload(job DlJob) (ok bool) {
	defer func() {
		dlStore.markFinished(job.ID())
		dlStore.flush(job.ID())
		d.cleanUpAborted(job.ID())
	}()

	if aborted := d.checkAborted(); aborted || d.checkAbortedJob(job) {
		return !aborted
	}

	for {
		objs, ok := job.GenNext()
		if !ok {
			_ = dlStore.setAllDispatched(job.ID(), true)
			return true
		}

		for _, obj := range objs {
			if aborted := d.checkAborted(); aborted || d.checkAbortedJob(job) {
				return !aborted
			}

			err, ok := d.blockingDispatchDownloadSingle(job, obj)
			if err != nil {
				glog.Errorf("Download job %s failed, couldn't download object %s, aborting; %s", job.ID(), obj.Link, err.Error())
				cmn.AssertNoErr(dlStore.setAborted(job.ID()))
				return ok
			}
			if !ok {
				return false
			}
		}
	}
}

func (d *dispatcher) jobAbortedCh(jobID string) <-chan struct{} {
	d.RLock()
	defer d.RUnlock()
	if abCh, ok := d.abortJob[jobID]; ok {
		return abCh
	}

	// chanel always sending something
	// if entry in the map is missing
	abCh := make(chan struct{})
	close(abCh)
	return abCh
}

func (d *dispatcher) checkAbortedJob(job DlJob) bool {
	select {
	case <-d.jobAbortedCh(job.ID()):
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

func (d *dispatcher) createTasksLom(job DlJob, obj cmn.DlObj) (*cluster.LOM, error) {
	lom := &cluster.LOM{T: d.parent.t, Objname: obj.Objname}
	err := lom.Init(job.Bucket(), job.Provider())
	if err == nil {
		err = lom.Load()
	}
	if err != nil {
		return nil, err
	}
	if lom.Exists() { // FIXME: add versioning
		if glog.V(4) {
			glog.Infof("object %q already exists - skipping", obj.Objname)
		}
		return nil, nil
	}

	if lom.ParsedFQN.MpathInfo == nil {
		err = fmt.Errorf("download task for %s failed. Failed to get mountpath for the request's fqn %s", obj.Link, lom.FQN)
		glog.Error(err)
		return nil, err
	}

	return lom, nil
}

func (d *dispatcher) prepareTask(job DlJob, obj cmn.DlObj) (*singleObjectTask, *jogger, error) {
	t := &singleObjectTask{
		parent: d.parent,
		request: &request{
			action:   taskDownload,
			id:       job.ID(),
			obj:      obj,
			bucket:   job.Bucket(),
			provider: job.Provider(),
			timeout:  job.Timeout(),
		},
		finishedCh: make(chan error, 1),
	}

	lom, err := d.createTasksLom(job, obj)
	if err != nil {
		glog.Warningf("error in handling downloader request: %s", err.Error())
		d.parent.stats.Add(stats.ErrDownloadCount, 1)

		dbErr := dlStore.persistError(t.id, t.obj.Objname, err.Error())
		cmn.AssertNoErr(dbErr)
		return nil, nil, err
	}

	if lom == nil {
		// object already exists
		return nil, nil, nil
	}

	t.fqn = lom.FQN
	j, ok := d.joggers[lom.ParsedFQN.MpathInfo.Path]
	cmn.AssertMsg(ok, fmt.Sprintf("no mpath exists for %v", t))
	return t, j, nil
}

// returns false if dispatcher was aborted in the meantime, true otherwise
func (d *dispatcher) blockingDispatchDownloadSingle(job DlJob, obj cmn.DlObj) (err error, ok bool) {
	_ = dlStore.incScheduled(job.ID())
	task, jogger, err := d.prepareTask(job, obj)
	if err != nil {
		return err, true
	}
	if task == nil || jogger == nil {
		err = dlStore.incFinished(job.ID())
		return err, true
	}

	select {
	// FIXME: if this particular jogger is full, but others are available, dispatcher
	// will wait with dispatching all of the requests anyway
	case jogger.putCh(task) <- task:
		return nil, true
	case <-d.jobAbortedCh(job.ID()):
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
	if !d.parent.Aborted() && d.parent.getNumPending(jInfo.ID) != 0 {
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

	for _, j := range d.joggers {
		j.Lock()

		// Abort currently running task, if belongs to a given job
		if j.task != nil && j.task.request.id == req.id {
			// Task is running
			j.task.cancel()
		}

		// Remove all pending tasks from queue
		if m, ok := j.q.m[req.id]; ok {
			d.parent.SubPending(int64(len(m)))
			delete(j.q.m, req.id)
		}

		j.Unlock()
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

	numPending := d.parent.getNumPending(jInfo.ID)

	dlErrors, err := dlStore.getErrors(req.id)
	if err != nil {
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}
	sort.Sort(cmn.TaskErrByName(dlErrors))

	req.writeResp(cmn.DlStatusResp{
		Finished:      int(jInfo.FinishedCnt.Load()),
		Total:         jInfo.Total,
		CurrentTasks:  currentTasks,
		FinishedTasks: finishedTasks,
		Errs:          dlErrors,

		Pending:       numPending,
		Aborted:       jInfo.Aborted.Load(),
		AllDispatched: jInfo.AllDispatched.Load(),
		Scheduled:     int(jInfo.ScheduledCnt.Load()),
	})
}

func (d *dispatcher) dispatchList(req *request) {
	records := dlStore.getList(req.regex)
	respMap := make(map[string]cmn.DlJobInfo)
	for _, r := range records {
		respMap[r.ID] = cmn.DlJobInfo{
			ID:          r.ID,
			Description: r.Description,
			NumErrors:   int(r.ErrorCnt.Load()),
			NumPending:  d.parent.getNumPending(r.ID),
			Aborted:     r.Aborted.Load(),
		}
	}

	req.writeResp(respMap)
}
