// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"errors"
	"fmt"
	"net/http"
	"sort"

	"github.com/NVIDIA/aistore/3rdparty/glog"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

// Dispatcher serves as middle layer between receiving download requests
// and serving them to joggers which actually download objects from a cloud.
// At the moment this layer is unnecessary, but soon it will become important
// It's purpose will be to asynchronously feed joggers with new objects
// to download, when joggers have available resources, instead of flooding them
// with multiple tasks, as it's done at the moment

type (
	dispatcher struct {
		parent  *Downloader
		joggers map[string]*jogger // mpath -> jogger
	}
)

func newDispatcher(parent *Downloader) *dispatcher {
	return &dispatcher{
		parent:  parent,
		joggers: make(map[string]*jogger, 8),
	}
}

func (d *dispatcher) init() {
	availablePaths, _ := d.parent.mountpaths.Get()
	for mpath := range availablePaths {
		d.addJogger(mpath)
	}
}

// TODO: in future add stopping all go routines
// which are still in progress
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

/*
 * dispatcher's dispatch methods (forwards request to jogger)
 */

// TODO: methods below should be called as go routine in the future

// TODO: handle canceling job, removing mpaths, errors etc which happened in the middle of dispatchDownload
// this should be taken care of especially when dispatchDownload will be run as goroutine
// TODO: make this method smarter: don't just put everything into jogger, but wait until
// it has resources to fulfill the request (meaning that there is space in jogger's download queue)
func (d *dispatcher) dispatchDownload(job DownloadJob) {
	for {
		objs, ok := job.GenNext()
		if !ok {
			return
		}

		for _, obj := range objs {
			if err := d.dispatchDownloadSingle(job, obj); err != nil {
				glog.Errorf("Download job %s failed, couldn't download object %s, aborting; %s", job.ID(), obj.Link, err.Error())
				cmn.AssertNoErr(d.parent.db.setAborted(job.ID()))
				return
			}
		}

	}
}

func (d *dispatcher) dispatchDownloadSingle(job DownloadJob, obj cmn.DlObj) error {
	var (
		err error
		ok  bool
		j   *jogger
	)

	t := &singleObjectTask{
		parent: d.parent,
		request: &request{
			action:      taskDownload,
			id:          job.ID(),
			obj:         obj,
			bucket:      job.Bucket(),
			bckProvider: job.BckProvider(),
			timeout:     job.Timeout(),
		},
		finishedCh: make(chan error, 1),
	}

	lom, errstr := cluster.LOM{T: d.parent.t, Bucket: t.bucket, Objname: t.obj.Objname, BucketProvider: t.bckProvider}.Init()
	if errstr == "" {
		_, errstr = lom.Load(true)
	}
	if errstr != "" {
		err = errors.New(errstr)
		goto finalize
	}
	if lom.Exists() { // FIXME: add versioning
		if glog.V(4) {
			glog.Infof("object %q already exists - skipping", t.obj.Objname)
		}
		goto finalize
	}
	t.fqn = lom.FQN

	if lom.ParsedFQN.MpathInfo == nil {
		err = fmt.Errorf("download task with %v failed. Failed to get mountpath for the request's fqn %s", t, t.fqn)
		glog.Error(err.Error())
		goto finalize
	}
	j, ok = d.joggers[lom.ParsedFQN.MpathInfo.Path]
	cmn.AssertMsg(ok, fmt.Sprintf("no mpath exists for %v", t))

	err = j.enqueue(t)

finalize:
	if err != nil {
		glog.Warningf("error in handling downloader request: %s", err.Error())
		d.parent.stats.Add(stats.ErrDownloadCount, 1)

		dbErr := t.parent.db.addError(t.id, t.obj.Objname, err.Error())
		cmn.AssertNoErr(dbErr)
	}

	return err
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

	err = d.parent.db.delJob(req.id)
	cmn.AssertNoErr(err) // Everything should be okay since getReqFromDB
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

	err = d.parent.db.setAborted(req.id)
	cmn.AssertNoErr(err) // Everything should be okay since getReqFromDB
	req.writeResp(nil)
}

func (d *dispatcher) dispatchStatus(req *request) {
	jInfo, err := d.parent.checkJob(req)
	if err != nil || jInfo == nil {
		return
	}

	currentTasks := d.parent.activeTasks(req.id)
	finishedTasks, err := d.parent.db.getTasks(req.id)
	if err != nil {
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}

	numPending := d.parent.getNumPending(jInfo.ID)

	dlErrors, err := d.parent.db.getErrors(req.id)
	if err != nil {
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}
	sort.Sort(cmn.TaskErrByName(dlErrors))

	req.writeResp(cmn.DlStatusResp{
		Finished:      jInfo.Finished,
		Total:         jInfo.Total,
		CurrentTasks:  currentTasks,
		FinishedTasks: finishedTasks,
		Errs:          dlErrors,

		NumPending: numPending,
		Aborted:    jInfo.Aborted,
	})
}

func (d *dispatcher) dispatchList(req *request) {
	records, err := d.parent.db.getList(req.regex)
	if err != nil {
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}

	respMap := make(map[string]cmn.DlJobInfo)
	for _, r := range records {
		respMap[r.ID] = cmn.DlJobInfo{
			ID:          r.ID,
			Description: r.Description,
			NumPending:  d.parent.getNumPending(r.ID),
			Aborted:     r.Aborted,
		}
	}
	req.writeResp(respMap)
}
