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

func (d *dispatcher) removeJogger(mpath string) {
	jogger, ok := d.joggers[mpath]
	if !ok {
		glog.Errorf("Invalid mountpath %q", mpath)
		return
	}

	delete(d.joggers, mpath)
	jogger.stop()
}

/*
 * dispatcher's dispatch methods (forwards request to jogger)
 */

// TODO: these methods should be called as go routine in the future
// for now, they're just synchronous calls, incremental steps...

func (d *dispatcher) dispatchDownload(t *task) {
	var (
		resp      response
		added, ok bool
		j         *jogger
	)
	lom, errstr := cluster.LOM{T: d.parent.t, Bucket: t.bucket, Objname: t.obj.Objname, BucketProvider: t.bckProvider}.Init()
	if errstr == "" {
		_, errstr = lom.Load(true)
	}
	if errstr != "" {
		resp.err, resp.statusCode = errors.New(errstr), http.StatusInternalServerError
		goto finalize
	}
	if lom.Exists() { // FIXME: add versioning
		resp.resp = fmt.Sprintf("object %q already exists - skipping", t.obj.Objname)
		goto finalize
	}
	t.fqn = lom.FQN

	if lom.ParsedFQN.MpathInfo == nil {
		err := fmt.Errorf("download task with %v failed. Failed to get mountpath for the request's fqn %s", t, t.fqn)
		glog.Error(err.Error())
		resp.err, resp.statusCode = err, http.StatusInternalServerError
		goto finalize
	}
	j, ok = d.joggers[lom.ParsedFQN.MpathInfo.Path]
	cmn.AssertMsg(ok, fmt.Sprintf("no mpath exists for %v", t))

	resp, added = j.enqueue(t)

finalize:
	t.write(resp.resp, resp.err, resp.statusCode)

	// Following can happen:
	//  * in case of error
	//  * object already exists
	//  * object already in queue
	//
	// In all of these cases we need to decrease pending since it was not added to the queue.
	if !added {
		d.parent.DecPending()
	}
}

func (d *dispatcher) dispatchRemove(req *request) {
	body, err := d.parent.checkJob(req)
	if err != nil {
		return
	}

	// There's a slight chance this doesn't happen if target rejoins after target checks for download not running
	if d.parent.getNumPending(body) != 0 {
		req.writeErrResp(fmt.Errorf("download job with id = %s is still running", body.ID), http.StatusBadRequest)
		return
	}

	err = d.parent.db.delJob(req.id)
	cmn.AssertNoErr(err) // Everything should be okay since getReqFromDB
	req.writeResp(nil)
}

func (d *dispatcher) dispatchAbort(req *request) {
	body, err := d.parent.checkJob(req)
	if err != nil {
		return
	}

	errs := make([]response, 0, len(body.Objs))
	for _, j := range d.joggers {
		j.Lock()
		for _, obj := range body.Objs {
			req.obj = obj
			req.bucket = body.Bucket
			req.bckProvider = body.BckProvider

			lom, errstr := cluster.LOM{T: d.parent.t, Bucket: req.bucket, Objname: req.obj.Objname}.Init()
			if errstr == "" {
				_, errstr = lom.Load(false)
			}
			if errstr != "" {
				errs = append(errs, response{
					err:        errors.New(errstr),
					statusCode: http.StatusInternalServerError,
				})
				continue
			}
			if lom.Exists() {
				continue
			}

			// Abort currently running task
			if j.task != nil && j.task.request.equals(req) {
				// Task is running
				j.task.cancel()
				continue
			}

			// If not running but in queue we need to decrease number of pending
			if existed := j.q.delete(req); existed {
				d.parent.DecPending()
			}
		}
		j.Unlock()
	}

	if len(errs) > 0 {
		r := errs[0] // TODO: we should probably print all errors
		req.writeErrResp(r.err, r.statusCode)
		return
	}

	err = d.parent.db.setAborted(req.id)
	cmn.AssertNoErr(err) // Everything should be okay since getReqFromDB
	req.writeResp(nil)
}

func (d *dispatcher) dispatchStatus(req *request) {
	body, err := d.parent.checkJob(req)
	if err != nil {
		return
	}

	total := len(body.Objs)
	finished := 0

	errs := make([]response, 0, len(body.Objs))
	for _, obj := range body.Objs {
		lom, errstr := cluster.LOM{T: d.parent.t, Bucket: body.Bucket, Objname: obj.Objname, BucketProvider: body.BckProvider}.Init()
		if errstr == "" {
			_, errstr = lom.Load(true)
		}
		if errstr != "" {
			errs = append(errs, response{
				err:        errors.New(errstr),
				statusCode: http.StatusInternalServerError,
			})
			continue
		}
		if lom.Exists() { // FIXME: checksum, etc.
			finished++
			continue
		}

		req.fqn = lom.FQN

		if lom.ParsedFQN.MpathInfo == nil {
			err := fmt.Errorf("status request with %v failed. Failed to obtain mountpath for request's fqn %s", req, req.fqn)
			glog.Error(err.Error())
			errs = append(errs, response{
				err:        err,
				statusCode: http.StatusInternalServerError,
			})
			continue
		}
		_, ok := d.joggers[lom.ParsedFQN.MpathInfo.Path]
		cmn.AssertMsg(ok, fmt.Sprintf("status request with %v failed. No corresponding mpath exists", req))
	}

	if len(errs) > 0 {
		r := errs[0] // TODO: we should probably print all errors
		req.writeErrResp(r.err, r.statusCode)
		return
	}

	currentTasks := d.parent.activeTasks(req.id)
	finishedTasks, err := d.parent.db.getTasks(req.id)
	if err != nil {
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}

	numPending := d.parent.getNumPending(body)

	dlErrors, err := d.parent.db.getErrors(req.id)
	if err != nil {
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}
	sort.Sort(cmn.TaskErrByName(dlErrors))

	req.writeResp(cmn.DlStatusResp{
		Finished:      finished,
		Total:         total,
		CurrentTasks:  currentTasks,
		FinishedTasks: finishedTasks,
		Errs:          dlErrors,

		NumPending: numPending,
		Aborted:    body.Aborted,
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
			NumPending:  d.parent.getNumPending(&r),
			Aborted:     r.Aborted,
		}
	}
	req.writeResp(respMap)
}
