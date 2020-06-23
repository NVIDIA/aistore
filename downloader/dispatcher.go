// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
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
	initInfoStore(parent.t.GetDB()) // it will be initialized only once
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
		d.waitFor(job)
		job.cleanup()
		d.cleanUpAborted(job.ID())
	}()

	if aborted := d.checkAborted(); aborted || d.checkAbortedJob(job) {
		return !aborted
	}

	var (
		diffResolver = NewDiffResolver(nil)
	)

	diffResolver.Start()

	// In case of `!job.Sync()` we don't want to traverse the whole bucket.
	// We just want to download requested objects so we know exactly which
	// objects must be checked (compared) and which not. Therefore, only traverse
	// bucket when we need to sync the objects.
	if job.Sync() {
		go func() {
			defer diffResolver.CloseSrc()

			err := fs.WalkBck(&fs.WalkBckOptions{
				Options: fs.Options{
					Bck: job.Bck(),
					CTs: []string{fs.ObjectType},
					Callback: func(fqn string, de fs.DirEntry) error {
						if diffResolver.Stopped() {
							return cmn.NewAbortedError("stopped")
						}
						lom := &cluster.LOM{T: d.parent.t, FQN: fqn}
						if err := lom.Init(job.Bck()); err != nil {
							return err
						}
						if !job.checkObj(lom.ObjName) {
							return nil
						}
						diffResolver.PushSrc(lom)
						return nil
					},
					Sorted: true,
				},
			})
			if err != nil && !errors.As(err, &cmn.AbortedError{}) {
				diffResolver.Abort(err)
			}
		}()
	}

	go func() {
		defer func() {
			diffResolver.CloseDst()
			if !job.Sync() {
				diffResolver.CloseSrc()
			}
		}()

		for {
			objs, ok := job.genNext()
			if !ok || diffResolver.Stopped() {
				return
			}

			for _, obj := range objs {
				if d.checkAborted() {
					err := cmn.NewAbortedError("dispatcher")
					diffResolver.Abort(err)
					return
				} else if d.checkAbortedJob(job) {
					diffResolver.Stop()
					return
				}

				if !job.Sync() {
					// When it is not a sync job, push LOM for a given object
					// because we need to check if it exists.
					lom := &cluster.LOM{T: d.parent.t, ObjName: obj.objName}
					if err := lom.Init(job.Bck()); err != nil {
						diffResolver.Abort(err)
						return
					}
					diffResolver.PushSrc(lom)
				}

				if obj.link != "" {
					diffResolver.PushDst(&WebResource{
						ObjName: obj.objName,
						Link:    obj.link,
					})
				} else {
					diffResolver.PushDst(&CloudResource{
						ObjName: obj.objName,
					})
				}
			}
		}
	}()

	for {
		result, err := diffResolver.Next()
		if err != nil {
			return false
		}
		switch result.Action {
		case DiffResolverRecv, DiffResolverSkip, DiffResolverErr, DiffResolverDelete:
			var obj dlObj
			if dst := result.Dst; dst != nil {
				obj = dlObj{
					objName:   dst.ObjName,
					link:      dst.Link,
					fromCloud: dst.Link == "",
				}
			} else {
				src := result.Src
				cmn.Assert(result.Action == DiffResolverDelete)
				obj = dlObj{
					objName:   src.ObjName,
					link:      "",
					fromCloud: true,
				}
			}

			dlStore.incScheduled(job.ID())

			if result.Action == DiffResolverSkip {
				dlStore.incSkipped(job.ID())
				continue
			}

			t := &singleObjectTask{
				parent: d.parent,
				obj:    obj,
				job:    job,
			}

			if result.Action == DiffResolverErr {
				t.markFailed(err.Error())
				continue
			}

			if result.Action == DiffResolverDelete {
				cmn.Assert(job.Sync())
				if err := d.parent.t.EvictObject(result.Src); err != nil {
					t.markFailed(err.Error())
				} else {
					dlStore.incFinished(job.ID())
				}
				continue
			}

			err, ok := d.blockingDispatchDownloadSingle(t)
			if err != nil {
				glog.Errorf("Download job %s failed, couldn't download object %s, aborting; %s", job.ID(), obj.link, err.Error())
				dlStore.setAborted(job.ID())
				return ok
			}
			if !ok {
				return false
			}
		case DiffResolverSend:
			cmn.Assert(job.Sync())
		case DiffResolverEOF:
			dlStore.setAllDispatched(job.ID(), true)
			return true
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

// returns false if dispatcher encountered hard error, true otherwise
func (d *dispatcher) blockingDispatchDownloadSingle(task *singleObjectTask) (err error, ok bool) {
	bck := cluster.NewBckEmbed(task.job.Bck())
	if err := bck.Init(d.parent.t.GetBowner(), d.parent.t.Snode()); err != nil {
		return err, true
	}

	mi, _, err := cluster.HrwMpath(bck.MakeUname(task.obj.objName))
	if err != nil {
		return err, false
	}
	jogger, ok := d.joggers[mi.Path]
	if !ok {
		err := fmt.Errorf("no jogger for mpath %s exists", mi.Path)
		return err, false
	}

	// NOTE: Throttle job before making jogger busy - we don't want to clog the
	//  jogger as other tasks from other jobs can be already ready to download.
	task.job.throttler().acquire()

	// Firstly, check if the job was aborted when we were sleeping.
	if d.checkAbortedJob(task.job) {
		return nil, true
	}

	// Secondly, try to push the new task into queue.
	select {
	// FIXME: if this particular jogger is full, but others are available, dispatcher
	//  will wait with dispatching all of the requests anyway
	case jogger.putCh(task) <- task:
		return nil, true
	case <-d.jobAbortedCh(task.job.ID()).Listen():
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

	dlStore.setAborted(req.id)
	req.writeResp(nil)
}

func (d *dispatcher) dispatchStatus(req *request) {
	jInfo, err := d.parent.checkJob(req)
	if err != nil || jInfo == nil {
		return
	}

	currentTasks := d.activeTasks(req.id)
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

func (d *dispatcher) activeTasks(reqID string) []TaskDlInfo {
	d.RLock()
	currentTasks := make([]TaskDlInfo, 0, len(d.joggers))
	for _, j := range d.joggers {
		task := j.getTask()
		if task != nil && task.id() == reqID {
			currentTasks = append(currentTasks, task.ToTaskDlInfo())
		}
	}
	d.RUnlock()

	sort.Sort(TaskInfoByName(currentTasks))
	return currentTasks
}

// pending returns `true` if any joggers has pending tasks for a given `reqID`,
// `false` otherwise.
func (d *dispatcher) pending(reqID string) bool {
	d.RLock()
	defer d.RUnlock()
	for _, j := range d.joggers {
		if j.pending(reqID) {
			return true
		}
	}
	return false
}

func (d *dispatcher) waitFor(job DlJob) {
	// PRECONDITION: all tasks should be dispatched.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if !d.pending(job.ID()) {
			break
		}
	}
}
