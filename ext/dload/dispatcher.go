// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact/xreg"
	"golang.org/x/sync/errgroup"
)

// Dispatcher serves as middle layer between receiving download requests
// and serving them to joggers which actually download objects from a remote location.

type (
	dispatcher struct {
		xdl         *Xact
		startupSema startupSema            // Semaphore which synchronizes goroutines at dispatcher startup.
		joggers     map[string]*jogger     // mpath -> jogger
		mtx         sync.RWMutex           // Protects map defined below.
		abortJob    map[string]*cos.StopCh // jobID -> abort job chan
		workCh      chan jobif
		stopCh      *cos.StopCh
		config      *cmn.Config
	}

	startupSema struct {
		started atomic.Bool
	}

	global struct {
		t      cluster.Target
		db     kvdb.Driver
		tstats stats.Tracker
		store  *infoStore

		// Downloader selects one of the two clients (below) by the destination URL.
		// Certification check is disabled for now and does not depend on cluster settings.
		clientH   *http.Client
		clientTLS *http.Client
	}
)

var g global

func Init(t cluster.Target, stats stats.Tracker, db kvdb.Driver, clientConf *cmn.ClientConf) {
	cargs := cmn.TransportArgs{Timeout: clientConf.TimeoutLong.D()}
	g.clientH = cmn.NewClient(cargs)
	g.clientTLS = cmn.NewClientTLS(cargs, cmn.TLSArgs{SkipVerify: true})
	if db == nil { // unit tests only
		debug.Assert(t == nil)
		return
	}

	g.t = t
	g.tstats = stats
	g.store = newInfoStore(db)
	g.db = db
	xreg.RegNonBckXact(&factory{})
}

////////////////
// dispatcher //
////////////////

func newDispatcher(xdl *Xact) *dispatcher {
	return &dispatcher{
		xdl:         xdl,
		startupSema: startupSema{},
		joggers:     make(map[string]*jogger, 8),
		workCh:      make(chan jobif),
		stopCh:      cos.NewStopCh(),
		abortJob:    make(map[string]*cos.StopCh, 100),
		config:      cmn.GCO.Get(),
	}
}

func (d *dispatcher) run() (err error) {
	var (
		// limit the number of concurrent job dispatches (goroutines)
		sema       = cos.NewSemaphore(5 * fs.NumAvail())
		group, ctx = errgroup.WithContext(context.Background())
	)
	availablePaths := fs.GetAvail()
	for mpath := range availablePaths {
		d.addJogger(mpath)
	}
	// allow other goroutines to run
	d.startupSema.markStarted()

	nlog.Infoln(d.xdl.Name(), "started, cnt:", len(availablePaths))
mloop:
	for {
		select {
		case <-d.xdl.IdleTimer():
			nlog.Infoln(d.xdl.Name(), "idle timeout")
			break mloop
		case errCause := <-d.xdl.ChanAbort():
			nlog.Infoln(d.xdl.Name(), "aborted:", errCause)
			break mloop
		case <-ctx.Done():
			break mloop
		case job := <-d.workCh:
			// Start dispatching each job in new goroutine to make sure that
			// all joggers are busy downloading the tasks (jobs with limits
			// may not saturate the full downloader throughput).
			d.mtx.Lock()
			d.abortJob[job.ID()] = cos.NewStopCh()
			d.mtx.Unlock()

			select {
			case <-d.xdl.IdleTimer():
				nlog.Infoln(d.xdl.Name(), "idle timeout")
				break mloop
			case errCause := <-d.xdl.ChanAbort():
				nlog.Infoln(d.xdl.Name(), "aborted:", errCause)
				break mloop
			case <-ctx.Done():
				break mloop
			case <-sema.TryAcquire():
				group.Go(func() error {
					defer sema.Release()
					if !d.dispatchDownload(job) {
						return cmn.NewErrAborted(job.String(), "download", nil)
					}
					return nil
				})
			}
		}
	}

	d.stop()
	return group.Wait()
}

// stop running joggers
// no need to cleanup maps, dispatcher should not be used after stop()
func (d *dispatcher) stop() {
	d.stopCh.Close()
	for _, jogger := range d.joggers {
		jogger.stop()
	}
}

func (d *dispatcher) addJogger(mpath string) {
	_, ok := d.joggers[mpath]
	debug.Assert(!ok)
	j := newJogger(d, mpath)
	go j.jog()
	d.joggers[mpath] = j
}

func (d *dispatcher) cleanupJob(jobID string) {
	d.mtx.Lock()
	if ch, exists := d.abortJob[jobID]; exists {
		ch.Close()
		delete(d.abortJob, jobID)
	}
	d.mtx.Unlock()
}

func (d *dispatcher) finish(job jobif) {
	verbose := d.config.FastV(4, cos.SmoduleDload)
	if verbose {
		nlog.Infof("Waiting for job %q", job.ID())
	}
	d.waitFor(job.ID())
	if verbose {
		nlog.Infof("Job %q finished waiting for all tasks", job.ID())
	}
	d.cleanupJob(job.ID())
	if verbose {
		nlog.Infof("Job %q cleaned up", job.ID())
	}
	job.cleanup()
	if verbose {
		nlog.Infof("Job %q has finished", job.ID())
	}
}

// forward request to designated jogger
func (d *dispatcher) dispatchDownload(job jobif) (ok bool) {
	defer d.finish(job)

	if aborted := d.checkAborted(); aborted || d.checkAbortedJob(job) {
		return !aborted
	}

	diffResolver := NewDiffResolver(&defaultDiffResolverCtx{})
	go diffResolver.Start()

	// In case of `!job.Sync()` we don't want to traverse the whole bucket.
	// We just want to download requested objects so we know exactly which
	// objects must be checked (compared) and which not. Therefore, only traverse
	// bucket when we need to sync the objects.
	if job.Sync() {
		go diffResolver.walk(job)
	}

	go diffResolver.push(job, d)

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
					objName:    dst.ObjName,
					link:       dst.Link,
					fromRemote: dst.Link == "",
				}
			} else {
				src := result.Src
				debug.Assertf(result.Action == DiffResolverDelete, "%d vs %d", result.Action, DiffResolverDelete)
				obj = dlObj{
					objName:    src.ObjName,
					link:       "",
					fromRemote: true,
				}
			}

			g.store.incScheduled(job.ID())

			if result.Action == DiffResolverSkip {
				g.store.incSkipped(job.ID())
				continue
			}

			task := &singleTask{xdl: d.xdl, obj: obj, job: job}
			if result.Action == DiffResolverErr {
				task.markFailed(result.Err.Error())
				continue
			}

			if result.Action == DiffResolverDelete {
				requiresSync := job.Sync()
				debug.Assert(requiresSync)
				if _, err := g.t.EvictObject(result.Src); err != nil {
					task.markFailed(err.Error())
				} else {
					g.store.incFinished(job.ID())
				}
				continue
			}

			ok, err := d.doSingle(task)
			if err != nil {
				nlog.Errorf("%s failed to download %s: %v", job, obj.objName, err)
				g.store.setAborted(job.ID()) // TODO -- FIXME: pass (report, handle) error, here and elsewhere
				return ok
			}
			if !ok {
				g.store.setAborted(job.ID())
				return false
			}
		case DiffResolverSend:
			requiresSync := job.Sync()
			debug.Assert(requiresSync)
		case DiffResolverEOF:
			g.store.setAllDispatched(job.ID(), true)
			return true
		}
	}
}

func (d *dispatcher) jobAbortedCh(jobID string) *cos.StopCh {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	if abCh, ok := d.abortJob[jobID]; ok {
		return abCh
	}

	// Channel always sending something if entry in the map is missing.
	abCh := cos.NewStopCh()
	abCh.Close()
	return abCh
}

func (d *dispatcher) checkAbortedJob(job jobif) bool {
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
func (d *dispatcher) doSingle(task *singleTask) (ok bool, err error) {
	bck := meta.CloneBck(task.job.Bck())
	if err := bck.Init(g.t.Bowner()); err != nil {
		return true, err
	}

	mi, _, err := fs.Hrw(bck.MakeUname(task.obj.objName))
	if err != nil {
		return false, err
	}
	jogger, ok := d.joggers[mi.Path]
	if !ok {
		err := fmt.Errorf("no jogger for mpath %s exists", mi.Path)
		return false, err
	}

	// NOTE: Throttle job before making jogger busy - we don't want to clog the
	//  jogger as other tasks from other jobs can be already ready to download.
	select {
	case <-task.job.throttler().tryAcquire():
		break
	case <-d.jobAbortedCh(task.job.ID()).Listen():
		return true, nil
	}

	// Secondly, try to push the new task into queue.
	select {
	// TODO -- FIXME: currently, dispatcher halts if any given jogger is "full" but others available
	case jogger.putCh(task) <- task:
		return true, nil
	case <-d.jobAbortedCh(task.job.ID()).Listen():
		task.job.throttler().release()
		return true, nil
	case <-d.stopCh.Listen():
		task.job.throttler().release()
		return false, nil
	}
}

func (d *dispatcher) adminReq(req *request) (resp any, statusCode int, err error) {
	if d.config.FastV(4, cos.SmoduleDload) {
		nlog.Infof("Admin request (id: %q, action: %q, onlyActive: %t)", req.id, req.action, req.onlyActive)
	}
	// Need to make sure that the dispatcher has fully initialized and started,
	// and it's ready for processing the requests.
	d.startupSema.waitForStartup()

	switch req.action {
	case actStatus:
		d.handleStatus(req)
	case actAbort:
		d.handleAbort(req)
	case actRemove:
		d.handleRemove(req)
	default:
		debug.Assertf(false, "%v; %v", req, req.action)
	}
	r := req.response
	return r.value, r.statusCode, r.err
}

func (*dispatcher) handleRemove(req *request) {
	dljob, err := g.store.checkExists(req)
	if err != nil {
		return
	}
	job := dljob.clone()
	if job.JobRunning() {
		req.errRsp(fmt.Errorf("job %q is still running", dljob.id), http.StatusBadRequest)
		return
	}
	g.store.delJob(req.id)
	req.okRsp(nil)
}

func (d *dispatcher) handleAbort(req *request) {
	if _, err := g.store.checkExists(req); err != nil {
		return
	}
	d.jobAbortedCh(req.id).Close()
	for _, j := range d.joggers {
		j.abortJob(req.id)
	}
	g.store.setAborted(req.id)
	req.okRsp(nil)
}

func (d *dispatcher) handleStatus(req *request) {
	var (
		finishedTasks []TaskDlInfo
		dlErrors      []TaskErrInfo
	)
	dljob, err := g.store.checkExists(req)
	if err != nil {
		return
	}

	currentTasks := d.activeTasks(req.id)
	if !req.onlyActive {
		finishedTasks, err = g.store.getTasks(req.id)
		if err != nil {
			req.errRsp(err, http.StatusInternalServerError)
			return
		}

		dlErrors, err = g.store.getErrors(req.id)
		if err != nil {
			req.errRsp(err, http.StatusInternalServerError)
			return
		}
		sort.Sort(TaskErrByName(dlErrors))
	}

	req.okRsp(&StatusResp{
		Job:           dljob.clone(),
		CurrentTasks:  currentTasks,
		FinishedTasks: finishedTasks,
		Errs:          dlErrors,
	})
}

func (d *dispatcher) activeTasks(reqID string) []TaskDlInfo {
	currentTasks := make([]TaskDlInfo, 0, len(d.joggers))
	for _, j := range d.joggers {
		task := j.getTask(reqID)
		if task != nil {
			currentTasks = append(currentTasks, task.ToTaskDlInfo())
		}
	}

	sort.Sort(TaskInfoByName(currentTasks))
	return currentTasks
}

// pending returns `true` if any joggers has pending tasks for a given `reqID`,
// `false` otherwise.
func (d *dispatcher) pending(jobID string) bool {
	for _, j := range d.joggers {
		if j.pending(jobID) {
			return true
		}
	}
	return false
}

// PRECONDITION: All tasks should be dispatched.
func (d *dispatcher) waitFor(jobID string) {
	for ; ; time.Sleep(time.Second) {
		if !d.pending(jobID) {
			return
		}
	}
}

/////////////////
// startupSema //
/////////////////

func (ss *startupSema) markStarted() { ss.started.Store(true) }

func (ss *startupSema) waitForStartup() {
	const (
		sleep   = 500 * time.Millisecond
		timeout = 10 * time.Second
		errmsg  = "FATAL: dispatcher takes too much time to start"
	)
	if ss.started.Load() {
		return
	}
	for total := time.Duration(0); !ss.started.Load(); total += sleep {
		time.Sleep(sleep)
		// should never happen even on slowest machines
		debug.Assert(total < timeout, errmsg)
		if total >= timeout && total < timeout+sleep*2 {
			nlog.Errorln(errmsg)
		}
	}
}
