// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

// =============================== Summary ====================================
//
// Downloader is a long running task that provides a AIS a means to download
// objects from the internet by providing a URL(referred to as a link) to the
// server where the object exists. Downloader does not make the HTTP GET
// requests to download the files itself- it purely manages the lifecycle of
// joggers and dispatches any download related request to the correct jogger
// instance.
//
// ====== API ======
//
// API exposed to the rest of the code includes the following operations:
//   * Run      - to run
//   * Stop     - to stop
//   * Download    - to download a new object from a URL
//   * Cancel      - to cancel a previously requested download (currently queued or currently downloading)
//   * Status      - to request the status of a previously requested download
// The Download, Cancel and Status requests are encapsulated into an internal
// request object and added to a request queue and then are dispatched to the
// correct jogger. The remaining operations are private to the Downloader and
// are used only internally.
//
// Each jogger, which corresponds to a mountpath, has a download channel
// (downloadCh) where download request that are dispatched from Downloader are
// queued. Thus, downloads occur on a per-mountpath basis and are handled one at
// a time by jogger as they arrive.
//
// ====== Downloading ======
//
// After Downloader receives a download request, adds it to the correct jogger's queue.
// Downloads are represented as extended actions (xactDownload), and there is at
// most one active xactDownload assigned to any jogger at any given time. The
// xactDownload are created when the jogger dequeues a download request from its
// downloadCh and are destroyed when the download is cancelled, finished or
// fails.
//
// After a xactDownload is created, a separate goroutine is spun up to make the
// actual GET request (jogger's download method). The goroutine for the jogger
// sits idle awaiting an abort(failed or cancelled) or finish message from the
// goroutine responsible for the actual download.
//
// ====== Cancelling ======
//
// When Downloader receives a cancel request, it cancels running task or
// if the task is scheduled but is not yet processed, then it is removed
// from queue (see: put, get). If the task is running, cancelFunc is
// invoked to cancel task's request.
//
// ====== Status Updates ======
//
// Status updates are made possible by progressReader, which just overwrites the
// io.Reader's Read method to additionally notify a Reporter Func, that gets
// notified the number of bytes that have been read every time we read from the
// response body from the HTTP GET request we make to to the link to download
// the object.
//
// When Downloader receives a status update request, it dispatches to a separate
// jogger goroutine that checks if the downloaded completed. Otherwise it checks
// if it is currently being downloaded. If it is being currently downloaded, it
// returns the progress. Otherwise, it returns that the object hasn't been
// downloaded yet. Now, the file may never be downloaded if the download was
// never queued to the downloadCh.
//
// Status updates are either reported in terms of size or size and percentage.
// Before downloading an object from a server, we attempt to make a HEAD request
// to obtain the object size using the "Content-Length" field returned in the
// Header. Note: not all servers implement a HEAD request handler. For these
// cases, a progress percentage is not returned, just the current number of
// bytes that have been downloaded.
//
// ====== Notes ======
//
// Downloader assumes that any type of download request is first sent to a proxy
// and then redirected to the correct target's Downloader (the proxy uses the
// HRW algorithm to determine the target). It is not possible to directly hit a
// Target's download endpoint to force an object to be downloaded to that
// Target, all request must go through a proxy first.
//
// ================================ Summary ====================================

const (
	adminRemove  = "REMOVE"
	adminCancel  = "CANCEL"
	adminStatus  = "STATUS"
	adminList    = "LIST"
	taskDownload = "DOWNLOAD"
	queueChSize  = 200

	putInQueueTimeout = time.Second * 10
)

var (
	httpClient = &http.Client{
		Timeout: cmn.GCO.Get().Timeout.DefaultLong,
	}
)

// public types
type (
	// Downloader implements the fs.PathRunner and XactDemand interface. When
	// download related requests are made to AIS using the download endpoint,
	// Downloader dispatches these requests to the corresponding jogger.
	Downloader struct {
		cmn.NamedID
		cmn.XactDemandBase

		t          cluster.Target
		mountpaths *fs.MountedFS
		stats      stats.Tracker

		mpathReqCh chan fs.ChangeReq
		adminCh    chan *request
		downloadCh chan *task
		joggers    map[string]*jogger // mpath -> jogger

		db *downloaderDB
	}
)

// private types
type (
	// The result of calling one of Downloader's exposed methos is encapsulated
	// in a response object, which is used to communicate the outcome of the
	// request.
	response struct {
		resp       interface{}
		err        error
		statusCode int
	}

	// Calling Downloader's exposed methods results in the creation of a request
	// for admin related tasks (i.e. cancelling and status updates) and a dlTask
	// for a downlad request. These objects are used by Downloader to process
	// the request, and are then dispatched to the correct jogger to be handled.
	request struct {
		action      string         // one of: adminCancel, adminList, adminStatus, taskDownload
		id          string         // id of the job task
		regex       *regexp.Regexp // regex of descriptions to return if id is empty
		obj         cmn.DlObj
		bucket      string
		bckProvider string
		timeout     string
		fqn         string         // fqn of the object after it has been committed
		responseCh  chan *response // where the outcome of the request is written
	}

	// task embeds cmn.XactBase, but it is not part of the targetrunner's xactions member
	// variable. Instead, Downloader manages the lifecycle of the extended action.
	task struct {
		parent *Downloader
		*request

		started time.Time
		ended   time.Time

		currentSize atomic.Int64 // the current size of the file (updated as the download progresses)
		totalSize   int64        // the total size of the file (nonzero only if Content-Length header was provided by the source of the file)
		finishedCh  chan error   // when a jogger finishes downloading a dlTask

		downloadCtx context.Context    // context with cancel function
		cancelFunc  context.CancelFunc // used to cancel the download after the request commences
	}

	queue struct {
		sync.RWMutex
		ch chan *task // for pending downloads
		m  map[string]struct{}
	}

	// Each jogger corresponds to a mpath. All types of download requests
	// corresponding to the jogger's mpath are forwarded to the jogger. Joggers
	// exist in the Downloader's jogger member variable, and run only when there
	// are dlTasks.
	jogger struct {
		mpath       string
		terminateCh chan struct{} // synchronizes termination
		parent      *Downloader

		q *queue

		sync.Mutex
		// lock protected
		task      *task // currently running download task
		stopAgent bool
	}

	progressReader struct {
		r        io.Reader
		reporter func(n int64)
	}
)

//==================================== Requests ===========================================

func (req *request) String() (str string) {
	str += fmt.Sprintf("id: %q, objname: %q, link: %q, from_cloud: %v, ", req.id, req.obj.Objname, req.obj.Link, req.obj.FromCloud)
	if req.bucket != "" {
		str += fmt.Sprintf("bucket: %q (provider: %q), ", req.bucket, req.bckProvider)
	}

	return "{" + strings.TrimSuffix(str, ", ") + "}"
}

func (req *request) uid() string {
	return fmt.Sprintf("%s|%s|%s|%v", req.obj.Link, req.bucket, req.obj.Objname, req.obj.FromCloud)
}

func (req *request) write(resp interface{}, err error, statusCode int) {
	req.responseCh <- &response{
		resp:       resp,
		err:        err,
		statusCode: statusCode,
	}
	close(req.responseCh)
}

func (req *request) writeErrResp(err error, statusCode int) {
	req.write(nil, err, statusCode)
}

func (req *request) writeResp(resp interface{}) {
	req.write(resp, nil, http.StatusOK)
}

func (req *request) equals(rhs *request) bool {
	return req.uid() == rhs.uid()
}

// ========================== progressReader ===================================

var _ io.ReadCloser = &progressReader{}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	pr.reporter(int64(n))
	return
}

func (pr *progressReader) Close() error {
	pr.r = nil
	pr.reporter = nil
	return nil
}

// ============================= Downloader ====================================
/*
 * Downloader implements the fs.PathRunner interface
 */

var _ fs.PathRunner = &Downloader{}

func (d *Downloader) ReqAddMountpath(mpath string)     { d.mpathReqCh <- fs.MountpathAdd(mpath) }
func (d *Downloader) ReqRemoveMountpath(mpath string)  { d.mpathReqCh <- fs.MountpathRem(mpath) }
func (d *Downloader) ReqEnableMountpath(mpath string)  {}
func (d *Downloader) ReqDisableMountpath(mpath string) {}

func (d *Downloader) addJogger(mpath string) {
	if _, ok := d.joggers[mpath]; ok {
		glog.Warningf("Attempted to add an already existing mountpath %q", mpath)
		return
	}
	mpathInfo, _ := d.mountpaths.Path2MpathInfo(mpath)
	if mpathInfo == nil {
		glog.Errorf("Attempted to add a mountpath %q with no corresponding filesystem", mpath)
		return
	}
	j := d.newJogger(mpath)
	go j.jog()
	d.joggers[mpath] = j
}

func (d *Downloader) removeJogger(mpath string) {
	jogger, ok := d.joggers[mpath]
	if !ok {
		glog.Errorf("Invalid mountpath %q", mpath)
		return
	}

	delete(d.joggers, mpath)
	jogger.stop()
}

/*
 * Downloader constructors
 */
func NewDownloader(t cluster.Target, stats stats.Tracker, f *fs.MountedFS, id int64, kind string) (d *Downloader, err error) {
	db, err := newDownloadDB()
	if err != nil {
		return nil, err
	}
	return &Downloader{
		XactDemandBase: *cmn.NewXactDemandBase(id, kind, "" /* no bucket */, false),
		t:              t,
		stats:          stats,
		mountpaths:     f,
		mpathReqCh:     make(chan fs.ChangeReq, 1),
		adminCh:        make(chan *request),
		downloadCh:     make(chan *task),
		joggers:        make(map[string]*jogger, 8),
		db:             db,
	}, nil
}

func (d *Downloader) newJogger(mpath string) *jogger {
	return &jogger{
		mpath:       mpath,
		parent:      d,
		q:           newQueue(),
		terminateCh: make(chan struct{}, 1),
	}
}

func (d *Downloader) init() {
	availablePaths, disabledPaths := d.mountpaths.Get()
	for mpath := range availablePaths {
		d.addJogger(mpath)
	}
	for mpath := range disabledPaths {
		d.addJogger(mpath)
	}
}

func (d *Downloader) Run() error {
	glog.Infof("Starting %s", d.Getname())
	d.t.GetFSPRG().Reg(d)
	d.init()
Loop:
	for {
		select {
		case req := <-d.adminCh:
			switch req.action {
			case adminStatus:
				d.dispatchStatus(req)
			case adminCancel:
				d.dispatchCancel(req)
			case adminRemove:
				d.dispatchRemove(req)
			case adminList:
				d.dispatchList(req)
			default:
				cmn.AssertFmt(false, req, req.action)
			}
		case task := <-d.downloadCh:
			d.dispatchDownload(task)
		case mpathRequest := <-d.mpathReqCh:
			switch mpathRequest.Action {
			case fs.Add:
				d.addJogger(mpathRequest.Path)
			case fs.Remove:
				d.removeJogger(mpathRequest.Path)
			}
		case <-d.ChanCheckTimeout():
			if d.Timeout() {
				glog.Infof("%s has timed out. Exiting...", d.Getname())
				break Loop
			}
		case <-d.ChanAbort():
			glog.Infof("%s has been aborted. Exiting...", d.Getname())
			break Loop
		}
	}
	d.Stop(nil)
	return nil
}

// Stop terminates the downloader
func (d *Downloader) Stop(err error) {
	d.t.GetFSPRG().Unreg(d)
	d.XactDemandBase.Stop()
	for _, jogger := range d.joggers {
		jogger.stop()
	}
	d.EndTime(time.Now())
	glog.Infof("Stopped %s", d.Getname())
}

/*
 * Downloader's exposed methods
 */

func (d *Downloader) Download(body *cmn.DlBody) (resp interface{}, err error, statusCode int) {
	d.IncPending()
	defer d.DecPending()

	if err := d.db.setJob(body.ID, body); err != nil {
		return err.Error(), err, http.StatusInternalServerError
	}

	responses := make([]chan *response, 0, len(body.Objs))
	for _, obj := range body.Objs {
		// Invariant: either there was an error before adding to queue, or file
		// was deleted from queue (on cancel or successful run). In both cases
		// we decrease pending.
		d.IncPending()

		rch := make(chan *response, 1)
		responses = append(responses, rch)
		t := &task{
			parent: d,
			request: &request{
				action:      taskDownload,
				id:          body.ID,
				obj:         obj,
				bucket:      body.Bucket,
				bckProvider: body.BckProvider,
				timeout:     body.Timeout,
				responseCh:  rch,
			},
			finishedCh: make(chan error, 1),
		}
		d.downloadCh <- t
	}

	for _, response := range responses {
		// await the response
		r := <-response
		if r.err != nil {
			d.Cancel(body.ID) // cancel whole job
			return r.resp, r.err, r.statusCode
		}
	}
	return nil, nil, http.StatusOK
}

func (d *Downloader) Cancel(id string) (resp interface{}, err error, statusCode int) {
	d.IncPending()
	req := &request{
		action:     adminCancel,
		id:         id,
		responseCh: make(chan *response, 1),
	}
	d.adminCh <- req

	// await the response
	r := <-req.responseCh
	d.DecPending()
	return r.resp, r.err, r.statusCode
}

func (d *Downloader) Remove(id string) (resp interface{}, err error, statusCode int) {
	d.IncPending()
	req := &request{
		action:     adminRemove,
		id:         id,
		responseCh: make(chan *response, 1),
	}
	d.adminCh <- req

	// await the response
	r := <-req.responseCh
	d.DecPending()
	return r.resp, r.err, r.statusCode
}

func (d *Downloader) Status(id string) (resp interface{}, err error, statusCode int) {
	d.IncPending()
	req := &request{
		action:     adminStatus,
		id:         id,
		responseCh: make(chan *response, 1),
	}
	d.adminCh <- req

	// await the response
	r := <-req.responseCh
	d.DecPending()
	return r.resp, r.err, r.statusCode
}

func (d *Downloader) List(regex *regexp.Regexp) (resp interface{}, err error, statusCode int) {
	d.IncPending()
	req := &request{
		action:     adminList,
		regex:      regex,
		responseCh: make(chan *response, 1),
	}
	d.adminCh <- req

	// await the response
	r := <-req.responseCh
	d.DecPending()
	return r.resp, r.err, r.statusCode
}

/*
 * Downloader's dispatch methods (forwards request to jogger)
 */

func (d *Downloader) dispatchDownload(t *task) {
	var (
		resp      response
		added, ok bool
		j         *jogger
	)
	lom, errstr := cluster.LOM{T: d.t, Bucket: t.bucket, Objname: t.obj.Objname, BucketProvider: t.bckProvider}.Init()
	if errstr == "" {
		_, errstr = lom.Load(true)
	}
	if errstr != "" {
		resp.err, resp.statusCode = errors.New(errstr), http.StatusInternalServerError
		goto finalize
	}
	if lom.Exists() { // FIXME: checksum, etc.
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

	resp, added = j.putIntoDownloadQueue(t)

finalize:
	t.write(resp.resp, resp.err, resp.statusCode)

	// Following can happen:
	//  * in case of error
	//  * object already exists
	//  * object already in queue
	//
	// In all of these cases we need to decrease pending since it was not added
	// to the queue.
	if !added {
		d.DecPending()
	}
}

func (d *Downloader) checkJob(req *request) (*cmn.DlBody, error) {
	body, err := d.db.getJob(req.id)
	if err != nil {
		if err == errJobNotFound {
			req.writeErrResp(fmt.Errorf("download job with id %q has not been found", req.id), http.StatusNotFound)
			return nil, err
		}
		req.writeErrResp(err, http.StatusInternalServerError)
		return nil, err
	}
	return body, nil
}

func (d *Downloader) dispatchRemove(req *request) {
	body, err := d.checkJob(req)
	if err != nil {
		return
	}

	// There's a slight chance this doesn't happen if target rejoins after target checks for download not running
	cmn.Assert(d.getNumPending(body) == 0)

	err = d.db.delJob(req.id)
	cmn.AssertNoErr(err) // everything should be okay since getReqFromDB
	req.writeResp(nil)
}

func (d *Downloader) dispatchCancel(req *request) {
	body, err := d.checkJob(req)
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

			lom, errstr := cluster.LOM{T: d.t, Bucket: req.bucket, Objname: req.obj.Objname}.Init()
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

			// Cancel currently running task
			if j.task != nil && j.task.request.equals(req) {
				// Task is running
				j.task.cancel()
				continue
			}

			// If not running but in queue we need to decrease number of pending
			if existed := j.q.delete(req); existed {
				d.DecPending()
			}
		}
		j.Unlock()
	}

	if len(errs) > 0 {
		r := errs[0] // TODO: we should probably print all errors
		req.writeErrResp(r.err, r.statusCode)
		return
	}

	err = d.db.setCancelled(req.id)
	cmn.AssertNoErr(err) // everything should be okay since getReqFromDB
	req.writeResp(nil)
}

func (d *Downloader) dispatchStatus(req *request) {
	body, err := d.checkJob(req)
	if err != nil {
		return
	}

	total := len(body.Objs)
	finished := 0

	errs := make([]response, 0, len(body.Objs))
	for _, obj := range body.Objs {
		lom, errstr := cluster.LOM{T: d.t, Bucket: body.Bucket, Objname: obj.Objname, BucketProvider: body.BckProvider}.Init()
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

	currentTasks := d.activeTasks(req.id)
	finishedTasks, err := d.db.getTasks(req.id)
	if err != nil {
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}

	numPending := d.getNumPending(body)

	dlErrors, err := d.db.getErrors(req.id)
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
		Cancelled:  body.Cancelled,
	})
}

func (d *Downloader) activeTasks(reqID string) []cmn.TaskDlInfo {
	currentTasks := make([]cmn.TaskDlInfo, 0, len(d.joggers))

	for _, j := range d.joggers {
		j.Lock()

		task := j.task
		if task != nil && task.id == reqID {
			info := cmn.TaskDlInfo{
				Name:       task.obj.Objname,
				Downloaded: task.currentSize.Load(),
				Total:      task.totalSize,

				StartTime: task.started,
				EndTime:   task.ended,

				Running: true,
			}
			currentTasks = append(currentTasks, info)
		}

		j.Unlock()
	}

	sort.Sort(cmn.TaskInfoByName(currentTasks))
	return currentTasks
}

// gets the number of pending files of reqId
func (d *Downloader) getNumPending(body *cmn.DlBody) int {
	numPending := 0

	for _, j := range d.joggers {
		for _, obj := range body.Objs {
			req := request{
				id:          body.ID,
				obj:         obj,
				bucket:      body.Bucket,
				bckProvider: body.BckProvider,
			}
			if j.q.exists(&req) {
				numPending++
			}
		}
	}

	return numPending
}

func (d *Downloader) dispatchList(req *request) {
	records, err := d.db.getList(req.regex)
	if err != nil {
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}

	respMap := make(map[string]cmn.DlJobInfo)
	for _, r := range records {
		respMap[r.ID] = cmn.DlJobInfo{
			ID:          r.ID,
			Description: r.Description,
			NumPending:  d.getNumPending(&r),
			Cancelled:   r.Cancelled,
		}
	}
	req.writeResp(respMap)
}

//==================================== jogger =====================================

func (j *jogger) putIntoDownloadQueue(task *task) (response, bool) {
	cmn.Assert(task != nil)
	added, err, errCode := j.q.put(task)
	if err != nil {
		return response{
			err:        err,
			statusCode: errCode,
		}, false
	}

	return response{
		resp: fmt.Sprintf("Download request %s added to queue", task),
	}, added
}

func (j *jogger) jog() {
	glog.Infof("Starting jogger for mpath %q.", j.mpath)
Loop:
	for {
		t := j.q.get()
		if t == nil {
			break Loop
		}
		j.Lock()
		if j.stopAgent {
			j.Unlock()
			break Loop
		}

		j.task = t
		j.Unlock()

		// start download
		go t.download()

		// await abort or completion
		<-t.waitForFinish()

		j.Lock()
		j.task.persist()
		j.task = nil
		j.Unlock()
		if exists := j.q.delete(t.request); exists {
			j.parent.DecPending()
		}
	}

	j.q.cleanup()
	j.terminateCh <- struct{}{}
}

// Stop terminates the jogger
func (j *jogger) stop() {
	glog.Infof("Stopping jogger for mpath: %s", j.mpath)
	j.q.stop()

	j.Lock()
	j.stopAgent = true
	if j.task != nil {
		j.task.abort(internalErrorMessage(), errors.New("stopped jogger"))
	}
	j.Unlock()

	<-j.terminateCh
}

func (t *task) download() {
	var (
		statusMsg string
		err       error
	)
	lom, errstr := cluster.LOM{T: t.parent.t, Bucket: t.bucket, Objname: t.obj.Objname}.Init()
	if errstr == "" {
		_, errstr = lom.Load(true)
	}
	if errstr != "" {
		t.abort(internalErrorMessage(), errors.New(errstr))
		return
	}
	if lom.Exists() {
		t.abort(internalErrorMessage(), errors.New("object with the same bucket and objname already exists"))
		return
	}

	if glog.V(4) {
		glog.Infof("Starting download for %v", t)
	}

	t.started = time.Now()
	if !t.obj.FromCloud {
		statusMsg, err = t.downloadLocal(lom)
	} else {
		statusMsg, err = t.downloadCloud(lom)
	}
	t.ended = time.Now()

	if err != nil {
		t.abort(statusMsg, err)
		return
	}

	t.parent.stats.AddMany(
		stats.NamedVal64{Name: stats.DownloadSize, Val: t.currentSize.Load()},
		stats.NamedVal64{Name: stats.DownloadLatency, Val: int64(time.Since(t.started))},
	)
	t.finishedCh <- nil
}

func (t *task) downloadLocal(lom *cluster.LOM) (string, error) {
	postFQN := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)

	// create request
	httpReq, err := http.NewRequest(http.MethodGet, t.obj.Link, nil)
	if err != nil {
		return "", err
	}

	requestWithContext := httpReq.WithContext(t.downloadCtx)
	response, err := httpClient.Do(requestWithContext)
	if err != nil {
		return httpClientErrorMessage(err.(*url.Error)), err // error returned by httpClient.Do() is a *url.Error
	}
	if response.StatusCode >= http.StatusBadRequest {
		return httpRequestErrorMessage(t.obj.Link, response), fmt.Errorf("status code: %d", response.StatusCode)
	}

	// Create a custom reader to monitor progress every time we read from response body stream
	progressReader := &progressReader{
		r: response.Body,
		reporter: func(n int64) {
			t.currentSize.Add(n)
		},
	}

	t.setTotalSize(response)

	cksum := getCksum(t.obj.Link, response)
	if err := t.parent.t.Receive(postFQN, progressReader, lom, cluster.ColdGet, cksum); err != nil {
		return internalErrorMessage(), err
	}
	return "", nil
}

func (t *task) setTotalSize(resp *http.Response) {
	totalSize := resp.ContentLength
	if totalSize > 0 {
		t.totalSize = totalSize
	}
}

func (t *task) downloadCloud(lom *cluster.LOM) (string, error) {
	if errstr, _ := t.parent.t.GetCold(t.downloadCtx, lom, true /* prefetch */); errstr != "" {
		return internalErrorMessage(), errors.New(errstr)
	}
	return "", nil
}

func (t *task) cancel() {
	t.cancelFunc()
}

// TODO: this should also inform somehow downloader status about being aborted/canceled
// Probably we need to extend the persistent database (db.go) so that it will contain
// also information about specific tasks.
func (t *task) abort(statusMsg string, err error) {
	t.parent.stats.Add(stats.ErrDownloadCount, 1)

	dbErr := t.parent.db.addError(t.id, t.obj.Objname, statusMsg)
	cmn.AssertNoErr(dbErr)

	glog.Errorf("error occurred when downloading %s: %v", t, err)

	t.finishedCh <- err
}

func (t *task) persist() {
	t.parent.db.persistTask(t.id, cmn.TaskDlInfo{
		Name:       t.obj.Objname,
		Downloaded: t.currentSize.Load(),
		Total:      t.totalSize,

		StartTime: t.started,
		EndTime:   t.ended,

		Running: false,
	})
}

func (t *task) waitForFinish() <-chan error {
	return t.finishedCh
}

func (t *task) String() string {
	return t.request.String()
}

func internalErrorMessage() string {
	return "Internal server error."
}

func httpClientErrorMessage(err *url.Error) string {
	return fmt.Sprintf("Error performing request to object's location (%s): %v.", err.URL, err.Err)
}

func httpRequestErrorMessage(url string, resp *http.Response) string {
	return fmt.Sprintf("Error downloading file from object's location (%s). Server returned status: %s.", url, resp.Status)
}

func newQueue() *queue {
	return &queue{
		ch: make(chan *task, queueChSize),
		m:  make(map[string]struct{}),
	}
}

func (q *queue) put(t *task) (added bool, err error, errCode int) {
	timer := time.NewTimer(putInQueueTimeout)

	q.Lock()
	defer q.Unlock()
	if _, exists := q.m[t.request.uid()]; exists {
		// If request already exists we should just omit this
		return false, nil, 0
	}

	select {
	case q.ch <- t:
		break
	case <-timer.C:
		return false, fmt.Errorf("timeout when trying to put task %v in queue, try later", t), http.StatusRequestTimeout
	}
	timer.Stop()
	q.m[t.request.uid()] = struct{}{}
	return true, nil, 0
}

// get try to find first task which was not yet canceled
func (q *queue) get() (foundTask *task) {
	for foundTask == nil {
		t, ok := <-q.ch
		if !ok {
			foundTask = nil
			return
		}

		q.RLock()
		if _, exists := q.m[t.request.uid()]; exists {
			// NOTE: We do not delete task here but postpone it until the task
			// has finished to prevent situation where we put task which is being
			// downloaded.
			foundTask = t
		}
		q.RUnlock()
	}

	timeout := cmn.GCO.Get().Downloader.Timeout
	if foundTask.timeout != "" {
		var err error
		timeout, err = time.ParseDuration(foundTask.timeout)
		cmn.AssertNoErr(err) // this should be checked beforehand
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	foundTask.downloadCtx = ctx
	foundTask.cancelFunc = cancel
	return
}

func (q *queue) exists(req *request) bool {
	q.RLock()
	_, exists := q.m[req.uid()]
	q.RUnlock()
	return exists
}

func (q *queue) delete(req *request) bool {
	q.Lock()
	_, exists := q.m[req.uid()]
	delete(q.m, req.uid())
	q.Unlock()
	return exists
}

func (q *queue) stop() {
	q.RLock()
	if q.ch != nil {
		close(q.ch)
	}
	q.RUnlock()
}

func (q *queue) cleanup() {
	q.Lock()
	q.ch = nil
	q.m = nil
	q.Unlock()
}

//
// Checksum validation helpers
//

const (
	gsHashHeader                    = "X-Goog-Hash"
	gsHashHeaderValueSeparator      = ","
	gsHashHeaderChecksumValuePrefix = "crc32c="

	gsStorageURL    = "storage.googleapis.com"
	gsAPIURL        = "www.googleapis.com"
	gsAPIPathPrefix = "/storage/v1"

	s3ChecksumHeader      = "ETag"
	s3IllegalChecksumChar = "-"

	s3UrlRegex = `(s3-|s3\.)?(.*)\.amazonaws\.com`
)

// Get file checksum if link points to Google storage or s3
func getCksum(link string, resp *http.Response) cmn.Cksummer {
	u, err := url.Parse(link)
	cmn.AssertNoErr(err)

	if isGoogleStorageURL(u) {
		hs, ok := resp.Header[gsHashHeader]
		if !ok {
			return nil
		}
		return cmn.NewCksum(cmn.ChecksumCRC32C, getChecksumFromGoogleFormat(hs))
	} else if isGoogleAPIURL(u) {
		hdr := resp.Header.Get(gsHashHeader)
		if hdr == "" {
			return nil
		}
		hs := strings.Split(hdr, gsHashHeaderValueSeparator)
		return cmn.NewCksum(cmn.ChecksumCRC32C, getChecksumFromGoogleFormat(hs))
	} else if isS3URL(link) {
		if cksum := resp.Header.Get(s3ChecksumHeader); cksum != "" && !strings.Contains(cksum, s3IllegalChecksumChar) {
			return cmn.NewCksum(cmn.ChecksumMD5, cksum)
		}
	}

	return nil
}

func isGoogleStorageURL(u *url.URL) bool {
	return u.Host == gsStorageURL
}

func isGoogleAPIURL(u *url.URL) bool {
	return u.Host == gsAPIURL && strings.HasPrefix(u.Path, gsAPIPathPrefix)
}

func isS3URL(link string) bool {
	re := regexp.MustCompile(s3UrlRegex)
	return re.MatchString(link)
}

func getChecksumFromGoogleFormat(hs []string) string {
	for _, h := range hs {
		if strings.HasPrefix(h, gsHashHeaderChecksumValuePrefix) {
			encoded := strings.TrimPrefix(h, gsHashHeaderChecksumValuePrefix)
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return ""
			}
			return hex.EncodeToString(decoded)
		}
	}

	return ""
}
