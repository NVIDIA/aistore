package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

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
// from queue (see: put, get). If the task is running, cancelDownloadFunc is
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
	adminCancel    = "CANCEL"
	adminStatus    = "STATUS"
	taskDownload   = "DOWNLOAD"
	downloadChSize = 200

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
	}
)

// private types
type (
	// The result of calling one of Downloader's exposed methos is encapsulated
	// in a response object, which is used to communicate the outcome of the
	// request.
	response struct {
		resp       string
		err        error
		statusCode int
	}

	// Calling Downloader's exposed methods results in the creation of a request
	// for admin related tasks (i.e. cancelling and status updates) and a dlTask
	// for a downlad request. These objects are used by Downloader to process
	// the request, and are then dispatched to the correct jogger to be handled.
	request struct {
		action     string // one of: adminCancel, adminStatus, taskDownload
		link       string
		bucket     string
		objname    string
		fqn        string         // fqn of the object after it has been committed
		responseCh chan *response // where the outcome of the request is written
	}

	// task embeds cmn.XactBase, but it is not part of the targetrunner's xactions member
	// variable. Instead, Downloader manages the lifecycle of the extended action.
	task struct {
		parent *Downloader
		*request

		sync.Mutex
		headers            map[string]string  // the headers that are forwarded to the get request to download the object.
		cancelDownloadFunc context.CancelFunc // used to cancel the download after the request commences
		currentSize        int64              // the current size of the file (updated as the download progresses)
		finishedCh         chan error         // when a jogger finishes downloading a dlTask
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
	if req.link != "" {
		str += fmt.Sprintf("link: %q, ", req.link)
	}

	if req.bucket != "" {
		str += fmt.Sprintf("bucket: %q, ", req.bucket)
	}

	if req.objname != "" {
		str += fmt.Sprintf("objname: %q, ", req.objname)
	}
	return "{" + strings.TrimSuffix(str, ", ") + "}"
}

func (req *request) write(resp string, err error, statusCode int) {
	req.responseCh <- &response{
		resp:       resp,
		err:        err,
		statusCode: statusCode,
	}
	close(req.responseCh)
}

func (req *request) writeErrResp(err error, statusCode int) {
	req.write("", err, statusCode)
}

func (req *request) writeResp(response string) {
	req.write(response, nil, http.StatusOK)
}

func (req *request) equals(rhs *request) bool {
	return req.objname == rhs.objname && req.bucket == rhs.bucket && req.link == rhs.link
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
func NewDownloader(t cluster.Target, stats stats.Tracker, f *fs.MountedFS, id int64, kind, bucket string) (d *Downloader) {
	return &Downloader{
		mpathReqCh:     make(chan fs.ChangeReq, 1),
		adminCh:        make(chan *request),
		downloadCh:     make(chan *task),
		joggers:        make(map[string]*jogger, 8),
		t:              t,
		stats:          stats,
		mountpaths:     f,
		XactDemandBase: *cmn.NewXactDemandBase(id, kind, bucket),
	}
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
			default:
				cmn.Assert(false, req, req.action)
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

func (d *Downloader) Download(body *cmn.DlBody) (resp string, err error, statusCode int) {
	d.IncPending()
	t := &task{
		parent: d,
		request: &request{
			action:     taskDownload,
			link:       body.Link,
			bucket:     body.Bucket,
			objname:    body.Objname,
			responseCh: make(chan *response, 1),
		},
		headers:    body.Headers,
		finishedCh: make(chan error, 1),
	}
	d.downloadCh <- t

	// await the response
	r := <-t.responseCh
	if r.err != nil {
		d.DecPending()
	}
	return r.resp, r.err, r.statusCode
}

func (d *Downloader) Cancel(body *cmn.DlBody) (resp string, err error, statusCode int) {
	d.IncPending()
	req := &request{
		action:     adminCancel,
		link:       body.Link,
		bucket:     body.Bucket,
		objname:    body.Objname,
		responseCh: make(chan *response, 1),
	}
	d.adminCh <- req

	// await the response
	r := <-req.responseCh
	d.DecPending()
	return r.resp, r.err, r.statusCode
}

func (d *Downloader) Status(body *cmn.DlBody) (resp string, err error, statusCode int) {
	d.IncPending()
	req := &request{
		action:     adminStatus,
		link:       body.Link,
		bucket:     body.Bucket,
		objname:    body.Objname,
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
	lom := &cluster.LOM{T: d.t, Bucket: t.bucket, Objname: t.objname}
	errstr := lom.Fill(cluster.LomFstat)
	if errstr != "" {
		t.writeErrResp(errors.New(errstr), http.StatusInternalServerError)
		return
	}
	if lom.Exists() {
		t.writeErrResp(fmt.Errorf("download task with %v failed, object with the same bucket and objname already exists", t), http.StatusConflict)
		return
	}
	t.fqn = lom.Fqn

	if lom.ParsedFQN.MpathInfo == nil {
		err := fmt.Errorf("download task with %v failed. Failed to get mountpath for the request's fqn %s", t, t.fqn)
		glog.Error(err.Error())
		t.writeErrResp(err, http.StatusInternalServerError)
		return
	}
	j, ok := d.joggers[lom.ParsedFQN.MpathInfo.Path]
	if !ok {
		err := fmt.Errorf("no mpath exists for %v", t)
		cmn.Assert(false, err)
	}
	j.addToDownloadQueue(t)
}

func (d *Downloader) dispatchCancel(req *request) {
	lom := &cluster.LOM{T: d.t, Bucket: req.bucket, Objname: req.objname}
	errstr := lom.Fill(cluster.LomFstat)
	if errstr != "" {
		req.writeErrResp(errors.New(errstr), http.StatusInternalServerError)
		return
	}
	if lom.Exists() {
		req.writeErrResp(fmt.Errorf("cancel request with %s failed, download has already finished", req), http.StatusBadRequest)
		return
	}
	req.fqn = lom.Fqn

	if lom.ParsedFQN.MpathInfo == nil {
		err := fmt.Errorf("cancel request with %v failed. Failed to obtain mountpath for request's fqn: %q", req, req.fqn)
		glog.Error(err.Error())
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}
	j, ok := d.joggers[lom.ParsedFQN.MpathInfo.Path]
	if !ok {
		err := fmt.Errorf("cancel request with %v failed. No corresponding mpath exists", req)
		cmn.Assert(false, err)
	}

	// Cancel currently running task
	j.Lock()
	if j.task != nil && j.task.request.equals(req) {
		j.task.cancel()
		req.writeResp(fmt.Sprintf("Currently running request has been canceled: %s", req))
		j.Unlock()
		return
	}
	j.Unlock()

	if !j.q.delete(req) {
		req.writeErrResp(fmt.Errorf("download for %s is not in the queue", req), http.StatusBadRequest)
		return
	}
	req.writeResp(fmt.Sprintf("Request has been canceled: %s", req))
}

func (d *Downloader) dispatchStatus(req *request) {
	lom := &cluster.LOM{T: d.t, Bucket: req.bucket, Objname: req.objname}
	errstr := lom.Fill(cluster.LomFstat)
	if errstr != "" {
		req.writeErrResp(errors.New(errstr), http.StatusInternalServerError)
		return
	}
	if lom.Exists() {
		// It is possible this file already existed on cluster and was never downloaded.
		resp := fmt.Sprintf("Download 100%% complete, total size %s, for %v.", cmn.B2S(lom.Size, 3), req)
		req.writeResp(resp)
		return
	}
	req.fqn = lom.Fqn

	if lom.ParsedFQN.MpathInfo == nil {
		err := fmt.Errorf("status request with %v failed. Failed to obtain mountpath for request's fqn %s", req, req.fqn)
		glog.Error(err.Error())
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}
	j, ok := d.joggers[lom.ParsedFQN.MpathInfo.Path]
	if !ok {
		err := fmt.Errorf("status request with %v failed. No corresponding mpath exists", req)
		cmn.Assert(false, err)
	}

	var written bool
	j.Lock()
	if j.task != nil {
		go j.task.downloadProgress(req)
		written = true
	}
	j.Unlock()

	if !written {
		req.writeErrResp(fmt.Errorf("object is not currently being downloaded for %v", req), http.StatusNotFound)
	}
}

//==================================== jogger =====================================

func (j *jogger) addToDownloadQueue(task *task) {
	cmn.Assert(task != nil)
	if err, errCode := j.q.put(task); err != nil {
		task.writeErrResp(err, errCode)
		return
	}

	task.writeResp(fmt.Sprintf("Download request %s added to queue", task))
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
		if err := <-t.waitForFinish(); err != nil {
			glog.Errorf("error occurred when downloading %s: %v", t, err)
		}
		j.Lock()
		j.task = nil
		j.Unlock()
		j.q.delete(t.request)
		j.parent.DecPending()

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
		j.task.abort(errors.New("stopped jogger"))
	}
	j.Unlock()

	<-j.terminateCh
}

func (t *task) download() {
	t.Lock()
	lom := &cluster.LOM{T: t.parent.t, Bucket: t.bucket, Objname: t.objname}
	if errstr := lom.Fill(cluster.LomFstat); errstr != "" {
		t.abort(errors.New(errstr))
		t.Unlock()
		return
	}
	if lom.Exists() {
		t.abort(errors.New("object with the same bucket and objname already exists"))
		t.Unlock()
		return
	}
	postFQN := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)

	// create request
	httpReq, err := http.NewRequest(http.MethodGet, t.link, nil)
	if err != nil {
		t.abort(err)
		t.Unlock()
		return
	}

	// add headers
	if len(t.headers) > 0 {
		for k, v := range t.headers {
			httpReq.Header.Set(k, v)
		}
	}

	// Do
	ctx, cancel := context.WithTimeout(context.Background(), cmn.GCO.Get().Timeout.SendFile)
	t.cancelDownloadFunc = cancel
	t.Unlock()
	requestWithContext := httpReq.WithContext(ctx)
	if glog.V(4) {
		glog.Infof("Starting download for %v", t)
	}
	started := time.Now()
	response, err := httpClient.Do(requestWithContext)
	if err != nil {
		t.abort(err)
		return
	}
	if response.StatusCode >= http.StatusBadRequest {
		t.abort(fmt.Errorf("status code: %d", response.StatusCode))
		return
	}

	// Create a custom reader to monitor progress every time we read from response body stream
	progressReader := &progressReader{
		r: response.Body,
		reporter: func(n int64) {
			t.Lock()
			t.currentSize += n
			t.Unlock()
		},
	}

	if err := t.parent.t.Receive(postFQN, progressReader, lom); err != nil {
		t.abort(err)
		return
	}

	t.parent.stats.AddMany(
		stats.NamedVal64{Name: stats.DownloadSize, Val: t.currentSize},
		stats.NamedVal64{Name: stats.DownloadLatency, Val: int64(time.Since(started))},
	)
	t.finishedCh <- nil
}

func (t *task) cancel() {
	t.Lock()
	if t.cancelDownloadFunc != nil {
		t.cancelDownloadFunc()
	}
	t.Unlock()
}

func (t *task) abort(err error) {
	t.parent.stats.Add(stats.ErrDownloadCount, 1)
	t.finishedCh <- err
}

func (t *task) waitForFinish() <-chan error {
	return t.finishedCh
}

func (t *task) downloadProgress(req *request) {
	t.Lock()
	if !t.request.equals(req) {
		t.Unlock()
		// Current object being downloaded does not match status request
		resp := fmt.Errorf("object is not currently being downloaded for %v", req)
		req.writeErrResp(resp, http.StatusNotFound)
	} else {
		currentSize := t.currentSize
		t.Unlock()
		size := cmn.B2S(currentSize, 3)
		resp := fmt.Sprintf("Downloaded %s so far for %v.", size, req)
		req.writeResp(resp)
	}
}

func (t *task) String() string {
	return t.request.String()
}

func newQueue() *queue {
	return &queue{
		ch: make(chan *task, downloadChSize),
		m:  make(map[string]struct{}),
	}
}

func (q *queue) put(t *task) (err error, errCode int) {
	timer := time.NewTimer(putInQueueTimeout)

	q.Lock()
	defer q.Unlock()
	if _, exists := q.m[t.request.String()]; exists {
		return fmt.Errorf("download with this link %q was already scheduled", t.link), http.StatusBadRequest
	}

	select {
	case q.ch <- t:
		break
	case <-timer.C:
		return fmt.Errorf("timeout when trying to put task %v in queue, try later", t), http.StatusRequestTimeout
	}
	timer.Stop()
	q.m[t.request.String()] = struct{}{}
	return nil, 0
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
		if _, exists := q.m[t.request.String()]; exists {
			// NOTE: task is deleted when it has finished to prevent situation
			// where we put task which is being downloaded.
			foundTask = t
		}
		q.RUnlock()
	}
	return
}

func (q *queue) delete(req *request) bool {
	q.Lock()
	_, exists := q.m[req.String()]
	delete(q.m, req.String())
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
