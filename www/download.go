package www

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

// ================================ Summary ===============================================
//
// Downloader is a long running task that provides a DFC a means to download objects from
// the internet by providing a URL(referred to as a link) to the server where the object exists.
// Downloader does not make the HTTP GET requests to download the files itself- it purely
// manages the lifecycle of joggers and dispatches any download related request to the
// correct jogger instance.
//
// ====== API ======
// API exposed to the rest of the code includes the following operations:
//   * Run      - to run
//   * Stop     - to stop
//   * Download    - to download a new object from a URL
//   * Cancel      - to cancel a previously requested download (currently queued or currently downloading)
//   * Status      - to request the status of a previously requested download
// The Download, Cancel and Status requests are encapsulated into an internal request object and added
// to a request queue and then are dispatched to the correct jogger.
// The remaining operations are private to the Downloader and are used only internally.
//
// Each jogger, which corresponds to a mountpath, has a download channel (downloadCh)
// where download request that are dispatched from Downloader are queued. Thus, downloads
// occur on a per-mountpath basis and are handled one at a time by jogger as they arrive.
// joggers are not a constantly running object, they are only spun up when there is a download request
// for one of the target's mountpaths, and continue to run as more downloads get dispatched to the
// jogger. If there are no more downloads for jogger, the goroutine exits until next time...
//
//
//====== Downloading ======
// After Downloader receives a download request, it starts up the correct
// jogger if it was not already running, and adds it to the jogger's
// downloadCh queue.
// Downloads are represented as extended actions (xactDownload), and there is at most one active xactDownload
// assigned to any jogger at any given time. The xactDownload are created when the jogger dequeues a
// download request from its downloadCh and are destroyed when the download is cancelled, finished or fails.
//
// After a xactDownload is created, a separate goroutine is spun up to make the actual GET request (jogger's
// download method). The goroutine for the jogger sits idle awaiting an abort(failed or cancelled) or
// finish message from the goroutine responsible for the actual download.
//
//====== Cancelling ======
// When Downloader receives a cancel request, it doesn't immediately cancel the download.
// Every cancelDownloadFrequency seconds (default 30), Downloader issues a flushCancelledDownloads
// call that goes through each of the joggers and checks if the current xactDownload for the
// jogger needs to be cancelled.
// If there is a match between the xactDownload's link and the cancel request's link,
// the xactDownload's cancelDownloadFunc function is called, causing the download to promptly fail.
//		The cancelDownloadFunc is created when a context object for the download is made.
//      Refer to jogger's download method.
//
// Otherwise, the link is added to the jogger's cancelMap so if the download is queued to
// be downloaded in the future, the jogger will first check if a download request that has been
// dequeued exists in the cancelMap before even considering to download it. When a jogger has no
// more download requests, it flushes the cancelMap before exiting.
//
// Thus it is possible for a download cancel request to be processed too late,
// at which point the download will finish. A user must use the object delete endpoint to
// delete the object if they still wish to remove the object.

//====== Status Updates ======
// Status updates are made possible by ProgressReader, which just overwrites the io.Reader's
// Read method to additionally notify a Reporter Func, that gets notified the number of bytes
// that have been read every time we read from the response body from the HTTP GET request
// we make to to the link to download the object.
//
// When Downloader receives a status update request, it dispatches to a separate jogger
// goroutine that checks if the downloaded completed. Otherwise it checks if it is
// currently being downloaded. If it is being currently downloaded, it returns the progress.
// Otherwise, it returns that the object hasn't been downloaded yet. Now, the file may
// never be downloaded if the download was never queued to the downloadCh.
//
// Status updates are either reported in terms of size or size and percentage. Before downloading
// an object from a server, we attempt to make a HEAD request to obtain the object size using the
// "Content-Length" field returned in the Header. Note: not all servers implement a HEAD
// request handler. For these cases, a progress percentage is not returned, just the current
// number of bytes that have been downloaded.
//
//====== Notes ======
// Downloader assumes that any type of download request is first sent to a proxy and then
// redirected to the correct target's Downloader (the proxy uses the HRW algorithm to determine the target).
// It is not possible to directly hit a Target's download endpoint to force an object to be downloaded to
// that Target, all request must go through a proxy first.
// ================================ Summary ===============================================

const (
	adminCancel             = "CANCEL"
	adminStatus             = "STATUS"
	taskDownload            = "DOWNLOAD"
	cancelDownloadFrequency = time.Second * 30
	downloadChSize          = 50
)

// public types
type (
	// Downloader implements the fs.PathRunner and XactDemand interface. When download related requests are made
	// to dfc using the download endpoint, Downloader dispatches these requests to the corresponding
	// jogger.
	Downloader struct {
		cmn.NamedID
		cmn.XactDemandBase

		mpathReqCh chan fs.ChangeReq
		adminCh    chan *request
		downloadCh chan *dlTask

		joggers    map[string]*jogger // mpath -> jogger
		mountpaths *fs.MountedFS
		T          cluster.Target
		httpClient *http.Client
	}
)

// private types
type (
	// The result of calling one of Downloader's exposed methos is encapsulated in a response
	// object, which is used to communicate the outcome of the request.
	response struct {
		resp       string
		err        error
		statusCode int
	}

	// Calling Downloader's exposed methods results in the creation of a request for admin related
	// tasks (i.e. cancelling and status updates) and a dlTask for a downlad request. These
	// objects are used by Downloader to process the request, and are then dispatched to the
	// correct jogger to be handled.
	request struct {
		action     string // one of: adminCancel, adminStatus, taskDownload
		link       string
		bucket     string
		objname    string
		fqn        string         // fqn of the object after it has been committed
		responseCh chan *response // where the outcome of the request is written
	}

	// dlTask embeds cmn.XactBase, but it is not part of the targetrunner's xactions member
	// variable. Instead, Downloader manages the lifecycle of the extended action.
	dlTask struct {
		request
		cmn.XactBase
		headers            map[string]interface{} // the headers that are forwarded to the get request to download the object.
		cancelDownloadFunc context.CancelFunc     // used to cancel the download after the request commences
		currentSize        int64                  // the current size of the file (updated as the download progresses)
	}

	// Each jogger corresponds to a mpath. All types of download requests corresponding to the
	// jogger's mpath are forwarded to the jogger. Joggers exist in the Downloader's jogger member variable,
	// and run only when there are dlTasks.
	jogger struct {
		mpath      string
		downloadCh chan *dlTask  // for pending downloads
		finishedCh chan struct{} // when a jogger finishes downloading a dlTask
		parent     *Downloader

		sync.Mutex
		// lock protected
		cancelMap map[string]bool // objects that need to be cancelled
		task      *dlTask         //current download the runner is handling
		running   bool            // if there is a goroutine for the jogger that is currently downloading
		stopAgent bool
	}

	ProgressReader struct {
		io.Reader
		Reporter func(n int64)
	}
)

//==================================== Requests ===========================================

func (req *request) String() (str string) {
	if req.link != "" {
		str += fmt.Sprintf("DL Link %q, ", req.link)
	}

	if req.bucket != "" {
		str += fmt.Sprintf("Bucket: %q, ", req.bucket)
	}

	if req.objname != "" {
		str += fmt.Sprintf("Objname: %q, ", req.objname)
	}
	strings.TrimSuffix(str, ", ")
	return
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

//==================================== ProgressReader =====================================

func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.Reader.Read(p)
	pr.Reporter(int64(n))
	return
}

//==================================== Downloader =====================================
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
	d.joggers[mpath] = d.newJogger(mpath)
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
func NewDownloader(t cluster.Target, f *fs.MountedFS, id int64, kind string) (d *Downloader) {
	return &Downloader{
		mpathReqCh: make(chan fs.ChangeReq, 1),
		adminCh:    make(chan *request),
		downloadCh: make(chan *dlTask),
		joggers:    make(map[string]*jogger, 8),
		T:          t,
		mountpaths: f,
		httpClient: &http.Client{
			Timeout: cmn.GCO.Get().Timeout.DefaultLong,
		},
		XactDemandBase: *cmn.NewXactDemandBase(id, kind),
	}
}

func (d *Downloader) newJogger(mpath string) *jogger {
	return &jogger{
		mpath:      mpath,
		cancelMap:  make(map[string]bool),
		downloadCh: make(chan *dlTask, downloadChSize),
		parent:     d,
		finishedCh: make(chan struct{}, 1),
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
	d.T.RegPathRunner(d)
	d.init()
	ticker := time.NewTicker(cancelDownloadFrequency)
Loop:
	for {
		select {
		case <-ticker.C:
			d.flushCancelledDownloads()
		case req := <-d.adminCh:
			switch req.action {
			case adminCancel:
				d.dispatchCancel(req)
			case adminStatus:
				d.dispatchStatus(req)
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
	ticker.Stop()
	d.Stop(nil)
	return nil
}

// flushCancelledDownloads() cancels any downloads that are currently running
// and have since been cancelled though a cancel request.
// Note: this flushing mechanic occurs every cancelDownloadFrequency seconds,
func (d *Downloader) flushCancelledDownloads() {
	for _, jogger := range d.joggers {
		jogger.Lock()
		if !jogger.running {
			jogger.Unlock()
			continue
		}
		jogger.Unlock()
		jogger.checkDownloadCancelled()
	}
}

// Stop terminates the downloader
func (d *Downloader) Stop(err error) {
	d.T.UnregPathRunner(d)
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
	task := &dlTask{
		request: request{
			action:     taskDownload,
			link:       body.Link,
			bucket:     body.Bucket,
			objname:    body.Objname,
			responseCh: make(chan *response, 1),
		},
		headers: body.Headers,
	}
	d.downloadCh <- task

	// await the response
	r := <-task.responseCh
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

func (d *Downloader) dispatchDownload(task *dlTask) {
	lom := &cluster.LOM{T: d.T, Bucket: task.bucket,
		Objname: task.objname}
	errstr := lom.Fill(cluster.LomFstat)
	if errstr != "" {
		task.writeErrResp(errors.New(errstr), http.StatusInternalServerError)
		return
	}
	if !lom.Exists() {
		task.writeErrResp(fmt.Errorf("Download task with %v failed. Object with the same bucket and objname already exists.", task), http.StatusConflict)
		return
	}
	task.fqn = lom.Fqn

	// check for invalid header
	if len(task.headers) > 0 {
		for _, v := range task.headers {
			if _, ok := v.(string); !ok {
				task.writeErrResp(
					fmt.Errorf("Download task %v failed. Headers should be a JSON object where both keys and values are strings.", task),
					http.StatusBadRequest,
				)
				return
			}
		}
	}

	mpathInfo, _ := fs.Mountpaths.Path2MpathInfo(task.fqn)
	if mpathInfo == nil {
		err := fmt.Errorf("Download task with %v failed. Failed to get mountpath for the request's fqn %s", task, task.fqn)
		glog.Error(err.Error())
		task.writeErrResp(err, http.StatusInternalServerError)
		return
	}
	a, ok := d.joggers[mpathInfo.Path]
	if !ok {
		err := fmt.Errorf("No mpath exists for %v", task)
		cmn.Assert(false, err)
	}
	a.addToDownloadQueue(task)
}

func (d *Downloader) dispatchCancel(req *request) {
	lom := &cluster.LOM{T: d.T, Bucket: req.bucket,
		Objname: req.objname}
	errstr := lom.Fill(cluster.LomFstat)
	if errstr != "" {
		req.writeErrResp(errors.New(errstr), http.StatusInternalServerError)
		return
	}
	if !lom.Exists() {
		req.writeErrResp(fmt.Errorf("Cancel request with %v failed. Download has already finished.", req), http.StatusBadRequest)
		return
	}
	req.fqn = lom.Fqn

	mpathInfo, _ := fs.Mountpaths.Path2MpathInfo(req.fqn)
	if mpathInfo == nil {
		err := fmt.Errorf("Cancel request with %v failed. Failed to obtain mountpath for request's fqn: %q.", req, req.fqn)
		glog.Error(err.Error())
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}
	a, ok := d.joggers[mpathInfo.Path]
	if !ok {
		err := fmt.Errorf("Cancel request with %v failed. No corresponding mpath exists", req)
		cmn.Assert(false, err)
	}
	a.addToCancelMap(req)
}

func (d *Downloader) dispatchStatus(req *request) {
	lom := &cluster.LOM{T: d.T, Bucket: req.bucket,
		Objname: req.objname}
	errstr := lom.Fill(cluster.LomFstat)
	if errstr != "" {
		req.writeErrResp(errors.New(errstr), http.StatusInternalServerError)
		return
	}
	req.fqn = lom.Fqn

	mpathInfo, _ := fs.Mountpaths.Path2MpathInfo(req.fqn)
	if mpathInfo == nil {
		err := fmt.Errorf("Status request with %v failed. Failed to obtain mountpath for request's fqn %s.", req, req.fqn)
		glog.Error(err.Error())
		req.writeErrResp(err, http.StatusInternalServerError)
		return
	}
	a, ok := d.joggers[mpathInfo.Path]
	if !ok {
		err := fmt.Errorf("Status request with %v failed. No corresponding mpath exists", req)
		cmn.Assert(false, err)
	}
	go a.downloadProgress(req)
}

//==================================== jogger =====================================

// checkDownloadCancelled aborts the current xaction if it is downloading a file that has
// been cancelled.
func (j *jogger) checkDownloadCancelled() (cancelled bool) {
	j.Lock()
	defer j.Unlock()
	if j.task == nil {
		return
	}
	if _, ok := j.cancelMap[j.task.link]; ok {
		j.task.Abort()
		delete(j.cancelMap, j.task.link)
		cancelled = true
	}
	return
}

func (j *jogger) conditionalDownloadAbort() {
	j.Lock()
	if j.task == nil {
		j.Unlock()
		return
	}

	if !j.task.Finished() {
		j.task.Abort()
	}
	j.Unlock()
	return
}

func (j *jogger) download() {
	j.Lock()
	lom := &cluster.LOM{T: j.parent.T, Bucket: j.task.bucket,
		Objname: j.task.objname}
	if errstr := lom.Fill(cluster.LomFstat); errstr != "" {
		j.task.Abort()
		j.Unlock()
		return
	}
	if lom.Exists() {
		if glog.V(4) {
			glog.Infof("Download with %v failed. Object with the same bucket and objname already exists.", j.task)
		}
		j.task.Abort()
		j.Unlock()
		return
	}
	postFQN := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)

	// create request
	httpReq, err := http.NewRequest(http.MethodGet, j.task.link, nil)
	if err != nil {
		glog.Errorf("Download with %v failed. Failed to create %s request, err: %s", j.task, http.MethodGet, err.Error())
		j.task.Abort()
		j.Unlock()
		return
	}

	// add headers
	if len(j.task.headers) > 0 {
		for k, v := range j.task.headers {
			httpReq.Header.Set(k, v.(string))
		}
	}

	// Do
	contextwith, cancel := context.WithTimeout(context.Background(), cmn.GCO.Get().Timeout.SendFile)
	j.task.cancelDownloadFunc = cancel
	j.Unlock()
	requestWithContext := httpReq.WithContext(contextwith)
	if glog.V(4) {
		glog.Infof("Starting download for %v", j.task)
	}
	response, err := j.parent.httpClient.Do(requestWithContext)

	if err != nil {
		glog.Errorf("Download with %v failed. Failed to GET from URL %q, err: %s", j.task, j.task.link, err.Error())
		j.conditionalDownloadAbort()
		return
	}

	//create a custom reader to monitor progress every time we read from response body stream
	updatedResponseReader := &ProgressReader{
		Reader: response.Body,
		Reporter: func(n int64) {
			j.Lock()
			if j.task == nil {
				j.Unlock()
				return
			}
			j.task.currentSize += n
			j.Unlock()
		},
	}

	var errstr string
	if _, lom.Nhobj, lom.Size, errstr = j.parent.T.Receive(postFQN, lom, "", updatedResponseReader); errstr != "" {
		j.conditionalDownloadAbort()
		return
	}

	if errstr, _ = j.parent.T.Commit(contextwith, lom, postFQN, false /*rebalance*/); errstr != "" {
		glog.Errorf("Download putCommit error %q", errstr)
		// what should we do here??????
		j.conditionalDownloadAbort()
		return
	}
	j.finishedCh <- struct{}{}
}

func (j *jogger) downloadProgress(req *request) {
	var resp string

	lom := &cluster.LOM{T: j.parent.T, Bucket: req.bucket,
		Objname: req.objname}
	errstr := lom.Fill(cluster.LomFstat)
	if errstr != "" {
		req.writeErrResp(errors.New(errstr), http.StatusInternalServerError)
		return
	}
	if !lom.Exists() {
		// It's possible this file already existed on DFC and was never downloaded.
		resp = fmt.Sprintf("Download 100%% complete, Total Size %s. For %v", cmn.B2S(lom.Size, 3), req)
		goto write
	}

	j.Lock()
	if !j.running || j.task == nil {
		resp = fmt.Sprintf("No downloads queued matching %v", req)
	} else if j.task.link != req.link {
		// Current object being downloaded does not match status request
		resp = fmt.Sprintf("Object is not currently being downloaded by DFC for %v", req)
	} else {
		currentSize := j.task.currentSize
		size := cmn.B2S(currentSize, 3)
		resp = fmt.Sprintf("Downloaded %s so far for %v.", size, req)
	}
	j.Unlock()

write:
	req.writeResp(resp)
}

func (j *jogger) addToCancelMap(req *request) {
	j.Lock()
	// Only add the cancel request if the jogger is running.
	if j.running {
		j.cancelMap[req.link] = true
	}
	j.Unlock()
	req.writeResp(fmt.Sprintf("Cancel request %s added to download cancel list.", req))
}

func (j *jogger) addToDownloadQueue(task *dlTask) {
	if task == nil {
		return
	}

	j.downloadCh <- task
	j.Lock()
	if !j.running {
		go j.run()
	}
	j.Unlock()
	task.writeResp(fmt.Sprintf("Download request with %v added to queue.", task))
}

func (j *jogger) run() {
	glog.Infof("Starting jogger for mpath %q.", j.mpath)
	j.Lock()
	j.running = true
	j.Unlock()
Loop:
	for {
		select {
		case task, ok := <-j.downloadCh:
			if !ok {
				break
			}
			j.Lock()
			if j.stopAgent {
				j.Unlock()
				break Loop
			}

			j.task = task
			j.Unlock()

			// check if request has been cancelled before we even start
			if j.checkDownloadCancelled() {
				goto cleanup
			}

			// start download
			go j.download()

			// await abort or completion
			select {
			case <-j.task.ChanAbort():
				glog.Infoln(fmt.Errorf("Aborted download for %v", j.task))
				break
			case <-j.finishedCh:
				glog.Infoln(fmt.Sprintf("Finished download for %v", j.task))
				j.task.EndTime(time.Now())
				break
			}
		cleanup:
			j.Lock()
			if j.task.cancelDownloadFunc != nil {
				j.task.cancelDownloadFunc()
			}
			// too late to cancel the download
			if _, ok := j.cancelMap[j.task.link]; ok {
				delete(j.cancelMap, j.task.link)
			}
			j.task = nil
			j.Unlock()
			j.parent.DecPending()

		default:
			break Loop
		}
	}
	j.Lock()
	// empty the cancel map, no more files to download
	j.cancelMap = make(map[string]bool)
	j.running = false
	j.Unlock()
}

// Stop terminates the jogger
func (j *jogger) stop() {
	glog.Infof("Stopping jogger for mpath: %s", j.mpath)
	close(j.downloadCh)

	j.Lock()
	j.stopAgent = true
	if j.running && j.task != nil {
		j.task.Abort()
	}
	j.Unlock()
}
