// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
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
// requests to download the objects itself- it purely manages the lifecycle of
// joggers. It translates requests to internal representation and forwards them
// to the Dispatcher. Dispatcher then dispatches the request to correct jogger instance.
//
// ====== API ======
//
// API exposed to the rest of the code includes the following operations:
//   * Run      - to run
//   * Stop     - to stop
//   * Download    - to download a new object from a URL
//   * Abort       - to abort a previously requested download (currently queued or currently downloading)
//   * Status      - to request the status of a previously requested download
// The Download, Abort and Status requests are encapsulated into an internal
// request object, added to a dispatcher's request queue and then are dispatched by dispatcher
// to the correct jogger. The remaining operations are private to the Downloader and
// are used only internally. Dispatcher is implemented as goroutine listening for
// incoming requests from Downloader
//
// Each jogger, which corresponds to one mountpath, has a download channel
// (downloadCh) where download requests, that are dispatched from Dispatcher, are
// queued. Thus, downloads occur on a per-mountpath basis and are handled one at
// a time by jogger as they arrive.
//
// ====== Downloading ======
//
// After Downloader received a download job, it sends the job to Dispatcher.
// Dispatcher processes one job at the time, extracting objects to download
// from job in batches. When joggers queues have available space for new objects
// to download, dispatcher puts objects to download in these queues. If joggers
// are currently full, dispatcher waits with dispatching next batch until they aren't.
//
// Single object's download is represented as object of `task` type, and there is at
// most one active task assigned to any jogger at any given time. The
// tasks are created when dispatcher wants to schedule download of an object
// for jogger and are destroyed when the download is aborted, finished or
// fails.
//
// After a task is created, a separate goroutine is spun up to make the
// actual GET request (jogger's download method). The goroutine for the jogger
// sits idle awaiting an abort(failed or aborted by user) or finish message from the
// goroutine responsible for the actual download.
//
// ====== Aborting ======
//
// When Dispatcher receives an abort request, it aborts running task or
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
// When Dispatcher receives a status update request, it dispatches to a separate
// jogger goroutine that checks if the downloaded completed. Otherwise it checks
// if it is currently being downloaded. If it is being currently downloaded, it
// returns the progress. Otherwise, it returns that the object hasn't been
// downloaded yet. Now, the file may never be downloaded if the download was
// never queued to the downloadCh.
//
// Status updates are either reported in terms of size or size and percentage.
// When downloading an object from a server, we attempt to obtain the object size
// using the "Content-Length" field returned in the Header.
// Note: not all servers respond with a "Content-Length" request header.
// For these cases, a progress percentage is not returned, just the current
// number of bytes that have been downloaded.
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
	actRemove = "REMOVE"
	actAbort  = "ABORT"
	actStatus = "STATUS"
	actList   = "LIST"

	jobsChSize = 1000
)

var (
	// Downloader cannot use global HTTP client because it must work with
	// arbitrary server. The downloader chooses the correct client by
	// server's URL. Certification check is disabled always for now and
	// does not depend on cluster settings.
	httpClient  = &http.Client{}
	httpsClient = cmn.NewClient(cmn.TransportArgs{
		UseHTTPS:   true,
		SkipVerify: true,
	})
)

// public types
type (
	// Downloader implements the fs.PathRunner and XactDemand interface. When
	// download related requests are made to AIS using the download endpoint,
	// Downloader dispatches these requests to the corresponding jogger.
	Downloader struct {
		cmn.Named
		cmn.XactDemandBase
		cmn.MountpathXact

		t          cluster.Target
		mountpaths *fs.MountedFS
		statsT     stats.Tracker

		mpathReqCh chan fs.ChangeReq
		adminCh    chan *request
		downloadCh chan DlJob

		dispatcher *dispatcher
	}
)

// private types
type (
	// The result of calling one of Downloader's exposed methods is encapsulated
	// in a response object, which is used to communicate the outcome of the
	// request.
	response struct {
		resp       interface{}
		err        error
		statusCode int
	}

	// Calling Downloader's exposed methods results in creation of a request
	// for admin related tasks (i.e. aborting and status updates). These
	// objects are used by Downloader to process the request, and are then
	// dispatched to the correct jogger to be handled.
	request struct {
		action     string         // one of: adminAbort, adminList, adminStatus, adminRemove
		id         string         // id of the job task
		regex      *regexp.Regexp // regex of descriptions to return if id is empty
		responseCh chan *response // where the outcome of the request is written
	}

	progressReader struct {
		r        io.Reader
		reporter func(n int64)
	}
)

func clientForURL(u string) *http.Client {
	if cmn.IsHTTPS(u) {
		return httpsClient
	}
	return httpClient
}

//==================================== Requests ===========================================

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

var (
	// interface guard
	_ fs.PathRunner = &Downloader{}
)

func (d *Downloader) Name() string                    { return "downloader" }
func (d *Downloader) ReqAddMountpath(mpath string)    { d.mpathReqCh <- fs.MountpathAdd(mpath) }
func (d *Downloader) ReqRemoveMountpath(mpath string) { d.mpathReqCh <- fs.MountpathRem(mpath) }
func (d *Downloader) ReqEnableMountpath(_ string)     {}
func (d *Downloader) ReqDisableMountpath(_ string)    {}

func NewDownloader(t cluster.Target, statsT stats.Tracker, f *fs.MountedFS) (d *Downloader) {
	downloader := &Downloader{
		XactDemandBase: *cmn.NewXactDemandBase(cmn.Download, cmn.Bck{Provider: cmn.ProviderAIS}),
		t:              t,
		statsT:         statsT,
		mountpaths:     f,
		mpathReqCh:     make(chan fs.ChangeReq, 1),
		adminCh:        make(chan *request),
		downloadCh:     make(chan DlJob, jobsChSize),
	}

	downloader.dispatcher = newDispatcher(downloader)
	return downloader
}

func (d *Downloader) init() {
	d.dispatcher.init()
}

// TODO: Downloader doesn't necessarily has to be a go routine
// all it does is forwards the requests to dispatcher
func (d *Downloader) Run() (err error) {
	glog.Infof("Starting %s", d.GetRunName())
	d.t.GetFSPRG().Reg(d)
	d.init()
Loop:
	for {
		select {
		case req := <-d.adminCh:
			switch req.action {
			case actStatus:
				d.dispatcher.dispatchStatus(req)
			case actAbort:
				d.dispatcher.dispatchAbort(req)
			case actRemove:
				d.dispatcher.dispatchRemove(req)
			case actList:
				d.dispatcher.dispatchList(req)
			default:
				cmn.AssertFmt(false, req, req.action)
			}
		case job := <-d.downloadCh:
			d.dispatcher.ScheduleForDownload(job)
		case req := <-d.mpathReqCh:
			err = fmt.Errorf("mountpaths have changed when downloader was running; %s: %s; aborting", req.Action, req.Path)
			break Loop
		case <-d.ChanCheckTimeout():
			if d.Timeout() {
				glog.Infof("%s has timed out. Exiting...", d.GetRunName())
				break Loop
			}
		case <-d.ChanAbort():
			glog.Infof("%s has been aborted. Exiting...", d.GetRunName())
			break Loop
		}
	}
	d.Stop(err)
	return nil
}

// Stop terminates the downloader
func (d *Downloader) Stop(err error) {
	d.t.GetFSPRG().Unreg(d)
	d.XactDemandBase.Stop()
	d.dispatcher.Abort()
	d.EndTime(time.Now())
	glog.Infof("Stopped %s", d.GetRunName())
	if err != nil {
		glog.Errorf("stopping downloader; %s", err.Error())
	}
}

/*
 * Downloader's exposed methods
 */
func (d *Downloader) Download(dJob DlJob) (resp interface{}, err error, statusCode int) {
	d.IncPending()
	defer d.DecPending()
	dlStore.setJob(dJob.ID(), dJob)

	select {
	case d.downloadCh <- dJob:
		return nil, nil, http.StatusOK
	default:
		return "downloader job queue is full", nil, http.StatusTooManyRequests
	}
}

func (d *Downloader) AbortJob(id string) (resp interface{}, err error, statusCode int) {
	d.IncPending()
	req := &request{
		action:     actAbort,
		id:         id,
		responseCh: make(chan *response, 1),
	}
	d.adminCh <- req

	// await the response
	r := <-req.responseCh
	d.DecPending()
	return r.resp, r.err, r.statusCode
}

func (d *Downloader) RemoveJob(id string) (resp interface{}, err error, statusCode int) {
	d.IncPending()
	req := &request{
		action:     actRemove,
		id:         id,
		responseCh: make(chan *response, 1),
	}
	d.adminCh <- req

	// await the response
	r := <-req.responseCh
	d.DecPending()
	return r.resp, r.err, r.statusCode
}

func (d *Downloader) JobStatus(id string) (resp interface{}, err error, statusCode int) {
	d.IncPending()
	req := &request{
		action:     actStatus,
		id:         id,
		responseCh: make(chan *response, 1),
	}
	d.adminCh <- req

	// await the response
	r := <-req.responseCh
	d.DecPending()
	return r.resp, r.err, r.statusCode
}

func (d *Downloader) ListJobs(regex *regexp.Regexp) (resp interface{}, err error, statusCode int) {
	d.IncPending()
	req := &request{
		action:     actList,
		regex:      regex,
		responseCh: make(chan *response, 1),
	}
	d.adminCh <- req

	// await the response
	r := <-req.responseCh
	d.DecPending()
	return r.resp, r.err, r.statusCode
}

func (d *Downloader) checkJob(req *request) (*downloadJobInfo, error) {
	jInfo, err := dlStore.getJob(req.id)
	if err != nil {
		if err == errJobNotFound {
			req.writeErrResp(fmt.Errorf("download job %q not found", req.id), http.StatusNotFound)
			return nil, err
		}
		req.writeErrResp(err, http.StatusInternalServerError)
		return nil, err
	}
	return jInfo, nil
}
