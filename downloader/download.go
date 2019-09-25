// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
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
	adminRemove  = "REMOVE"
	adminAbort   = "ABORT"
	adminStatus  = "STATUS"
	adminList    = "LIST"
	taskDownload = "DOWNLOAD"

	jobsChSize = 1000
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
		cmn.MountpathXact

		t          cluster.Target
		mountpaths *fs.MountedFS
		stats      stats.Tracker

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
	// for admin related tasks (i.e. aborting and status updates) and a dlTask
	// for a download request. These objects are used by Downloader to process
	// the request, and are then dispatched to the correct jogger to be handled.
	request struct {
		action     string         // one of: adminAbort, adminList, adminStatus, adminRemove, taskDownload
		id         string         // id of the job task
		regex      *regexp.Regexp // regex of descriptions to return if id is empty
		obj        cmn.DlObj
		bucket     string
		provider   string
		timeout    string
		fqn        string         // fqn of the object after it has been committed
		responseCh chan *response // where the outcome of the request is written
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
		str += fmt.Sprintf("bucket: %q (provider: %q), ", req.bucket, req.provider)
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
func (d *Downloader) Description() string {
	return "download objects into a bucket"
}

/*
 * Downloader constructors
 */
func NewDownloader(t cluster.Target, stats stats.Tracker, f *fs.MountedFS, id int64, kind string) (d *Downloader) {
	downloader := &Downloader{
		XactDemandBase: *cmn.NewXactDemandBase(id, kind, "" /* no bucket */, false),
		t:              t,
		stats:          stats,
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
	glog.Infof("Starting %s", d.Getname())
	d.t.GetFSPRG().Reg(d)
	d.init()
Loop:
	for {
		select {
		case req := <-d.adminCh:
			switch req.action {
			case adminStatus:
				d.dispatcher.dispatchStatus(req)
			case adminAbort:
				d.dispatcher.dispatchAbort(req)
			case adminRemove:
				d.dispatcher.dispatchRemove(req)
			case adminList:
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
				glog.Infof("%s has timed out. Exiting...", d.Getname())
				break Loop
			}
		case <-d.ChanAbort():
			glog.Infof("%s has been aborted. Exiting...", d.Getname())
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
	glog.Infof("Stopped %s", d.Getname())
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
		action:     adminAbort,
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

func (d *Downloader) JobStatus(id string) (resp interface{}, err error, statusCode int) {
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

func (d *Downloader) ListJobs(regex *regexp.Regexp) (resp interface{}, err error, statusCode int) {
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

func (d *Downloader) checkJob(req *request) (*DownloadJobInfo, error) {
	jInfo, err := dlStore.getJob(req.id)
	if err != nil {
		if err == errJobNotFound {
			req.writeErrResp(fmt.Errorf("download job with id %q has not been found", req.id), http.StatusNotFound)
			return nil, err
		}
		req.writeErrResp(err, http.StatusInternalServerError)
		return nil, err
	}
	return jInfo, nil
}

func (d *Downloader) activeTasks(reqID string) []cmn.TaskDlInfo {
	d.dispatcher.RLock()
	currentTasks := make([]cmn.TaskDlInfo, 0, len(d.dispatcher.joggers))
	for _, j := range d.dispatcher.joggers {
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
	d.dispatcher.RUnlock()

	sort.Sort(cmn.TaskInfoByName(currentTasks))
	return currentTasks
}

// Gets the number of pending files of reqId
func (d *Downloader) getNumPending(jobID string) int {
	numPending := 0

	d.dispatcher.RLock()
	for _, j := range d.dispatcher.joggers {
		if tasks, ok := j.q.m[jobID]; ok {
			numPending += len(tasks)
		}
	}
	d.dispatcher.RUnlock()

	return numPending
}

func internalErrorMessage() string {
	return "Internal server error."
}

func httpClientErrorMessage(err *url.Error) string {
	return fmt.Sprintf("Error performing request to object's location (%s): %v.", err.URL, err.Err)
}

func httpRequestErrorMessage(url string, resp *http.Response) string {
	return fmt.Sprintf("Error downloading file from object's location (%s). Server returned status: %s", url, resp.Status)
}

//
// Checksum validation helpers
//

const (
	gsHashHeader                    = "X-Goog-Hash"
	gsHashHeaderValueSeparator      = ","
	gsHashHeaderChecksumValuePrefix = "crc32c="

	s3ChecksumHeader      = "ETag"
	s3IllegalChecksumChar = "-"
)

// Get file checksum if link points to Google storage or s3
func getCksum(link string, resp *http.Response) *cmn.Cksum {
	u, err := url.Parse(link)
	cmn.AssertNoErr(err)

	if cmn.IsGoogleStorageURL(u) {
		hs, ok := resp.Header[gsHashHeader]
		if !ok {
			return nil
		}
		return cmn.NewCksum(cmn.ChecksumCRC32C, getChecksumFromGoogleFormat(hs))
	} else if cmn.IsGoogleAPIURL(u) {
		hdr := resp.Header.Get(gsHashHeader)
		if hdr == "" {
			return nil
		}
		hs := strings.Split(hdr, gsHashHeaderValueSeparator)
		return cmn.NewCksum(cmn.ChecksumCRC32C, getChecksumFromGoogleFormat(hs))
	} else if cmn.IsS3URL(link) {
		if cksum := resp.Header.Get(s3ChecksumHeader); cksum != "" && !strings.Contains(cksum, s3IllegalChecksumChar) {
			return cmn.NewCksum(cmn.ChecksumMD5, cksum)
		}
	}

	return nil
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
