// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
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
// from queue (see: put, get). If the task is running, `cancel` function is
// invoked to abort task's request.
//
// ====== Status Updates ======
//
// Status updates are made possible by progressReader that overwrites the
// io.Reader's Read method to additionally notify a Reporter Func.
// The notification includes the number of bytes read so far from the GET
// response body.
//
// When Dispatcher receives a status update request, it dispatches to a separate
// jogger goroutine that checks if the downloaded completed. Otherwise it checks
// if it is currently being downloaded. If it is being currently downloaded, it
// returns the progress. Otherwise, it returns that the object hasn't been
// downloaded yet. Now, the file may never be downloaded if the download (request)
// was never queued to the downloadCh.
//
// Status updates are either reported in terms of size or size and percentage.
// When downloading an object from a server, we attempt to obtain the object size
// using the "Content-Length" field returned in the Header.
// NOTE: Not all servers respond with a "Content-Length" request header.
// In these cases progress percentage is not returned, just the current
// downloaded size (in bytes).
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
)

var (
	// Downloader cannot use global HTTP client because it must work with
	// arbitrary server. The downloader chooses the correct client by
	// server's URL. Certification check is disabled always for now and
	// does not depend on cluster settings.
	httpClient  = cmn.NewClient(cmn.TransportArgs{})
	httpsClient = cmn.NewClient(cmn.TransportArgs{
		UseHTTPS:   true,
		SkipVerify: true,
	})
	instance atomic.Int64
)

type (
	// Downloader implements xaction.Demand interface.
	// Upon getting requests via AIS download endpoint,
	// Downloader dispatches these requests to the corresponding jogger.
	Downloader struct {
		xaction.DemandBase
		t          cluster.Target
		statsT     stats.Tracker
		dispatcher *dispatcher
	}

	// The result of calling one of Downloader's exposed methods is encapsulated
	// in a response object, which is used to communicate the outcome of the
	// request.
	response struct {
		value      interface{}
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
		response   *response      // where the outcome of the request is written
		onlyActive bool           // request status of only active tasks
	}

	progressReader struct {
		r        io.Reader
		reporter func(n int64)
	}

	dowFactory struct {
		xreg.BaseEntry
		xact   *Downloader
		t      cluster.Target
		statsT stats.Tracker
	}
)

// interface guard
var (
	_ xaction.Demand = (*Downloader)(nil)
	_ xreg.Renewable = (*dowFactory)(nil)
	_ io.ReadCloser  = (*progressReader)(nil)
)

func init() {
	xreg.RegGlobXact(&dowFactory{})
}

func clientForURL(u string) *http.Client {
	if cos.IsHTTPS(u) {
		return httpsClient
	}
	return httpClient
}

/////////////
// request //
/////////////

func (req *request) write(value interface{}, err error, statusCode int) {
	req.response = &response{
		value:      value,
		err:        err,
		statusCode: statusCode,
	}
}

func (req *request) writeErrResp(err error, statusCode int) {
	req.write(nil, err, statusCode)
}

func (req *request) writeResp(value interface{}) {
	req.write(value, nil, http.StatusOK)
}

////////////////////
// progressReader //
////////////////////

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

////////////////
// dowFactory //
////////////////

func (*dowFactory) New(args xreg.Args, _ *cluster.Bck) xreg.Renewable {
	return &dowFactory{t: args.T, statsT: args.Custom.(stats.Tracker)}
}

func (p *dowFactory) Start() error {
	xdl := newDownloader(p.t, p.statsT)
	p.xact = xdl
	go xdl.Run()
	return nil
}
func (*dowFactory) Kind() string        { return cmn.ActDownload }
func (p *dowFactory) Get() cluster.Xact { return p.xact }

////////////////
// Downloader //
////////////////

func (*Downloader) Name() string {
	i := strconv.FormatInt(instance.Load(), 10)
	return "downloader" + i
}

func newDownloader(t cluster.Target, statsT stats.Tracker) (d *Downloader) {
	downloader := &Downloader{
		DemandBase: *xaction.NewXDB(cos.GenUUID(), cmn.Download, nil),
		t:          t,
		statsT:     statsT,
	}
	downloader.dispatcher = newDispatcher(downloader)
	downloader.InitIdle()
	instance.Inc()
	return downloader
}

func (d *Downloader) Run() {
	glog.Infof("starting %s", d.Name())
	err := d.dispatcher.run()
	d.stop(err)
}

// stop terminates the downloader and all dependent entities.
func (d *Downloader) stop(err error) {
	d.DemandBase.Stop()
	d.Finish(err)
}

func (d *Downloader) Download(dJob DlJob) (resp interface{}, statusCode int, err error) {
	d.IncPending()
	defer d.DecPending()
	dlStore.setJob(dJob.ID(), dJob)
	config := cmn.GCO.Get()
	select {
	case d.dispatcher.downloadCh <- dJob:
		return nil, http.StatusOK, nil
	default:
		select {
		case d.dispatcher.downloadCh <- dJob:
			return nil, http.StatusOK, nil
		case <-time.After(config.Timeout.CplaneOperation.D()):
			return "downloader job queue is full", http.StatusTooManyRequests, nil
		}
	}
}

func (d *Downloader) AbortJob(id string) (resp interface{}, statusCode int, err error) {
	d.IncPending()
	defer d.DecPending()
	req := &request{
		action: actAbort,
		id:     id,
	}
	return d.dispatcher.dispatchAdminReq(req)
}

func (d *Downloader) RemoveJob(id string) (resp interface{}, statusCode int, err error) {
	d.IncPending()
	defer d.DecPending()
	req := &request{
		action: actRemove,
		id:     id,
	}
	return d.dispatcher.dispatchAdminReq(req)
}

func (d *Downloader) JobStatus(id string, onlyActive bool) (resp interface{}, statusCode int, err error) {
	d.IncPending()
	defer d.DecPending()
	req := &request{
		action:     actStatus,
		id:         id,
		onlyActive: onlyActive,
	}
	return d.dispatcher.dispatchAdminReq(req)
}

func (d *Downloader) ListJobs(regex *regexp.Regexp) (resp interface{}, statusCode int, err error) {
	d.IncPending()
	defer d.DecPending()
	req := &request{
		action: actList,
		regex:  regex,
	}
	return d.dispatcher.dispatchAdminReq(req)
}

func (d *Downloader) checkJob(req *request) (*downloadJobInfo, error) {
	jInfo, err := dlStore.getJob(req.id)
	if err != nil {
		cos.Assert(errors.Is(err, errJobNotFound))
		err := cmn.NewNotFoundError("%s: download job %q", d.t.Snode(), req.id)
		req.writeErrResp(err, http.StatusNotFound)
		return nil, err
	}
	return jInfo, nil
}

func (d *Downloader) Stats() cluster.XactStats {
	baseStats := d.DemandBase.Stats().(*xaction.BaseXactStatsExt)
	baseStats.Ext = &xaction.BaseXactDemandStatsExt{IsIdle: d.Pending() == 0}
	return baseStats
}
