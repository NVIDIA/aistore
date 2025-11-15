// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"io"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
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

type (
	factory struct {
		xreg.RenewBase
		xctn *Xact
		bck  *meta.Bck
	}
	Xact struct {
		xact.DemandBase
		p          *factory
		dispatcher *dispatcher
	}

	// The result of calling one of Downloader's public methods is encapsulated
	// in a response object, which is used to communicate the outcome of the
	// request.
	response struct {
		value      any
		err        error
		statusCode int
	}

	// Calling Downloader's public methods results in creation of a request
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
)

// interface guard
var (
	_ xact.Demand    = (*Xact)(nil)
	_ xreg.Renewable = (*factory)(nil)
	_ io.ReadCloser  = (*progressReader)(nil)
)

/////////////
// factory //
/////////////

func (*factory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &factory{RenewBase: xreg.RenewBase{Args: args}, bck: args.Custom.(*meta.Bck)}
}

func (p *factory) Start() error {
	xdl := newXact(p)
	p.xctn = xdl

	g.once.Do(func() {
		g.store = newInfoStore(g.db)
	})

	go xdl.Run(nil)
	return nil
}

func (*factory) Kind() string     { return apc.ActDownload }
func (p *factory) Get() core.Xact { return p.xctn }

func (*factory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

//////////
// Xact //
//////////

func newXact(p *factory) (xld *Xact) {
	xld = &Xact{p: p}
	xld.dispatcher = newDispatcher(xld)
	xld.DemandBase.Init(p.UUID(), apc.Download, p.bck, 0 /*use default*/)
	return
}

func (xld *Xact) Run(*sync.WaitGroup) {
	nlog.Infoln("starting", xld.Name())
	err := xld.dispatcher.run()
	if err != nil {
		xld.AddErr(err)
	}
	xld.stop()
}

// stop terminates the downloader and all dependent entities.
func (xld *Xact) stop() {
	xld.DemandBase.Stop()
	xld.Finish()
}

func (xld *Xact) Download(job jobif) (resp any, statusCode int, err error) {
	xld.IncPending()
	defer xld.DecPending()

	dljob := g.store.setJob(job)

	select {
	case xld.dispatcher.workCh <- job:
		return dljob.id, http.StatusOK, nil // TODO -- FIXME: dljob.clone() all the way to client (+below)
	default:
		select {
		case xld.dispatcher.workCh <- job:
			return dljob.id, http.StatusOK, nil
		case <-time.After(cmn.Rom.CplaneOperation()):
			return "downloader job queue is full", http.StatusTooManyRequests, nil
		}
	}
}

func (xld *Xact) AbortJob(id string) (resp any, statusCode int, err error) {
	xld.IncPending()
	req := &request{action: actAbort, id: id}
	resp, statusCode, err = xld.dispatcher.adminReq(req)
	xld.DecPending()
	return
}

func (xld *Xact) RemoveJob(id string) (resp any, statusCode int, err error) {
	xld.IncPending()
	req := &request{action: actRemove, id: id}
	resp, statusCode, err = xld.dispatcher.adminReq(req)
	xld.DecPending()
	return
}

func (xld *Xact) JobStatus(id string, onlyActive bool) (resp any, statusCode int, err error) {
	xld.IncPending()
	req := &request{action: actStatus, id: id, onlyActive: onlyActive}
	resp, statusCode, err = xld.dispatcher.adminReq(req)
	xld.DecPending()
	return
}

func (*Xact) CtlMsg() string       { return "" }
func (xld *Xact) Snap() *core.Snap { return xld.Base.NewSnap(xld) }

/////////////
// request //
/////////////

func (req *request) rsp(value any, err error, statusCode int) {
	req.response = &response{value: value, err: err, statusCode: statusCode}
}
func (req *request) errRsp(err error, statusCode int) { req.rsp(nil, err, statusCode) }
func (req *request) okRsp(value any)                  { req.rsp(value, nil, http.StatusOK) }

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
