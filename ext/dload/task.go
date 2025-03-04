// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/stats"
)

const (
	gcsUA = "gcloud-golang-storage/20151204" // from cloud.google.com/go/storage/storage.go (userAgent).
)

const (
	retryCnt         = 10  // number of retries to external resource
	reqTimeoutFactor = 1.2 // newTimeout = prevTimeout * reqTimeoutFactor
	internalErrorMsg = "internal server error"
)

type singleTask struct {
	xdl         *Xact
	job         jobif
	obj         dlObj
	started     atomic.Time
	ended       atomic.Time
	currentSize atomic.Int64       // current file size (updated as the download progresses)
	totalSize   atomic.Int64       // total size (nonzero iff Content-Length header was provided by the source)
	downloadCtx context.Context    // w/ cancel function
	getCtx      context.Context    // w/ timeout and size
	cancel      context.CancelFunc // to cancel in-progress download
}

// List of HTTP status codes which we shouldn'task retry (just report the job failed).
var terminalStatuses = map[int]struct{}{
	http.StatusNotFound:          {},
	http.StatusPaymentRequired:   {},
	http.StatusUnauthorized:      {},
	http.StatusForbidden:         {},
	http.StatusMethodNotAllowed:  {},
	http.StatusNotAcceptable:     {},
	http.StatusProxyAuthRequired: {},
	http.StatusGone:              {},
}

////////////////
// singleTask //
////////////////

func (task *singleTask) init() {
	// NOTE: `cancel` is called on abort or when download finishes.
	task.downloadCtx, task.cancel = context.WithCancel(context.Background())
}

func (task *singleTask) download(lom *core.LOM) {
	err := lom.InitBck(task.job.Bck())
	if err == nil {
		err = lom.Load(true /*cache it*/, false /*locked*/)
	}
	if err != nil && !os.IsNotExist(err) {
		task.markFailed(internalErrorMsg)
		return
	}

	if cmn.Rom.FastV(4, cos.SmoduleDload) {
		nlog.Infof("Starting download for %v", task)
	}

	task.started.Store(time.Now())
	lom.SetAtimeUnix(task.started.Load().UnixNano())
	if task.obj.fromRemote {
		err = task.downloadRemote(lom)
	} else {
		err = task.downloadLocal(lom)
	}
	task.ended.Store(time.Now())

	if err != nil {
		task.markFailed(err.Error())
		return
	}

	g.store.incFinished(task.jobID())

	vlabs := map[string]string{stats.VarlabBucket: lom.Bck().Cname("")}
	lsize := task.currentSize.Load()
	g.tstats.AddWith(
		cos.NamedVal64{Name: stats.DloadSize, Value: lsize, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.DloadLatencyTotal, Value: int64(task.ended.Load().Sub(task.started.Load())), VarLabs: vlabs},
	)
	task.xdl.ObjsAdd(1, lsize)
}

func (task *singleTask) _dlocal(lom *core.LOM, timeout time.Duration) (bool /*err is fatal*/, error) {
	ctx, cancel := context.WithTimeout(task.downloadCtx, timeout)
	defer cancel()

	task.getCtx = ctx

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, task.obj.link, http.NoBody)
	if err != nil {
		return true, err
	}

	// Set "User-Agent" header when doing requests to Google Cloud Storage.
	// This should increase the number of connections to GCS.
	if cos.IsGoogleStorageURL(req.URL) {
		req.Header.Add("User-Agent", gcsUA)
	}

	resp, err := clientForURL(task.obj.link).Do(req) //nolint:bodyclose // cos.Close
	if err != nil {
		return false, err
	}

	fatal, err := task._dput(lom, req, resp)
	cos.Close(resp.Body)
	return fatal, err
}

func (task *singleTask) _dput(lom *core.LOM, req *http.Request, resp *http.Response) (bool /*err is fatal*/, error) {
	if resp.StatusCode >= http.StatusBadRequest {
		if resp.StatusCode == http.StatusNotFound {
			return false, cmn.NewErrHTTP(req, fmt.Errorf("%q does not exist", task.obj.link), http.StatusNotFound)
		}
		return false, cmn.NewErrHTTP(req,
			fmt.Errorf("failed to download %q: status %d", task.obj.link, resp.StatusCode),
			resp.StatusCode)
	}

	r := task.wrapReader(resp.Body)
	size := attrsFromLink(task.obj.link, resp, lom)
	task.setTotalSize(size)

	params := core.AllocPutParams()
	{
		params.WorkTag = "dl"
		params.Reader = r
		params.OWT = cmn.OwtPut
		params.Atime = task.started.Load()
		params.Size = size
		params.Xact = task.xdl
	}
	erp := core.T.PutObject(lom, params)
	core.FreePutParams(params)
	if erp != nil {
		return true, erp
	}
	if err := lom.Load(true /*cache it*/, false /*locked*/); err != nil {
		return true, err
	}
	return false, nil
}

func (task *singleTask) downloadLocal(lom *core.LOM) (err error) {
	var (
		timeout = task.initialTimeout()
		fatal   bool
	)
	for i := range retryCnt {
		fatal, err = task._dlocal(lom, timeout)
		if err == nil || fatal {
			return err
		}

		// handle more
		if errors.Is(err, context.Canceled) || errors.Is(err, errThrottlerStopped) {
			return err // canceled or stopped, so just return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			nlog.Warningf("%s [retries: %d/%d]: timeout (%v) - increasing and retrying", task, i, retryCnt, timeout)
			timeout = time.Duration(float64(timeout) * reqTimeoutFactor)
		} else if herr := cmn.Err2HTTPErr(err); herr != nil {
			nlog.Warningf("%s [retries: %d/%d]: failed to perform request: %v (code: %d)", task, i, retryCnt, err, herr.Status)
			if _, exists := terminalStatuses[herr.Status]; exists {
				return err // nothing we can do
			}
		} else {
			if !cos.IsRetriableConnErr(err) {
				return err // ditto
			}
			nlog.Warningf("%s [retries: %d/%d]: connection failed with (%v), retrying...", task, i, retryCnt, err)
		}
		task.reset()
	}
	return err
}

func (task *singleTask) setTotalSize(size int64) {
	if size > 0 {
		task.totalSize.Store(size)
	}
}

func (task *singleTask) reset() {
	task.totalSize.Store(0)
	task.currentSize.Store(0)
}

func (task *singleTask) downloadRemote(lom *core.LOM) error {
	// Set custom context values (used by `ais/backend/*`).
	ctx, cancel := context.WithTimeout(task.downloadCtx, task.initialTimeout())
	defer cancel()

	ctx = context.WithValue(ctx, cos.CtxReadWrapper, cos.ReadWrapperFunc(task.wrapReader))
	ctx = context.WithValue(ctx, cos.CtxSetSize, cos.SetSizeFunc(task.setTotalSize))
	task.getCtx = ctx

	// Do final GET (prefetch) request.
	_, err := core.T.GetCold(ctx, lom, task.xdl.Kind(), cmn.OwtGetTryLock)
	return err
}

func (task *singleTask) initialTimeout() time.Duration {
	config := cmn.GCO.Get()
	timeout := config.Downloader.Timeout.D()
	if task.job.Timeout() != 0 {
		timeout = task.job.Timeout()
	}
	return timeout
}

func (task *singleTask) wrapReader(r io.ReadCloser) io.ReadCloser {
	// Create a custom reader to monitor progress every time we read from response body stream.
	r = &progressReader{
		r: r,
		reporter: func(n int64) {
			task.currentSize.Add(n)
			nl.OnProgress(task.job.Notif())
		},
	}
	// Wrap around throttler reader (noop if throttling is disabled).
	r = task.job.throttler().wrapReader(task.getCtx, r)
	return r
}

// Probably we need to extend the persistent database (db.go) so that it will contain
// also information about specific tasks.
func (task *singleTask) markFailed(statusMsg string) {
	g.tstats.Inc(stats.ErrDloadCount)
	g.store.persistError(task.jobID(), task.obj.objName, statusMsg)
	g.store.incErrorCnt(task.jobID())
}

func (task *singleTask) persist() {
	if err := g.store.persistTaskInfo(task); err != nil {
		nlog.Errorln(err)
	}
}

func (task *singleTask) jobID() string { return task.job.ID() }

func (task *singleTask) uid() string {
	return fmt.Sprintf("%s|%s|%s|%v", task.obj.link, task.job.Bck(), task.obj.objName, task.obj.fromRemote)
}

func (task *singleTask) ToTaskDlInfo() TaskDlInfo {
	ended := task.ended.Load()
	return TaskDlInfo{
		Name:       task.obj.objName,
		Downloaded: task.currentSize.Load(),
		Total:      task.totalSize.Load(),
		StartTime:  task.started.Load(),
		EndTime:    ended,
	}
}

func (task *singleTask) String() (str string) {
	return fmt.Sprintf(
		"{id: %q, obj_name: %q, link: %q, from_remote: %v, bucket: %q}",
		task.jobID(), task.obj.objName, task.obj.link, task.obj.fromRemote, task.job.Bck(),
	)
}
