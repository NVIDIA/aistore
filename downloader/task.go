// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/stats"
)

const (
	retryCnt         = 10               // number of retries to external resource
	reqTimeoutFactor = 1.2              // newTimeout = prevTimeout * reqTimeoutFactor
	headReqTimeout   = 15 * time.Second // timeout for HEAD request to get the Content-Length
	internalErrorMsg = "internal server error"
)

// List of HTTP status codes on which we should
// not retry and just mark job as failed.
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

type (
	singleObjectTask struct {
		parent *Downloader
		job    DlJob
		obj    dlObj

		started atomic.Time
		ended   atomic.Time

		currentSize atomic.Int64 // The current size of the file (updated as the download progresses).
		totalSize   atomic.Int64 // The total size of the file (nonzero only if Content-Length header was provided by the source of the file).

		downloadCtx context.Context    // Context with cancel function.
		cancel      context.CancelFunc // Used to cancel the download after the request commences.
	}
)

func (t *singleObjectTask) init() {
	// NOTE: `cancel` is called on abort or when download finishes.
	t.downloadCtx, t.cancel = context.WithCancel(context.Background())
}

func (t *singleObjectTask) download() {
	defer t.cancel()

	lom := cluster.AllocLOM(t.obj.objName)
	defer cluster.FreeLOM(lom)
	err := lom.Init(t.job.Bck())
	if err == nil {
		err = lom.Load(true /*cache it*/, false /*locked*/)
	}
	if err != nil && !os.IsNotExist(err) {
		t.markFailed(internalErrorMsg)
		return
	}

	if glog.V(4) {
		glog.Infof("Starting download for %v", t)
	}

	t.started.Store(time.Now())
	lom.SetAtimeUnix(t.started.Load().UnixNano())
	if t.obj.fromRemote {
		err = t.downloadRemote(lom)
	} else {
		err = t.downloadLocal(lom)
	}
	t.ended.Store(time.Now())

	if err != nil {
		t.markFailed(err.Error())
		return
	}

	dlStore.incFinished(t.jobID())

	t.parent.statsT.AddMany(
		stats.NamedVal64{Name: stats.DownloadSize, Value: t.currentSize.Load()},
		stats.NamedVal64{Name: stats.DownloadLatency, Value: int64(t.ended.Load().Sub(t.started.Load()))},
	)
	t.parent.ObjectsInc()
	t.parent.BytesAdd(t.currentSize.Load())
}

func (t *singleObjectTask) tryDownloadLocal(lom *cluster.LOM, timeout time.Duration) (fatal bool, err error) {
	ctx, cancel := context.WithTimeout(t.downloadCtx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.obj.link, nil)
	if err != nil {
		return true, err
	}

	// Set "User-Agent" header when doing requests to Google Cloud Storage.
	// This should increase number of connections to GCS.
	if cos.IsGoogleStorageURL(req.URL) {
		req.Header.Add("User-Agent", cmn.GcsUA)
	}

	resp, err := clientForURL(t.obj.link).Do(req)
	if err != nil {
		return false, err
	}
	defer cos.Close(resp.Body)

	if resp.StatusCode >= http.StatusBadRequest {
		return false, cmn.NewErrHTTP(req, "", resp.StatusCode)
	}

	r := t.wrapReader(ctx, resp.Body)
	size := attrsFromLink(t.obj.link, resp, lom)
	t.setTotalSize(size)

	params := cluster.PutObjectParams{
		Tag:      "dl",
		Reader:   r,
		RecvType: cluster.RegularPut,
		Started:  t.started.Load(),
	}
	if err := t.parent.t.PutObject(lom, params); err != nil {
		return true, err
	}
	return true, lom.Load(true /*cache it*/, false /*locked*/) // TODO: review
}

func (t *singleObjectTask) downloadLocal(lom *cluster.LOM) (err error) {
	var (
		timeout = t.initialTimeout()
		fatal   bool
	)
	for i := 0; i < retryCnt; i++ {
		fatal, err = t.tryDownloadLocal(lom, timeout)
		if err == nil || fatal {
			return err
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, errThrottlerStopped) {
			// Download was canceled or stopped, so just return.
			return err
		}
		if errors.Is(err, context.DeadlineExceeded) {
			glog.Warningf("%s [retries: %d/%d]: timeout (%v) - increasing and retrying...",
				t, i, retryCnt, timeout)
			timeout = time.Duration(float64(timeout) * reqTimeoutFactor)
		} else if httpErr := cmn.Err2HTTPErr(err); httpErr != nil {
			glog.Warningf("%s [retries: %d/%d]: failed to perform request: %v (code: %d)", t, i, retryCnt, err,
				httpErr.Status)
			if _, exists := terminalStatuses[httpErr.Status]; exists {
				// Nothing we can do...
				return err
			}
			// Otherwise retry...
		} else if cos.IsRetriableConnErr(err) {
			glog.Warningf("%s [retries: %d/%d]: connection failed with (%v), retrying...", t, i, retryCnt, err)
		} else {
			glog.Warningf("%s [retries: %d/%d]: unexpected error (%v), retrying...", t, i, retryCnt, err)
		}

		t.reset()
	}
	return err
}

func (t *singleObjectTask) wrapReader(ctx context.Context, r io.ReadCloser) io.ReadCloser {
	// Create a custom reader to monitor progress every time we read from response body stream.
	r = &progressReader{
		r: r,
		reporter: func(n int64) {
			t.currentSize.Add(n)
			nl.OnProgress(t.job.Notif())
		},
	}
	// Wrap around throttler reader (noop if throttling is disabled).
	r = t.job.throttler().wrapReader(ctx, r)
	return r
}

func (t *singleObjectTask) setTotalSize(size int64) {
	if size > 0 {
		t.totalSize.Store(size)
	}
}

func (t *singleObjectTask) reset() {
	t.totalSize.Store(0)
	t.currentSize.Store(0)
}

func (t *singleObjectTask) downloadRemote(lom *cluster.LOM) error {
	// Set custom context values (used by `ais/backend/*`).
	ctx, cancel := context.WithTimeout(t.downloadCtx, t.initialTimeout())
	defer cancel()
	wrapReader := func(r io.ReadCloser) io.ReadCloser { return t.wrapReader(ctx, r) }
	ctx = context.WithValue(ctx, cmn.CtxReadWrapper, cmn.ReadWrapperFunc(wrapReader))
	ctx = context.WithValue(ctx, cmn.CtxSetSize, cmn.SetSizeFunc(t.setTotalSize))

	// Do final GET (prefetch) request.
	_, err := t.parent.t.GetCold(ctx, lom, cluster.Prefetch)
	return err
}

func (t *singleObjectTask) initialTimeout() time.Duration {
	config := cmn.GCO.Get()
	timeout := config.Downloader.Timeout.D()
	if t.job.Timeout() != 0 {
		timeout = t.job.Timeout()
	}
	return timeout
}

// Probably we need to extend the persistent database (db.go) so that it will contain
// also information about specific tasks.
func (t *singleObjectTask) markFailed(statusMsg string) {
	t.parent.statsT.Add(stats.ErrDownloadCount, 1)

	dlStore.persistError(t.jobID(), t.obj.objName, statusMsg)
	dlStore.incErrorCnt(t.jobID())
}

func (t *singleObjectTask) persist() {
	_ = dlStore.persistTaskInfo(t.jobID(), t.ToTaskDlInfo())
}

func (t *singleObjectTask) jobID() string { return t.job.ID() }
func (t *singleObjectTask) uid() string {
	return fmt.Sprintf("%s|%s|%s|%v", t.obj.link, t.job.Bck(), t.obj.objName, t.obj.fromRemote)
}

func (t *singleObjectTask) ToTaskDlInfo() TaskDlInfo {
	ended := t.ended.Load()
	return TaskDlInfo{
		Name:       t.obj.objName,
		Downloaded: t.currentSize.Load(),
		Total:      t.totalSize.Load(),

		StartTime: t.started.Load(),
		EndTime:   ended,

		Running: ended.IsZero(),
	}
}

func (t *singleObjectTask) String() (str string) {
	return fmt.Sprintf(
		"{id: %q, obj_name: %q, link: %q, from_remote: %v, bucket: %q}",
		t.jobID(), t.obj.objName, t.obj.link, t.obj.fromRemote, t.job.Bck(),
	)
}
