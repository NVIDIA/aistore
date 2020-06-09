// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

const (
	retryCnt         = 10               // number of retries to external resource
	reqTimeoutFactor = 1.2              // newTimeout = prevTimeout * reqTimeoutFactor
	headReqTimeout   = 15 * time.Second // timeout for HEAD request to get the Content-Length
	internalErrorMsg = "internal server error"
)

var (
	// List of HTTP status codes on which we should
	// not retry and just mark job as failed.
	terminalStatuses = map[int]struct{}{
		http.StatusNotFound:          {},
		http.StatusPaymentRequired:   {},
		http.StatusUnauthorized:      {},
		http.StatusForbidden:         {},
		http.StatusMethodNotAllowed:  {},
		http.StatusNotAcceptable:     {},
		http.StatusProxyAuthRequired: {},
		http.StatusGone:              {},
	}
)

type (
	singleObjectTask struct {
		parent *Downloader
		job    DlJob
		obj    dlObj

		started atomic.Time
		ended   atomic.Time

		currentSize atomic.Int64 // the current size of the file (updated as the download progresses)
		totalSize   atomic.Int64 // the total size of the file (nonzero only if Content-Length header was provided by the source of the file)

		downloadCtx context.Context    // context with cancel function
		cancelFunc  context.CancelFunc // used to cancel the download after the request commences
	}
)

func (t *singleObjectTask) download() {
	lom := &cluster.LOM{T: t.parent.t, ObjName: t.obj.objName}
	err := lom.Init(t.job.Bck())
	if err == nil {
		err = lom.Load()
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
	if t.obj.fromCloud {
		err = t.downloadCloud(lom)
	} else {
		err = t.downloadLocal(lom)
	}
	t.ended.Store(time.Now())

	if err != nil {
		t.markFailed(err.Error())
		return
	}

	dlStore.incFinished(t.id())

	t.parent.statsT.AddMany(
		stats.NamedVal64{Name: stats.DownloadSize, Value: t.currentSize.Load()},
		stats.NamedVal64{Name: stats.DownloadLatency, Value: int64(t.started.Load().Sub(t.ended.Load()))},
	)
	t.parent.ObjectsInc()
	t.parent.BytesAdd(t.currentSize.Load())
}

func (t *singleObjectTask) tryDownloadLocal(lom *cluster.LOM, timeout time.Duration) error {
	var (
		workFQN = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)
	)

	ctx, cancel := context.WithTimeout(t.downloadCtx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.obj.link, nil)
	if err != nil {
		return err
	}

	// Set "User-Agent" header when doing requests to Google Cloud Storage.
	// This should increase number of connections to GCS.
	if cmn.IsGoogleStorageURL(req.URL) {
		req.Header.Add("User-Agent", cmn.GcsUA)
	}

	resp, err := clientForURL(t.obj.link).Do(req)
	if err != nil {
		return err
	}
	defer func() {
		debug.AssertNoErr(resp.Body.Close())
	}()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("request failed with %d status code (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	var (
		r   = t.wrapReader(ctx, resp.Body)
		roi = roiFromLink(t.obj.link, resp)
	)

	t.setTotalSize(roi.size)

	lom.SetCustomMD(roi.md)
	err = t.parent.t.PutObject(cluster.PutObjectParams{
		LOM:          lom,
		Reader:       r,
		WorkFQN:      workFQN,
		RecvType:     cluster.ColdGet,
		Started:      t.started.Load(),
		WithFinalize: true,
	})
	if err != nil {
		return err
	}
	if err := lom.Load(); err != nil {
		return err
	}
	return nil
}

func (t *singleObjectTask) downloadLocal(lom *cluster.LOM) (err error) {
	var (
		httpErr = &cmn.HTTPError{}
		timeout = t.initialTimeout()
	)
	for i := 0; i < retryCnt; i++ {
		err = t.tryDownloadLocal(lom, timeout)
		if err == nil {
			return nil
		} else if errors.Is(err, context.Canceled) || errors.Is(err, errThrottlerStopped) {
			// Download was canceled or stopped, so just return.
			return err
		} else if errors.Is(err, context.DeadlineExceeded) {
			glog.Warningf("%s [retries: %d/%d]: context exceeded with timeout (%v), increasing and retrying...", t, i, retryCnt, timeout)
			timeout = time.Duration(float64(timeout) * reqTimeoutFactor)
		} else if errors.As(err, &httpErr) {
			glog.Warningf("%s [retries: %d/%d]: failed to perform request: %v (code: %d)", t, i, retryCnt, err, httpErr.Status)
			if _, exists := terminalStatuses[httpErr.Status]; exists {
				// Nothing we can do...
				return err
			}
			// Otherwise retry...
		} else if cmn.IsErrConnectionReset(err) || cmn.IsErrConnectionRefused(err) {
			glog.Warningf("%s [retries: %d/%d]: connection failed with (%v), retrying...", t, i, retryCnt, err)
		} else {
			glog.Warningf("%s [retries: %d/%d]: unexpected error (%v), retrying...", t, i, retryCnt, err)
		}

		t.reset()
	}
	return
}

func (t *singleObjectTask) wrapReader(ctx context.Context, r io.ReadCloser) io.ReadCloser {
	// Create a custom reader to monitor progress every time we read from response body stream.
	r = &progressReader{
		r: r,
		reporter: func(n int64) {
			t.currentSize.Add(n)
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

func (t *singleObjectTask) downloadCloud(lom *cluster.LOM) error {
	// Set custom context values (used by `ais/cloud/*`).
	ctx, cancel := context.WithTimeout(t.downloadCtx, t.initialTimeout())
	defer cancel()
	wrapReader := func(r io.ReadCloser) io.ReadCloser { return t.wrapReader(ctx, r) }
	ctx = context.WithValue(ctx, cmn.CtxReadWrapper, cmn.ReadWrapperFunc(wrapReader))
	ctx = context.WithValue(ctx, cmn.CtxSetSize, cmn.SetSizeFunc(t.setTotalSize))

	// Do final GET (prefetch) request.
	err, _ := t.parent.t.GetCold(ctx, lom, true /*prefetch*/)
	return err
}

func (t *singleObjectTask) initialTimeout() time.Duration {
	timeout := cmn.GCO.Get().Downloader.Timeout
	if t.job.Timeout() != 0 {
		timeout = t.job.Timeout()
	}
	return timeout
}

func (t *singleObjectTask) cancel() {
	if t.cancelFunc != nil {
		t.cancelFunc()
	}
}

// Probably we need to extend the persistent database (db.go) so that it will contain
// also information about specific tasks.
func (t *singleObjectTask) markFailed(statusMsg string) {
	t.cancel()
	t.parent.statsT.Add(stats.ErrDownloadCount, 1)

	dlStore.persistError(t.id(), t.obj.objName, statusMsg)
	dlStore.incErrorCnt(t.id())
}

func (t *singleObjectTask) persist() {
	_ = dlStore.persistTaskInfo(t.id(), t.ToTaskDlInfo())
}

func (t *singleObjectTask) id() string { return t.job.ID() }
func (t *singleObjectTask) uid() string {
	return fmt.Sprintf("%s|%s|%s|%v", t.obj.link, t.job.Bck(), t.obj.objName, t.obj.fromCloud)
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
		"{id: %q, obj_name: %q, link: %q, from_cloud: %v, bucket: %q}",
		t.id(), t.obj.objName, t.obj.link, t.obj.fromCloud, t.job.Bck(),
	)
}
