// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

const (
	retryCnt          = 10               // number of retries to external resource
	defaultReqTimeout = 30 * time.Minute // default request timeout when downloading web content
	reqTimeoutFactor  = 1.2              // newTimeout = prevTimeout * reqTimeoutFactor
	headReqTimeout    = 5 * time.Second  // timeout for HEAD request to get the Content-Length
	internalErrorMsg  = "internal server error"
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
		*request

		started time.Time
		ended   time.Time

		currentSize atomic.Int64 // the current size of the file (updated as the download progresses)
		totalSize   atomic.Int64 // the total size of the file (nonzero only if Content-Length header was provided by the source of the file)

		downloadCtx context.Context    // context with cancel function
		cancelFunc  context.CancelFunc // used to cancel the download after the request commences
	}
)

func (t *singleObjectTask) download() {
	lom := &cluster.LOM{T: t.parent.t, ObjName: t.obj.objName}
	err := lom.Init(t.bck)
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

	t.started = time.Now()
	lom.SetAtimeUnix(t.started.UnixNano())
	if t.obj.fromCloud {
		err = t.downloadCloud(lom)
	} else {
		err = t.downloadLocal(lom, t.started)
	}
	t.ended = time.Now()

	if err != nil {
		t.markFailed(err.Error())
		return
	}

	if err := dlStore.incFinished(t.id); err != nil {
		glog.Error(err)
	}

	t.parent.statsT.AddMany(
		stats.NamedVal64{Name: stats.DownloadSize, Value: t.currentSize.Load()},
		stats.NamedVal64{Name: stats.DownloadLatency, Value: int64(time.Since(t.started))},
	)
	t.parent.ObjectsInc()
	t.parent.BytesAdd(t.currentSize.Load())
}

func (t *singleObjectTask) tryDownloadLocal(lom *cluster.LOM, started time.Time, timeout time.Duration) error {
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

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		err, _ := cmn.NewHTTPError(req, "", resp.StatusCode)
		return err
	}

	// Create a custom reader to monitor progress every time we read from response body stream
	progressReader := &progressReader{
		r: resp.Body,
		reporter: func(n int64) {
			t.currentSize.Add(n)
		},
	}

	t.setTotalSize(resp)

	roi := getRemoteObjInfo(t.obj.link, resp)
	err = t.parent.t.PutObject(cluster.PutObjectParams{
		LOM:          lom,
		Reader:       progressReader,
		WorkFQN:      workFQN,
		RecvType:     cluster.ColdGet,
		Cksum:        roi.cksum,
		Version:      roi.version,
		Started:      started,
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

func (t *singleObjectTask) downloadLocal(lom *cluster.LOM, started time.Time) (err error) {
	var (
		httpErr = &cmn.HTTPError{}
		timeout = defaultReqTimeout
	)
	for i := 0; i < retryCnt; i++ {
		err = t.tryDownloadLocal(lom, started, timeout)
		if err == nil {
			return nil
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

func (t *singleObjectTask) setTotalSize(resp *http.Response) {
	if resp.ContentLength > 0 {
		t.totalSize.Store(resp.ContentLength)
	}
}

func (t *singleObjectTask) reset() {
	t.totalSize.Store(0)
	t.currentSize.Store(0)
}

func (t *singleObjectTask) downloadCloud(lom *cluster.LOM) error {
	if err, _ := t.parent.t.GetCold(t.downloadCtx, lom, true /* prefetch */); err != nil {
		return err
	}
	return nil
}

func (t *singleObjectTask) cancel() {
	t.cancelFunc()
}

// TODO: this should also inform somehow downloader status about being Aborted/canceled
// Probably we need to extend the persistent database (db.go) so that it will contain
// also information about specific tasks.
func (t *singleObjectTask) markFailed(statusMsg string) {
	t.cancel()
	t.parent.statsT.Add(stats.ErrDownloadCount, 1)

	dlStore.persistError(t.id, t.obj.objName, statusMsg)
	if err := dlStore.incErrorCnt(t.id); err != nil {
		glog.Error(err)
	}
}

func (t *singleObjectTask) persist() {
	_ = dlStore.persistTaskInfo(t.id, TaskDlInfo{
		Name:       t.obj.objName,
		Downloaded: t.currentSize.Load(),
		Total:      t.totalSize.Load(),

		StartTime: t.started,
		EndTime:   t.ended,

		Running: false,
	})
}

func (t *singleObjectTask) String() string {
	return t.request.String()
}
