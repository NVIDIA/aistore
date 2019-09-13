// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

const (
	// Number of retries when doing a request to external resource.
	retryCnt = 3
)

type (
	singleObjectTask struct {
		parent *Downloader
		*request

		started time.Time
		ended   time.Time

		currentSize atomic.Int64 // the current size of the file (updated as the download progresses)
		totalSize   int64        // the total size of the file (nonzero only if Content-Length header was provided by the source of the file)
		finishedCh  chan error   // when a jogger finishes downloading a dlTask

		downloadCtx context.Context    // context with cancel function
		cancelFunc  context.CancelFunc // used to cancel the download after the request commences
	}
)

func (t *singleObjectTask) download() {
	var (
		statusMsg string
	)
	lom := &cluster.LOM{T: t.parent.t, Objname: t.obj.Objname}
	err := lom.Init(t.bucket, t.bckProvider)
	if err == nil {
		err = lom.Load()
	}
	if err != nil {
		t.abort(internalErrorMessage(), err)
		return
	}
	if lom.Exists() {
		t.abort(internalErrorMessage(), errors.New("object with the same bucket and objname already exists"))
		return
	}

	if glog.V(4) {
		glog.Infof("Starting download for %v", t)
	}

	t.started = time.Now()
	lom.SetAtimeUnix(t.started.UnixNano())
	if !t.obj.FromCloud {
		statusMsg, err = t.downloadLocal(lom, t.started)
	} else {
		statusMsg, err = t.downloadCloud(lom)
	}
	t.ended = time.Now()

	if err != nil {
		t.abort(statusMsg, err)
		return
	}

	if err := dlStore.incFinished(t.id); err != nil {
		glog.Errorf(err.Error())
	}

	t.parent.stats.AddMany(
		stats.NamedVal64{Name: stats.DownloadSize, Val: t.currentSize.Load()},
		stats.NamedVal64{Name: stats.DownloadLatency, Val: int64(time.Since(t.started))},
	)
	t.parent.XactDemandBase.ObjectsInc()
	t.parent.XactDemandBase.BytesAdd(t.currentSize.Load())
	t.finishedCh <- nil
}

func (t *singleObjectTask) downloadLocal(lom *cluster.LOM, started time.Time) (errMsg string, err error) {
	var (
		postFQN = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)
		req     *http.Request
		resp    *http.Response
	)

	for i := 0; i < retryCnt; i++ {
		// Create request
		req, err = http.NewRequest(http.MethodGet, t.obj.Link, nil)
		if err != nil {
			continue
		}

		// Set "User-Agent" header when doing requests to Google Cloud Storage.
		// This should increase number of connections to GCS.
		if cmn.IsGoogleStorageURL(req.URL) {
			req.Header.Add("User-Agent", cmn.GcsUA)
		}

		reqWithCtx := req.WithContext(t.downloadCtx)
		resp, err = httpClient.Do(reqWithCtx)
		if err != nil {
			// Error returned by httpClient.Do() is a *url.Error
			errMsg = httpClientErrorMessage(err.(*url.Error))
			continue
		}
		if resp.StatusCode >= http.StatusBadRequest {
			errMsg = httpRequestErrorMessage(t.obj.Link, resp)
			err = fmt.Errorf("status code: %d", resp.StatusCode)
			continue
		}

		break // no error, we can proceed
	}

	if err != nil {
		return
	}

	// Create a custom reader to monitor progress every time we read from response body stream
	progressReader := &progressReader{
		r: resp.Body,
		reporter: func(n int64) {
			t.currentSize.Add(n)
		},
	}

	t.setTotalSize(resp)

	cksum := getCksum(t.obj.Link, resp)
	if err := t.parent.t.PutObject(postFQN, progressReader, lom, cluster.ColdGet, cksum, started); err != nil {
		return internalErrorMessage(), err
	}
	return "", nil
}

func (t *singleObjectTask) setTotalSize(resp *http.Response) {
	totalSize := resp.ContentLength
	if totalSize > 0 {
		t.totalSize = totalSize
	}
}

func (t *singleObjectTask) downloadCloud(lom *cluster.LOM) (string, error) {
	if err, _ := t.parent.t.GetCold(t.downloadCtx, lom, true /* prefetch */); err != nil {
		return internalErrorMessage(), err
	}
	return "", nil
}

func (t *singleObjectTask) cancel() {
	t.cancelFunc()
}

// TODO: this should also inform somehow downloader status about being Aborted/canceled
// Probably we need to extend the persistent database (db.go) so that it will contain
// also information about specific tasks.
func (t *singleObjectTask) abort(statusMsg string, err error) {
	t.parent.stats.Add(stats.ErrDownloadCount, 1)

	dbErr := dlStore.persistError(t.id, t.obj.Objname, statusMsg)
	cmn.AssertNoErr(dbErr)

	dbErr = dlStore.incErrorCnt(t.id)
	cmn.AssertNoErr(dbErr)

	t.finishedCh <- err
}

func (t *singleObjectTask) persist() {
	_ = dlStore.persistTaskInfo(t.id, cmn.TaskDlInfo{
		Name:       t.obj.Objname,
		Downloaded: t.currentSize.Load(),
		Total:      t.totalSize,

		StartTime: t.started,
		EndTime:   t.ended,

		Running: false,
	})
}

func (t *singleObjectTask) waitForFinish() <-chan error {
	return t.finishedCh
}

func (t *singleObjectTask) String() string {
	return t.request.String()
}
