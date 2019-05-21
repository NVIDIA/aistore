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

type (
	task struct {
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

func (t *task) download() {
	var (
		statusMsg string
		err       error
	)
	lom, errstr := cluster.LOM{T: t.parent.t, Bucket: t.bucket, Objname: t.obj.Objname}.Init()
	if errstr == "" {
		_, errstr = lom.Load(true)
	}
	if errstr != "" {
		t.abort(internalErrorMessage(), errors.New(errstr))
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

	t.parent.stats.AddMany(
		stats.NamedVal64{Name: stats.DownloadSize, Val: t.currentSize.Load()},
		stats.NamedVal64{Name: stats.DownloadLatency, Val: int64(time.Since(t.started))},
	)
	t.parent.XactDemandBase.ObjectsInc()
	t.parent.XactDemandBase.BytesAdd(t.currentSize.Load())
	t.finishedCh <- nil
}

func (t *task) downloadLocal(lom *cluster.LOM, started time.Time) (string, error) {
	postFQN := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)

	// Create request
	httpReq, err := http.NewRequest(http.MethodGet, t.obj.Link, nil)
	if err != nil {
		return "", err
	}

	requestWithContext := httpReq.WithContext(t.downloadCtx)
	response, err := httpClient.Do(requestWithContext)
	if err != nil {
		return httpClientErrorMessage(err.(*url.Error)), err // Error returned by httpClient.Do() is a *url.Error
	}
	if response.StatusCode >= http.StatusBadRequest {
		return httpRequestErrorMessage(t.obj.Link, response), fmt.Errorf("status code: %d", response.StatusCode)
	}

	// Create a custom reader to monitor progress every time we read from response body stream
	progressReader := &progressReader{
		r: response.Body,
		reporter: func(n int64) {
			t.currentSize.Add(n)
		},
	}

	t.setTotalSize(response)

	cksum := getCksum(t.obj.Link, response)
	if err := t.parent.t.Receive(postFQN, progressReader, lom, cluster.ColdGet, cksum, started); err != nil {
		return internalErrorMessage(), err
	}
	return "", nil
}

func (t *task) setTotalSize(resp *http.Response) {
	totalSize := resp.ContentLength
	if totalSize > 0 {
		t.totalSize = totalSize
	}
}

func (t *task) downloadCloud(lom *cluster.LOM) (string, error) {
	if errstr, _ := t.parent.t.GetCold(t.downloadCtx, lom, true /* prefetch */); errstr != "" {
		return internalErrorMessage(), errors.New(errstr)
	}
	return "", nil
}

func (t *task) cancel() {
	t.cancelFunc()
}

// TODO: this should also inform somehow downloader status about being aborted/canceled
// Probably we need to extend the persistent database (db.go) so that it will contain
// also information about specific tasks.
func (t *task) abort(statusMsg string, err error) {
	t.parent.stats.Add(stats.ErrDownloadCount, 1)

	dbErr := t.parent.db.addError(t.id, t.obj.Objname, statusMsg)
	cmn.AssertNoErr(dbErr)

	t.finishedCh <- err
}

func (t *task) persist() {
	t.parent.db.persistTask(t.id, cmn.TaskDlInfo{
		Name:       t.obj.Objname,
		Downloaded: t.currentSize.Load(),
		Total:      t.totalSize,

		StartTime: t.started,
		EndTime:   t.ended,

		Running: false,
	})
}

func (t *task) waitForFinish() <-chan error {
	return t.finishedCh
}

func (t *task) String() string {
	return t.request.String()
}
