// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"fmt"
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
	// Number of retries when doing a request to external resource.
	retryCnt         = 3
	internalErrorMsg = "internal server error"
)

type (
	singleObjectTask struct {
		parent *Downloader
		*request

		started time.Time
		ended   time.Time

		currentSize atomic.Int64 // the current size of the file (updated as the download progresses)
		totalSize   int64        // the total size of the file (nonzero only if Content-Length header was provided by the source of the file)

		downloadCtx context.Context    // context with cancel function
		cancelFunc  context.CancelFunc // used to cancel the download after the request commences
	}
)

func (t *singleObjectTask) download() {
	lom := &cluster.LOM{T: t.parent.t, ObjName: t.obj.ObjName}
	err := lom.Init(t.bck)
	if err == nil {
		err = lom.Load()
	}
	if err != nil && !os.IsNotExist(err) {
		t.markFailed(internalErrorMsg)
		return
	}
	if err == nil {
		t.markFailed(t.obj.ObjName + " already exists")
		return
	}

	if glog.V(4) {
		glog.Infof("Starting download for %v", t)
	}

	t.started = time.Now()
	lom.SetAtimeUnix(t.started.UnixNano())
	if t.obj.FromCloud {
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

func (t *singleObjectTask) downloadLocal(lom *cluster.LOM, started time.Time) error {
	var (
		postFQN = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)
		req     *http.Request
		resp    *http.Response
		err     error
	)

	for i := 0; i < retryCnt; i++ {
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
		if err == nil && resp.StatusCode <= http.StatusOK {
			break
		}
		if err == nil {
			err = fmt.Errorf("downloading %s failed with status %d", t.obj.Link, resp.StatusCode)
			resp.Body.Close()
		}
	}

	if err != nil {
		return err
	}

	// Create a custom reader to monitor progress every time we read from response body stream
	defer resp.Body.Close()
	progressReader := &progressReader{
		r: resp.Body,
		reporter: func(n int64) {
			t.currentSize.Add(n)
		},
	}

	t.setTotalSize(resp)

	cksum := getCksum(t.obj.Link, resp)
	err = t.parent.t.PutObject(cluster.PutObjectParams{
		LOM:          lom,
		Reader:       progressReader,
		WorkFQN:      postFQN,
		RecvType:     cluster.ColdGet,
		Cksum:        cksum,
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

func (t *singleObjectTask) setTotalSize(resp *http.Response) {
	if resp.ContentLength > 0 {
		t.totalSize = resp.ContentLength
	}
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

	dlStore.persistError(t.id, t.obj.ObjName, statusMsg)
	if err := dlStore.incErrorCnt(t.id); err != nil {
		glog.Error(err)
	}
}

func (t *singleObjectTask) persist() {
	_ = dlStore.persistTaskInfo(t.id, TaskDlInfo{
		Name:       t.obj.ObjName,
		Downloaded: t.currentSize.Load(),
		Total:      t.totalSize,

		StartTime: t.started,
		EndTime:   t.ended,

		Running: false,
	})
}

func (t *singleObjectTask) String() string {
	return t.request.String()
}
