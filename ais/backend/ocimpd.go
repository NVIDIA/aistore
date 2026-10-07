//go:build oci

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"

	ocios "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// A child is claimed once by its worker or abandoned on cancellation.
// Closing done publishes err and rc to Read and Close.
type ociMPDChildStruct struct {
	mpd     *ociMPDStruct
	le      *list.Element
	done    chan struct{}
	start   int64
	length  int64
	err     error
	rc      io.ReadCloser
	claimed atomic.Bool
}

type ociMPDStruct struct {
	sync.Mutex                      // serializes accessed to .nextStart, .closeInProgress, & .childList
	ctx             context.Context // shared by the initial GET, children, and waits
	cancel          context.CancelFunc
	bp              *ocibp
	client          *ocios.ObjectStorageClient
	bucketName      string
	objectName      string
	objectSize      int64
	nextStart       int64 // == .objectSize once all children have been launched
	closeInProgress bool
	childList       *list.List
}

// when the initial request contains the whole object, without MPD children
type ociReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *ociReadCloser) Close() error {
	r.cancel()
	return r.ReadCloser.Close()
}

// getObjReaderViaMPD will accept the previously fetched initial object part's response, extract
// the full object size from that response, and multi-thread the downloading of the remaining
// object data. Up to bp.mpdMaxThreads such parts will be GET simultaneously and delivered (in
// the proper sequence) via the returned io.ReadCloser (res.R).
//
// The initial (up to) bp.mpdMaxThreads are launched via a call to launchChildren(). Each child
// performs their particular ranged GET and, upon completion or failure, will call launchChildren().
// Upon return, if there are no currently executing children, we know we are launching children,
// but note that those children remain in context in order to deliver, in sequence, the data of
// the object sequenced properly.
//
// Errors will be reported as and when each such child is called upon to return its data. The
// role of GetObjectReaderViaMPD is merely to set up those children to return their data (or
// errors obtaining their data) via the returned io.ReadCloser (res.R).
func (bp *ocibp) getObjReaderViaMPD(ctx context.Context, cancel context.CancelFunc, lom *core.LOM,
	client *ocios.ObjectStorageClient, resp *ocios.GetObjectResponse) (res core.GetReaderResult) {
	var (
		cloudBck      = lom.Bck().RemoteBck()
		err           error
		h             = cmn.BackendHelpers.OCI
		mpd           *ociMPDStruct
		mpdFirstChild *ociMPDChildStruct
		objectSize    int64
		partLength    int64
	)

	lom.SetCustomKey(cmn.SourceObjMD, apc.OCI)
	if v, ok := h.EncodeETag(resp.ETag); ok {
		lom.SetCustomKey(cmn.ETag, v)
	}
	if v, ok := h.EncodeCksum(resp.ContentMd5); ok {
		lom.SetCustomKey(cmn.MD5ObjMD, v)
	}

	if resp.ContentRange == nil {
		lom.ObjAttrs().Size = *resp.ContentLength
		res.R = &ociReadCloser{ReadCloser: resp.Content, cancel: cancel}
		res.Size = *resp.ContentLength
		return res
	}

	_, partLength, objectSize, err = cmn.ParseRangeHdr(*resp.ContentRange)
	if err != nil {
		res.ErrCode, res.Err = ociErrorToAISError("ParseRangeHdr", cloudBck.Name, lom.ObjName, *resp.ContentRange, err, int(http.StatusRequestedRangeNotSatisfiable))
		return res
	}

	if partLength == objectSize {
		lom.ObjAttrs().Size = *resp.ContentLength
		res.R = &ociReadCloser{ReadCloser: resp.Content, cancel: cancel}
		res.Size = *resp.ContentLength
		return res
	}

	mpdFirstChild = &ociMPDChildStruct{
		// .mpd will be filled in below
		done:   make(chan struct{}),
		start:  0,
		length: partLength,
		// .err already known to be nil
		rc: resp.Content,
	}

	mpdFirstChild.claimed.Store(true)
	close(mpdFirstChild.done)

	mpd = &ociMPDStruct{
		ctx:        ctx,
		cancel:     cancel,
		bp:         bp,
		client:     client,
		bucketName: cloudBck.Name,
		objectName: lom.ObjName,
		objectSize: objectSize,
		nextStart:  partLength,
		childList:  list.New(),
	}

	mpd.Lock()
	mpdFirstChild.mpd = mpd
	mpdFirstChild.le = mpd.childList.PushBack(mpdFirstChild)
	mpd.launchChildren()
	mpd.Unlock()

	res.R = mpd
	res.Size = objectSize

	return res
}

///////////////////////
// ociMPDChildStruct //
///////////////////////

func (mpdChild *ociMPDChildStruct) Run() {
	if !mpdChild.claimed.CompareAndSwap(false, true) {
		return
	}
	rangeHeader := cmn.MakeRangeHdr(mpdChild.start, mpdChild.length)

	req := ocios.GetObjectRequest{
		NamespaceName: &mpdChild.mpd.bp.namespace,
		BucketName:    &mpdChild.mpd.bucketName,
		ObjectName:    &mpdChild.mpd.objectName,
		Range:         &rangeHeader,
	}

	resp, err := mpdChild.mpd.client.GetObject(mpdChild.mpd.ctx, req)
	if err == nil {
		mpdChild.rc = resp.Content
	} else {
		mpdChild.rc = nil
		_, mpdChild.err = ociErrorToAISError("GetObject", mpdChild.mpd.bucketName, mpdChild.mpd.objectName, rangeHeader, err, resp)
	}

	close(mpdChild.done)
}

func (mpdChild *ociMPDChildStruct) String() string {
	return fmt.Sprintf(
		"[MPD] oc://%s/%s %d-%d/%d",
		mpdChild.mpd.bucketName,
		mpdChild.mpd.objectName,
		mpdChild.start,
		mpdChild.start+mpdChild.length-1,
		mpdChild.mpd.objectSize)
}

func (mpdChild *ociMPDChildStruct) abandon() {
	if mpdChild.claimed.CompareAndSwap(false, true) {
		mpdChild.err = context.Cause(mpdChild.mpd.ctx)
		close(mpdChild.done)
	}
}

func (mpdChild *ociMPDChildStruct) wait() error {
	select {
	case <-mpdChild.done:
	case <-mpdChild.mpd.ctx.Done():
		mpdChild.abandon()
		// A claimed worker publishes its result after the canceled SDK call returns.
		<-mpdChild.done
	}
	return mpdChild.err
}

//////////////////
// ociMPDStruct //
//////////////////

// launchChildren will append sufficient children of mpd.childList to either reach objectSize
// or exhaust mpd.bp.mpdMaxThreads (whichever comes first). If a close is in progress, no
// children will be launched however. Note the assumption that the mpd lock is held when called.
func (mpd *ociMPDStruct) launchChildren() {
	var (
		mpdChild   *ociMPDChildStruct
		partLength int64
	)

	if !mpd.closeInProgress && mpd.ctx.Err() == nil {
		for (mpd.nextStart < mpd.objectSize) && (int64(mpd.childList.Len()) < mpd.bp.mpdMaxThreads) {
			if (mpd.nextStart + mpd.bp.mpdSegmentMaxSize) <= mpd.objectSize {
				partLength = mpd.bp.mpdSegmentMaxSize
			} else {
				partLength = mpd.objectSize - mpd.nextStart
			}

			mpdChild = &ociMPDChildStruct{
				mpd:    mpd,
				done:   make(chan struct{}),
				start:  mpd.nextStart,
				length: partLength,
			}
			mpd.nextStart += partLength
			mpdChild.le = mpd.childList.PushBack(mpdChild)
			mpd.bp.pooledLaunchChild(mpdChild)
		}
	}
}

func (mpd *ociMPDStruct) Read(p []byte) (int, error) {
	mpd.Lock()
	defer mpd.Unlock()
	if err := context.Cause(mpd.ctx); err != nil {
		return 0, err
	}

	le := mpd.childList.Front()
	if le == nil {
		if mpd.nextStart == mpd.objectSize {
			return 0, io.EOF
		}
		// We have nothing to return just yet, but at least another
		// child can be launched to serve a subsequent call to Read()
		mpd.launchChildren()
		return 0, nil
	}

	mpdChild, ok := le.Value.(*ociMPDChildStruct)
	if !ok {
		return 0, errors.New("(*ociMPDStruct).Read() le.Value.(*ociMPDChildStruct) returned !ok")
	}
	if err := mpdChild.wait(); err != nil {
		return 0, err
	}

	n, err := mpdChild.rc.Read(p)
	if err == io.EOF {
		mpdChild.err = mpdChild.rc.Close()
		if mpdChild.err == nil {
			_ = mpd.childList.Remove(mpdChild.le)
			mpd.launchChildren()
			err = nil
		} else {
			err = mpdChild.err
		}
	}
	return n, err
}

func (mpd *ociMPDStruct) Close() (err error) {
	mpd.cancel() // before locking: Read may hold the mutex while blocked
	mpd.Lock()
	defer mpd.Unlock()
	if mpd.closeInProgress {
		err = errors.New("(*ociMPDStruct).Close() called while close already in progress or complete")
		return
	}
	mpd.closeInProgress = true
	mpd.abandonQueued() // Read can no longer enqueue children
	for {
		le := mpd.childList.Front()
		if le == nil {
			return
		}
		mpdChild, ok := le.Value.(*ociMPDChildStruct)
		if !ok {
			mpd.closeInProgress = false
			err = errors.New("(*ociMPDStruct).Close() le.Value.(*ociMPDChildStruct) returned !ok")
			return
		}
		_ = mpdChild.wait()
		if mpdChild.rc != nil {
			if e := mpdChild.rc.Close(); e != nil && err == nil {
				err = fmt.Errorf("(*ociMPDStruct).Close() mpdChild.Close() failed: %w", e)
			}
		}
		_ = mpd.childList.Remove(mpdChild.le)
	}
}

// remove this download's queued children while holding the shared pool lock
func (mpd *ociMPDStruct) abandonQueued() {
	bp := mpd.bp
	bp.Lock()
	defer bp.Unlock()
	for le := bp.mpChildPendingList.Front(); le != nil; {
		next := le.Next()
		if child, ok := le.Value.(*ociMPDChildStruct); ok && child.mpd == mpd {
			bp.mpChildPendingList.Remove(le)
			child.abandon()
		}
		le = next
	}
}
