//go:build oci

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
	ocios "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

type ociMPDChildStruct struct {
	sync.WaitGroup // .Wait() will return when either .err is set to non-nil or .rc is available
	mpd            *ociMPDStruct
	le             *list.Element
	start          int64
	length         int64
	err            error
	rc             io.ReadCloser
}

type ociMPDStruct struct {
	sync.Mutex      // serializes accessed to .nextStart, .closeInProgress, & .childList
	ctx             context.Context
	bp              *ocibp
	bucketName      string
	objectName      string
	objectSize      int64
	nextStart       int64 // == .objectSize once all children have been launched
	closeInProgress bool
	childList       *list.List
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
func (bp *ocibp) getObjReaderViaMPD(ctx context.Context, lom *core.LOM, resp *ocios.GetObjectResponse) (res core.GetReaderResult) {
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
		res.R = resp.Content
		res.Size = *resp.ContentLength
		return
	}

	_, partLength, objectSize, err = cmn.ParseRangeHdr(*resp.ContentRange)
	if err != nil {
		res.Err = err
		res.ErrCode = ociStatus(nil)
		return
	}

	if partLength == objectSize {
		lom.ObjAttrs().Size = *resp.ContentLength
		res.R = resp.Content
		res.Size = *resp.ContentLength
		return
	}

	mpdFirstChild = &ociMPDChildStruct{
		// sync.WaitGroup will never be .Add(1)'d, so .Wait() will immediately complete
		// .mpd will be filled in below
		start:  0,
		length: partLength,
		// .err already known to be nil
		rc: resp.Content,
	}

	mpd = &ociMPDStruct{
		ctx:        ctx,
		bp:         bp,
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

	return
}

// launchChildren will append sufficient children of mpd.childList to either reach objectSize
// or exhaust mpd.bp.mpdMaxThreads (whichever comes first). If a close is in progress, no
// children will be launched however. Note the assumption that the mpd lock is held when called.
func (mpd *ociMPDStruct) launchChildren() {
	var (
		mpdChild   *ociMPDChildStruct
		partLength int64
	)

	if !mpd.closeInProgress {
		for (mpd.nextStart < mpd.objectSize) && (int64(mpd.childList.Len()) < mpd.bp.mpdMaxThreads) {
			if (mpd.nextStart + mpd.bp.mpdSegmentMaxSize) <= mpd.objectSize {
				partLength = mpd.bp.mpdSegmentMaxSize
			} else {
				partLength = mpd.objectSize - mpd.nextStart
			}
			mpdChild = &ociMPDChildStruct{
				mpd:    mpd,
				start:  mpd.nextStart,
				length: partLength,
			}
			mpd.nextStart += partLength
			mpdChild.Add(1)
			mpdChild.le = mpd.childList.PushBack(mpdChild)
			mpd.bp.pooledLauchChild(mpdChild)
		}
	}
}

func (mpdChild *ociMPDChildStruct) Run() {
	rangeHeader := cmn.MakeRangeHdr(mpdChild.start, mpdChild.length)

	req := ocios.GetObjectRequest{
		NamespaceName: &mpdChild.mpd.bp.namespace,
		BucketName:    &mpdChild.mpd.bucketName,
		ObjectName:    &mpdChild.mpd.objectName,
		Range:         &rangeHeader,
	}

	resp, err := mpdChild.mpd.bp.client.GetObject(mpdChild.mpd.ctx, req)
	if err == nil {
		mpdChild.rc = resp.Content
	} else {
		mpdChild.err = err
	}

	mpdChild.Done()
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

func (mpd *ociMPDStruct) Read(p []byte) (n int, err error) {
	mpd.Lock()
	defer mpd.Unlock()
	le := mpd.childList.Front()
	if le == nil {
		if mpd.nextStart == mpd.objectSize {
			err = io.EOF
		} else {
			// We have nothing to return just yet, but at least another
			// child can be launched to serve a subsequent call to Read()
			mpd.launchChildren()
		}
		return
	}
	mpdChild, ok := le.Value.(*ociMPDChildStruct)
	if !ok {
		err = errors.New("(*ociMPDStruct).Read() le.Value.(*ociMPDChildStruct) returned !ok")
		return
	}
	mpdChild.Wait()
	if mpdChild.err != nil {
		err = mpdChild.err
		return
	}
	n, err = mpdChild.rc.Read(p)
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
	return
}

func (mpd *ociMPDStruct) Close() (err error) {
	mpd.Lock()
	defer mpd.Unlock()
	if mpd.closeInProgress {
		err = errors.New("(*ociMPDStruct).Close() called while close already in progress or complete")
		return
	}
	mpd.closeInProgress = true
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
		mpdChild.Wait()
		err = mpdChild.rc.Close()
		if err != nil {
			err = fmt.Errorf("(*ociMPDStruct).Close() mpdChild.Close() failed: %v", err)
			return
		}
		_ = mpd.childList.Remove(mpdChild.le)
	}
}
