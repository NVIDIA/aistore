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

type ociMPUChildStruct struct {
	mpu     *ociMPUStruct
	le      *list.Element
	partNum int
	start   int64
	length  int64
	buf     []byte
	bufPos  int
	eTag    string
}

type ociMPUStruct struct {
	sync.Mutex      // serializes access to .abortInProgress, .childList, .childSlice, err, & .rc
	sync.WaitGroup  // .Wait() will return when last .childList element completes & no others are needed
	ctx             context.Context
	bp              *ocibp
	namespaceName   string
	bucketName      string
	objectName      string
	objectSize      int64
	uploadID        string
	totalParts      int
	nextPartNum     int   // Note: must be in [1:10000]
	nextStart       int64 // == .objectSize once all children have been launched
	abortInProgress bool
	childList       *list.List // only contains currently active children
	childSlice      []*ociMPUChildStruct
	err             []error
	rc              io.ReadCloser
}

// putObjViaMPU will split the supplied data into multiple parts and leverage
// the OCI support for MultiPartUpload to perform the upload with higher
// bandwidth and lower latency. Up to bp.mpuMaxThreads such parts will be
// PUT simultaneously and, once all parts have been uploaded, logically
// arranged to appear as a single object.
//
// The initial (up to) bp.mpuMaxThreads are launched via a call to launchChildren().
// Each child performs their particular part PUT and, upon completion or failure,
// will call launchChildren(). Upon return, if there are no currently executing
// children, we know we are done and my inform the parent that all the parts have
// been uploaded or there has been some error.
//
// Finally, if there are no errors, putObjViaMPU() can perform the MPU "complete"
// operation that logically combines all those uploaded parts. If there were errors,
// the MPU operation is aborted.
func (bp *ocibp) putObjViaMPU(r io.ReadCloser, lom *core.LOM, objectSize int64) (int, error) {
	var (
		cloudBck   = lom.Bck().RemoteBck()
		err        error
		mpu        *ociMPUStruct
		mpuChild   *ociMPUChildStruct
		status     int
		totalParts int
	)

	totalParts = int(objectSize + bp.mpuSegmentMaxSize - 1)
	totalParts /= int(bp.mpuSegmentMaxSize)

	mpu = &ociMPUStruct{
		ctx:           context.Background(),
		bp:            bp,
		namespaceName: bp.namespace,
		bucketName:    cloudBck.Name,
		objectName:    lom.ObjName,
		objectSize:    objectSize,
		totalParts:    totalParts,
		nextPartNum:   1,
		childList:     list.New(),
		childSlice:    make([]*ociMPUChildStruct, 0, totalParts),
		err:           make([]error, 0, totalParts),
		rc:            r,
	}

	createReq := ocios.CreateMultipartUploadRequest{
		NamespaceName: &mpu.namespaceName,
		BucketName:    &mpu.bucketName,
		CreateMultipartUploadDetails: ocios.CreateMultipartUploadDetails{
			Object: &mpu.objectName,
		},
	}

	createResp, err := bp.client.CreateMultipartUpload(mpu.ctx, createReq)
	if err != nil {
		return ociStatus(createResp.RawResponse), err
	}

	mpu.uploadID = *createResp.MultipartUpload.UploadId

	mpu.Lock()
	mpu.Add(1)
	mpu.launchChildren()
	mpu.Unlock()
	mpu.Wait()

	if len(mpu.err) > 0 {
		abortReq := ocios.AbortMultipartUploadRequest{
			NamespaceName: &mpu.namespaceName,
			BucketName:    &mpu.bucketName,
			ObjectName:    &mpu.objectName,
			UploadId:      &mpu.uploadID,
		}

		abortResp, err := bp.client.AbortMultipartUpload(mpu.ctx, abortReq)
		if err == nil {
			status = ociStatus(nil)
			errConcat := ""
			for _, err = range mpu.err {
				if errConcat == "" {
					errConcat = "[]string{\"" + err.Error() + "\""
				} else {
					errConcat += ",\"" + err.Error() + "\""
				}
			}
			errConcat += "}"
			err = errors.New(errConcat)
		} else {
			status = ociStatus(abortResp.RawResponse)
		}
		return status, err
	}

	commitReq := ocios.CommitMultipartUploadRequest{
		NamespaceName: &mpu.namespaceName,
		BucketName:    &mpu.bucketName,
		ObjectName:    &mpu.objectName,
		UploadId:      &mpu.uploadID,
		CommitMultipartUploadDetails: ocios.CommitMultipartUploadDetails{
			PartsToCommit: make([]ocios.CommitMultipartUploadPartDetails, 0, mpu.nextPartNum),
		},
	}

	for _, mpuChild = range mpu.childSlice {
		commitReq.CommitMultipartUploadDetails.PartsToCommit = append(
			commitReq.CommitMultipartUploadDetails.PartsToCommit,
			ocios.CommitMultipartUploadPartDetails{
				PartNum: &mpuChild.partNum,
				Etag:    &mpuChild.eTag,
			})
	}

	commitResp, err := bp.client.CommitMultipartUpload(mpu.ctx, commitReq)
	if err != nil {
		return ociStatus(commitResp.RawResponse), err
	}

	h := cmn.BackendHelpers.OCI

	lom.SetCustomKey(apc.HdrBackendProvider, apc.OCI)
	if v, ok := h.EncodeETag(commitResp.ETag); ok {
		lom.SetCustomKey(cmn.ETag, v)
	}
	if v, ok := h.EncodeCksum(commitResp.OpcMultipartMd5); ok {
		lom.SetCustomKey(cmn.MD5ObjMD, v)
	}

	return 0, nil
}

// launchChildren will append sufficient children of mpu.childList to either reach objectSize
// or exhaust mpu.bp.mpuMaxThreads (whichever comes first). Note the assumption that the mpu
// lock is held when called.
func (mpu *ociMPUStruct) launchChildren() {
	var (
		buf        []byte
		bufLength  int64
		err        error
		mpuChild   *ociMPUChildStruct
		n          int
		partLength int64
	)

	if !mpu.abortInProgress {
		for (mpu.nextStart < mpu.objectSize) && (int64(mpu.childList.Len()) < mpu.bp.mpuMaxThreads) {
			if (mpu.nextStart + mpu.bp.mpuSegmentMaxSize) <= mpu.objectSize {
				partLength = mpu.bp.mpuSegmentMaxSize
			} else {
				partLength = mpu.objectSize - mpu.nextStart
			}
			buf = make([]byte, partLength)
			bufLength = 0
			for bufLength < partLength {
				n, err = mpu.rc.Read(buf[bufLength:])
				if err != nil {
					mpu.abortInProgress = true
					mpu.err = append(mpu.err, err)
					return
				}
				bufLength += int64(n)
			}
			mpuChild = &ociMPUChildStruct{
				mpu:     mpu,
				partNum: mpu.nextPartNum,
				start:   mpu.nextStart,
				length:  partLength,
				buf:     buf,
			}
			mpu.childSlice = append(mpu.childSlice, mpuChild)
			mpu.nextPartNum++
			mpu.nextStart += partLength
			mpuChild.le = mpu.childList.PushBack(mpuChild)
			mpu.bp.pooledLauchChild(mpuChild)
		}
	}
}

func (mpuChild *ociMPUChildStruct) Run() {
	req := ocios.UploadPartRequest{
		NamespaceName:  &mpuChild.mpu.namespaceName,
		BucketName:     &mpuChild.mpu.bucketName,
		ObjectName:     &mpuChild.mpu.objectName,
		UploadId:       &mpuChild.mpu.uploadID,
		UploadPartNum:  &mpuChild.partNum,
		ContentLength:  &mpuChild.length,
		UploadPartBody: mpuChild,
	}

	resp, err := mpuChild.mpu.bp.client.UploadPart(mpuChild.mpu.ctx, req)

	mpuChild.mpu.Lock()
	defer mpuChild.mpu.Unlock()

	if err == nil {
		mpuChild.eTag = *resp.ETag
	} else {
		err = fmt.Errorf("[%d] %s", mpuChild.partNum, err)
		mpuChild.mpu.err = append(mpuChild.mpu.err, err)
		mpuChild.mpu.abortInProgress = true
	}

	_ = mpuChild.mpu.childList.Remove(mpuChild.le)

	mpuChild.mpu.launchChildren()
	if mpuChild.mpu.childList.Len() == 0 {
		// We are the last child to finish... so wake up PutObj()
		mpuChild.mpu.Done()
	}
}

func (mpuChild *ociMPUChildStruct) String() string {
	return fmt.Sprintf(
		"[MPU] oc://%s/%s %d-%d/%d (partNum:%d)",
		mpuChild.mpu.bucketName,
		mpuChild.mpu.objectName,
		mpuChild.start,
		mpuChild.start+mpuChild.length-1,
		mpuChild.mpu.objectSize,
		mpuChild.partNum)
}

func (mpuChild *ociMPUChildStruct) Read(p []byte) (n int, err error) {
	n = int(mpuChild.length) - mpuChild.bufPos
	if n > len(p) {
		n = len(p)
	}
	if n > 0 {
		copy(p, mpuChild.buf[mpuChild.bufPos:(mpuChild.bufPos+n)])
		mpuChild.bufPos += n
	}
	if int64(mpuChild.bufPos) == mpuChild.length {
		err = io.EOF
	}
	return
}

func (*ociMPUChildStruct) Close() (err error) {
	// Nothing to do here
	return
}
