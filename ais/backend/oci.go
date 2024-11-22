//go:build oci

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

const (
	maxPageSizeMin                     = 1
	maxPageSizeMax                     = 1000
	maxPageSizeDefault                 = maxPageSizeMax
	maxDownloadSegmentSizeMin          = 4 * 1024
	maxDownloadSegmentSizeMax          = 256 * 1024 * 1024
	maxDownloadSegmentSizeDefault      = maxDownloadSegmentSizeMax
	multiPartDownloadThresholdMin      = 8 * 1024
	multiPartDownloadThresholdMax      = 512 * 1024
	multiPartDownloadThresholdDefault  = multiPartDownloadThresholdMax
	multiPartDownloadMaxThreadsMin     = 4
	multiPartDownloadMaxThreadsMax     = 64
	multiPartDownloadMaxThreadsDefault = 16
	maxUploadSegmentSizeMin            = 4 * 1024
	maxUploadSegmentSizeMax            = 256 * 1024 * 1024
	maxUploadSegmentSizeDefault        = maxDownloadSegmentSizeMax
	multiPartUploadThresholdMin        = 8 * 1024
	multiPartUploadThresholdMax        = 512 * 1024
	multiPartUploadThresholdDefault    = multiPartUploadThresholdMax
	multiPartUploadMaxThreadsMin       = 4
	multiPartUploadMaxThreadsMax       = 64
	multiPartUploadMaxThreadsDefault   = 16
)

type ocibp struct {
	t                           core.TargetPut
	mm                          *memsys.MMSA
	configurationProvider       common.ConfigurationProvider
	compartmentOCID             string
	maxPageSize                 uint64
	maxDownloadSegmentSize      uint64
	multiPartDownloadThreshold  uint64
	multiPartDownloadMaxThreads uint64
	maxUploadSegmentSize        uint64
	multiPartUploadThreshold    uint64
	multiPartUploadMaxThreads   uint64
	objectStorageClient         objectstorage.ObjectStorageClient
	namespace                   string
	base
}

func NewOCI(t core.TargetPut, tstats stats.Tracker) (core.Backend, error) {
	var (
		err                  error
		getNamespaceRequest  objectstorage.GetNamespaceRequest
		getNamespaceResponse objectstorage.GetNamespaceResponse
	)

	bp := &ocibp{
		t:    t,
		mm:   t.PageMM(),
		base: base{provider: apc.AWS},
	}

	bp.configurationProvider = common.NewRawConfigurationProvider(
		os.Getenv(env.OCI.TenancyOCID),
		os.Getenv(env.OCI.UserOCID),
		os.Getenv(env.OCI.Region),
		os.Getenv(env.OCI.Fingerprint),
		os.Getenv(env.OCI.PrivateKey),
		nil)

	bp.compartmentOCID = os.Getenv(env.OCI.CompartmentOCID)

	bp.maxPageSize, err = fetchFromEnvOrDefault(env.OCI.MaxPageSize, maxPageSizeMin, maxPageSizeMax, maxPageSizeDefault, false)
	if err != nil {
		return nil, err
	}
	bp.maxDownloadSegmentSize, err = fetchFromEnvOrDefault(env.OCI.MaxDownloadSegmentSize, maxDownloadSegmentSizeMin, maxDownloadSegmentSizeMax, maxDownloadSegmentSizeDefault, true)
	if err != nil {
		return nil, err
	}
	bp.multiPartDownloadThreshold, err = fetchFromEnvOrDefault(env.OCI.MultiPartDownloadThreshold, multiPartDownloadThresholdMin, multiPartDownloadThresholdMax, multiPartDownloadThresholdDefault, true)
	if err != nil {
		return nil, err
	}
	bp.multiPartDownloadMaxThreads, err = fetchFromEnvOrDefault(env.OCI.MultiPartDownloadMaxThreads, multiPartDownloadMaxThreadsMin, multiPartDownloadMaxThreadsMax, multiPartDownloadMaxThreadsDefault, false)
	if err != nil {
		return nil, err
	}
	bp.maxUploadSegmentSize, err = fetchFromEnvOrDefault(env.OCI.MaxUploadSegmentSize, maxUploadSegmentSizeMin, maxUploadSegmentSizeMax, maxUploadSegmentSizeDefault, true)
	if err != nil {
		return nil, err
	}
	bp.multiPartUploadThreshold, err = fetchFromEnvOrDefault(env.OCI.MultiPartUploadThreshold, multiPartUploadThresholdMin, multiPartUploadThresholdMax, multiPartUploadThresholdDefault, true)
	if err != nil {
		return nil, err
	}
	bp.multiPartUploadMaxThreads, err = fetchFromEnvOrDefault(env.OCI.MultiPartUploadMaxThreads, multiPartUploadMaxThreadsMin, multiPartUploadMaxThreadsMax, multiPartUploadMaxThreadsDefault, false)
	if err != nil {
		return nil, err
	}

	bp.objectStorageClient, err = objectstorage.NewObjectStorageClientWithConfigurationProvider(bp.configurationProvider)
	if err != nil {
		return nil, err
	}

	getNamespaceRequest = objectstorage.GetNamespaceRequest{}

	getNamespaceResponse, err = bp.objectStorageClient.GetNamespace(context.Background(), getNamespaceRequest)
	if err != nil {
		return nil, err
	}

	bp.namespace = *getNamespaceResponse.Value

	bp.base.init(t.Snode(), tstats)

	return bp, nil
}

func fetchFromEnvOrDefault(envName string, envMin, envMax, envDefault uint64, allowByteMultiplier bool) (val uint64, err error) {
	var (
		envValueAsInt    int
		envValueAsString string
	)

	envValueAsString = os.Getenv(envName)
	if envValueAsString == "" {
		val = envDefault
		err = nil
		return
	}

	if allowByteMultiplier {
		val, err = byteSizeParse(envValueAsString)
		if err != nil {
			err = fmt.Errorf("byteSizeParse(envValueAsString == \"%s\") failed: %v", envValueAsString, err)
			return
		}
	} else {
		envValueAsInt, err = strconv.Atoi(envValueAsString)
		if err != nil {
			err = fmt.Errorf("strconv.Atoi(envValueAsString == \"%s\") failed: %v", envValueAsString, err)
			return
		}
		if envValueAsInt < 0 {
			err = fmt.Errorf("strconv.Atoi(envValueAsString == \"%s\") returned negative value: %v", envValueAsString, envValueAsInt)
			return
		}
		val = uint64(envValueAsInt)
	}

	if val < envMin {
		err = fmt.Errorf("envValue (%v) < envMin (%v)", val, envMin)
		return
	}
	if val > envMax {
		err = fmt.Errorf("envValue (%v) > envMax (%v)", val, envMax)
		return
	}

	err = nil
	return
}

func ociRawRequestStatusToAISError(rawResponse *http.Response) (ecode int) {
	if rawResponse == nil {
		ecode = http.StatusInternalServerError
	} else {
		ecode = rawResponse.StatusCode
	}
	return
}

// as core.Backend --------------------------------------------------------------

// func (*ocibp) ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (ecode int, err error) {
func (*ocibp) ListObjects(_ *meta.Bck, _ *apc.LsoMsg, _ *cmn.LsoRes) (ecode int, err error) {
	ecode = http.StatusNotImplemented
	err = errors.New("[TODO] implement func (*ocibp) ListObjects()")
	fmt.Printf("[UNDO] Returning from ListObjects() with ecode: %v err: %v\n", ecode, err)
	ecode = 0 // [UNDO]
	err = nil // [UNDO]
	return
}

func (bp *ocibp) ListBuckets(_ cmn.QueryBcks) (bcks cmn.Bcks, ecode int, err error) {
	var (
		idx                 int
		item                objectstorage.BucketSummary
		listBucketsRequest  objectstorage.ListBucketsRequest
		listBucketsResponse objectstorage.ListBucketsResponse
	)

	listBucketsRequest = objectstorage.ListBucketsRequest{
		NamespaceName: &bp.namespace,
		CompartmentId: &bp.compartmentOCID,
	}

	listBucketsResponse, err = bp.objectStorageClient.ListBuckets(context.Background(), listBucketsRequest)
	if err != nil {
		ecode = ociRawRequestStatusToAISError(listBucketsResponse.RawResponse)
		return
	}

	bcks = make(cmn.Bcks, len(listBucketsResponse.Items))

	for idx, item = range listBucketsResponse.Items {
		bcks[idx] = cmn.Bck{
			Name:     *item.Name,
			Provider: apc.OCI,
		}
	}

	ecode = 0
	err = nil

	return
}

func (bp *ocibp) PutObj(r io.ReadCloser, lom *core.LOM, _ *http.Request) (ecode int, err error) {
	var (
		putObjectRequest  objectstorage.PutObjectRequest
		putObjectResponse objectstorage.PutObjectResponse
	)

	// [TODO] Need to implement multi-threaded PUT when "length" exceeds bp.multiPartUploadThreshold

	putObjectRequest = objectstorage.PutObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &lom.Bck().Name,
		ObjectName:    &lom.ObjName,
		PutObjectBody: r,
	}

	putObjectResponse, err = bp.objectStorageClient.PutObject(context.Background(), putObjectRequest)
	if err != nil {
		ecode = ociRawRequestStatusToAISError(putObjectResponse.RawResponse)
		return
	}

	cos.Close(r)

	ecode = 0
	err = nil

	return
}

func (bp *ocibp) DeleteObj(lom *core.LOM) (ecode int, err error) {
	var (
		deleteObjectRequest  objectstorage.DeleteObjectRequest
		deleteObjectResponse objectstorage.DeleteObjectResponse
	)

	deleteObjectRequest = objectstorage.DeleteObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &lom.Bck().Name,
		ObjectName:    &lom.ObjName,
	}

	deleteObjectResponse, err = bp.objectStorageClient.DeleteObject(context.Background(), deleteObjectRequest)
	if err != nil {
		ecode = ociRawRequestStatusToAISError(deleteObjectResponse.RawResponse)
		return
	}

	ecode = 0
	err = nil

	return
}

func (bp *ocibp) HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, ecode int, err error) {
	var (
		headBucketRequest  objectstorage.HeadBucketRequest
		headBucketResponse objectstorage.HeadBucketResponse
	)

	headBucketRequest = objectstorage.HeadBucketRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &bck.Name,
	}

	headBucketResponse, err = bp.objectStorageClient.HeadBucket(ctx, headBucketRequest)
	if err != nil {
		ecode = ociRawRequestStatusToAISError(headBucketResponse.RawResponse)
		return
	}

	bckProps = make(cos.StrKVs, 4)
	bckProps[apc.HdrBackendProvider] = apc.OCI

	ecode = 0
	err = nil

	return
}

func (bp *ocibp) HeadObj(ctx context.Context, lom *core.LOM, _ *http.Request) (objAttrs *cmn.ObjAttrs, ecode int, err error) {
	var (
		headObjectRequest  objectstorage.HeadObjectRequest
		headObjectResponse objectstorage.HeadObjectResponse
	)

	headObjectRequest = objectstorage.HeadObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &lom.Bck().Name,
		ObjectName:    &lom.ObjName,
	}

	headObjectResponse, err = bp.objectStorageClient.HeadObject(ctx, headObjectRequest)
	if err != nil {
		ecode = ociRawRequestStatusToAISError(headObjectResponse.RawResponse)
		return
	}

	objAttrs = &cmn.ObjAttrs{
		CustomMD: make(cos.StrKVs, 4),
		Size:     headObjectResponse.RawResponse.ContentLength,
	}
	objAttrs.CustomMD[apc.HdrBackendProvider] = apc.OCI

	ecode = 0
	err = nil

	return
}

func (bp *ocibp) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT, _ *http.Request) (ecode int, err error) {
	var (
		putParams *core.PutParams
		res       core.GetReaderResult
	)

	res = bp.GetObjReader(ctx, lom, 0, 0)
	if res.Err != nil {
		ecode = res.ErrCode
		err = res.Err
		return
	}

	ecode = 0

	putParams = allocPutParams(res, owt)
	err = bp.t.PutObject(lom, putParams)
	core.FreePutParams(putParams)

	return
}

func (bp *ocibp) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
	var (
		err               error
		getObjectRequest  objectstorage.GetObjectRequest
		getObjectResponse objectstorage.GetObjectResponse
		rangeHeader       string
	)

	// [TODO] Need to implement multi-threaded GET when "length" exceeds bp.multiPartDownloadThreshold

	getObjectRequest = objectstorage.GetObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &lom.Bck().Name,
		ObjectName:    &lom.ObjName,
	}
	if length > 0 {
		rangeHeader = cmn.MakeRangeHdr(offset, length)
		getObjectRequest.Range = &rangeHeader
	}

	getObjectResponse, err = bp.objectStorageClient.GetObject(ctx, getObjectRequest)
	if err != nil {
		res.Err = err
		res.ErrCode = ociRawRequestStatusToAISError(getObjectResponse.RawResponse)
		return
	}

	res.R = getObjectResponse.Content
	res.Size = *getObjectResponse.ContentLength

	return
}
