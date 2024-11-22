//go:build oci

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

// Outstanding [TODO] items:
//   0) Make coding style in this file match the rest of the codebase:
//        a) too long/ugly var/field names
//        b) cumbersome function names
//        c) line length explosions (prefer 80 col... but *not* 1-line-per-arg style)
//   1) Need to parse OCI's ~/.oci/config file for non-ENV defaults (for req'd settings)
//   2) Validate ListObjects() should only return Name & Size in all cases (or improve)
//   3) Handle non-descending ListObjects() case (including listing of "virtual" directories)
//   4) Multi-Segment-Upload utilization (for fast/large object PUTs)... if practical
//   5) Add support for object versioning
//   6) Multi-Segment-Download utilization (for fast/large object GETs)... if practical

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	ociCommon "github.com/oracle/oci-go-sdk/v65/common"
	ociObjectstorage "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

const (
	maxPageSizeMin                     = 1
	maxPageSizeMax                     = 1000
	maxPageSizeDefault                 = maxPageSizeMax
	maxDownloadSegmentSizeMin          = 4 * cos.KiB
	maxDownloadSegmentSizeMax          = 256 * cos.MiB
	maxDownloadSegmentSizeDefault      = maxDownloadSegmentSizeMax
	multiPartDownloadThresholdMin      = 8 * cos.KiB
	multiPartDownloadThresholdMax      = 512 * cos.MiB
	multiPartDownloadThresholdDefault  = multiPartDownloadThresholdMax
	multiPartDownloadMaxThreadsMin     = 4
	multiPartDownloadMaxThreadsMax     = 64
	multiPartDownloadMaxThreadsDefault = 16
	maxUploadSegmentSizeMin            = 4 * cos.KiB
	maxUploadSegmentSizeMax            = 256 * cos.MiB
	maxUploadSegmentSizeDefault        = maxUploadSegmentSizeMax
	multiPartUploadThresholdMin        = 8 * cos.KiB
	multiPartUploadThresholdMax        = 512 * cos.MiB
	multiPartUploadThresholdDefault    = multiPartUploadThresholdMax
	multiPartUploadMaxThreadsMin       = 4
	multiPartUploadMaxThreadsMax       = 64
	multiPartUploadMaxThreadsDefault   = 16
)

type ocibp struct {
	t                           core.TargetPut
	configurationProvider       ociCommon.ConfigurationProvider
	compartmentOCID             string
	maxPageSize                 uint64
	maxDownloadSegmentSize      uint64
	multiPartDownloadThreshold  uint64
	multiPartDownloadMaxThreads uint64
	maxUploadSegmentSize        uint64
	multiPartUploadThreshold    uint64
	multiPartUploadMaxThreads   uint64
	client                      ociObjectstorage.ObjectStorageClient
	namespace                   string
	base
}

func NewOCI(t core.TargetPut, tstats stats.Tracker) (core.Backend, error) {
	var (
		err                  error
		getNamespaceRequest  ociObjectstorage.GetNamespaceRequest
		getNamespaceResponse ociObjectstorage.GetNamespaceResponse
	)

	bp := &ocibp{
		t:    t,
		base: base{provider: apc.AWS},
	}

	// [TODO] See alternatively oci-go-sdk/common/configuration.go's ConfigurationProviderFromFile()

	bp.configurationProvider = ociCommon.NewRawConfigurationProvider(
		os.Getenv(env.OCI.TenancyOCID),
		os.Getenv(env.OCI.UserOCID),
		os.Getenv(env.OCI.Region),
		os.Getenv(env.OCI.Fingerprint),
		os.Getenv(env.OCI.PrivateKey),
		nil)

	bp.compartmentOCID = os.Getenv(env.OCI.CompartmentOCID)

	bp.maxPageSize, err = fetchFromEnvOrDefault(env.OCI.MaxPageSize, maxPageSizeMin, maxPageSizeMax, maxPageSizeDefault)
	if err != nil {
		return nil, err
	}
	bp.maxDownloadSegmentSize, err = fetchFromEnvOrDefault(env.OCI.MaxDownloadSegmentSize, maxDownloadSegmentSizeMin, maxDownloadSegmentSizeMax, maxDownloadSegmentSizeDefault)
	if err != nil {
		return nil, err
	}
	bp.multiPartDownloadThreshold, err = fetchFromEnvOrDefault(env.OCI.MultiPartDownloadThreshold, multiPartDownloadThresholdMin, multiPartDownloadThresholdMax, multiPartDownloadThresholdDefault)
	if err != nil {
		return nil, err
	}
	bp.multiPartDownloadMaxThreads, err = fetchFromEnvOrDefault(env.OCI.MultiPartDownloadMaxThreads, multiPartDownloadMaxThreadsMin, multiPartDownloadMaxThreadsMax, multiPartDownloadMaxThreadsDefault)
	if err != nil {
		return nil, err
	}
	bp.maxUploadSegmentSize, err = fetchFromEnvOrDefault(env.OCI.MaxUploadSegmentSize, maxUploadSegmentSizeMin, maxUploadSegmentSizeMax, maxUploadSegmentSizeDefault)
	if err != nil {
		return nil, err
	}
	bp.multiPartUploadThreshold, err = fetchFromEnvOrDefault(env.OCI.MultiPartUploadThreshold, multiPartUploadThresholdMin, multiPartUploadThresholdMax, multiPartUploadThresholdDefault)
	if err != nil {
		return nil, err
	}
	bp.multiPartUploadMaxThreads, err = fetchFromEnvOrDefault(env.OCI.MultiPartUploadMaxThreads, multiPartUploadMaxThreadsMin, multiPartUploadMaxThreadsMax, multiPartUploadMaxThreadsDefault)
	if err != nil {
		return nil, err
	}

	bp.client, err = ociObjectstorage.NewObjectStorageClientWithConfigurationProvider(bp.configurationProvider)
	if err != nil {
		return nil, err
	}

	getNamespaceRequest = ociObjectstorage.GetNamespaceRequest{}

	getNamespaceResponse, err = bp.client.GetNamespace(context.Background(), getNamespaceRequest)
	if err != nil {
		return nil, err
	}

	bp.namespace = *getNamespaceResponse.Value

	bp.base.init(t.Snode(), tstats)

	return bp, nil
}

func fetchFromEnvOrDefault(envName string, envMin, envMax, envDefault uint64) (val uint64, err error) {
	var (
		envValueAsInt64  int64
		envValueAsString string
	)

	envValueAsString = os.Getenv(envName)
	if envValueAsString == "" {
		val = envDefault
		err = nil
		return
	}

	envValueAsInt64, err = cos.ParseSize(envValueAsString, "")
	if err != nil {
		err = fmt.Errorf("envValue (\"%s\") not parse-able (err: %v)", envValueAsString, err)
		return
	}
	if envValueAsInt64 < 0 {
		err = fmt.Errorf("envValue (\"%s\") must not be negative", envValueAsString)
		return
	}

	val = uint64(envValueAsInt64)

	if val < envMin {
		err = fmt.Errorf("envValue (%v) < envMin (%v)", val, envMin)
		return
	}
	if val > envMax {
		err = fmt.Errorf("envValue (%v) > envMax (%v)", val, envMax)
		return
	}

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

func (bp *ocibp) ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (ecode int, err error) {
	var (
		delimiter           = string("/")
		fields              string
		limitAsInt          int
		listObjectsRequest  ociObjectstorage.ListObjectsRequest
		listObjectsResponse ociObjectstorage.ListObjectsResponse
		lsoEnt              *cmn.LsoEnt
		objectSummary       ociObjectstorage.ObjectSummary
	)

	if (msg.PageSize == 0) || (int(bp.maxPageSize) < int(msg.PageSize)) {
		limitAsInt = int(bp.maxPageSize)
	} else {
		if msg.PageSize < maxPageSizeMin {
			ecode = http.StatusInternalServerError
			err = fmt.Errorf("msg.PageSize (%v) must be at least maxPageSizeMin (%v)", msg.PageSize, maxPageSizeMin)
			return
		}
		limitAsInt = int(msg.PageSize)
	}

	// [TODO] Assume that we always want Name and Size
	//        Testing msg.IsFlagSet(apc.LsNameOnly) and msg.IsFlagSet(apc.LsNameSize) don't seem properly set

	fields = "name,size"

	listObjectsRequest = ociObjectstorage.ListObjectsRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &bck.Name,
		Limit:         &limitAsInt,
		Fields:        &fields,
	}
	if msg.Prefix != "" {
		listObjectsRequest.Prefix = &msg.Prefix
	}
	if msg.ContinuationToken != "" {
		listObjectsRequest.Start = &msg.ContinuationToken
	}
	if msg.IsFlagSet(apc.LsNoRecursion) {
		// [TODO] Need to handle case where I need to enumerate directories (while not "decending")
		listObjectsRequest.Delimiter = &delimiter
	}

	listObjectsResponse, err = bp.client.ListObjects(context.Background(), listObjectsRequest)
	if err != nil {
		ecode = ociRawRequestStatusToAISError(listObjectsResponse.RawResponse)
		return
	}

	if listObjectsResponse.NextStartWith == nil {
		lst.ContinuationToken = ""
	} else {
		lst.ContinuationToken = *listObjectsResponse.NextStartWith
	}

	lst.Entries = make(cmn.LsoEntries, 0, len(listObjectsResponse.Objects))
	for _, objectSummary = range listObjectsResponse.Objects {
		lsoEnt = &cmn.LsoEnt{}
		lsoEnt.Name = *objectSummary.Name
		if objectSummary.Size != nil {
			lsoEnt.Size = *objectSummary.Size
		}
		lst.Entries = append(lst.Entries, lsoEnt)
	}

	return
}

func (bp *ocibp) ListBuckets(_ cmn.QueryBcks) (bcks cmn.Bcks, ecode int, err error) {
	var (
		idx                 int
		item                ociObjectstorage.BucketSummary
		listBucketsRequest  ociObjectstorage.ListBucketsRequest
		listBucketsResponse ociObjectstorage.ListBucketsResponse
	)

	listBucketsRequest = ociObjectstorage.ListBucketsRequest{
		NamespaceName: &bp.namespace,
		CompartmentId: &bp.compartmentOCID,
	}

	listBucketsResponse, err = bp.client.ListBuckets(context.Background(), listBucketsRequest)
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

	return
}

func (bp *ocibp) PutObj(r io.ReadCloser, lom *core.LOM, _ *http.Request) (ecode int, err error) {
	var (
		putObjectRequest  ociObjectstorage.PutObjectRequest
		putObjectResponse ociObjectstorage.PutObjectResponse
	)

	// [TODO] Need to implement multi-threaded PUT when "length" exceeds bp.multiPartUploadThreshold

	putObjectRequest = ociObjectstorage.PutObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &lom.Bck().Name,
		ObjectName:    &lom.ObjName,
		PutObjectBody: r,
	}

	putObjectResponse, err = bp.client.PutObject(context.Background(), putObjectRequest)
	if err != nil {
		ecode = ociRawRequestStatusToAISError(putObjectResponse.RawResponse)
		return
	}

	lom.SetCustomKey(apc.HdrBackendProvider, apc.OCI)
	if putObjectResponse.ETag != nil {
		lom.SetCustomKey(cmn.ETag, *putObjectResponse.ETag)
	}
	if putObjectResponse.OpcContentMd5 != nil {
		lom.SetCustomKey(cmn.MD5ObjMD, *putObjectResponse.OpcContentMd5)
	}

	cos.Close(r)

	return
}

func (bp *ocibp) DeleteObj(lom *core.LOM) (ecode int, err error) {
	var (
		deleteObjectRequest  ociObjectstorage.DeleteObjectRequest
		deleteObjectResponse ociObjectstorage.DeleteObjectResponse
	)

	deleteObjectRequest = ociObjectstorage.DeleteObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &lom.Bck().Name,
		ObjectName:    &lom.ObjName,
	}

	deleteObjectResponse, err = bp.client.DeleteObject(context.Background(), deleteObjectRequest)
	if err != nil {
		ecode = ociRawRequestStatusToAISError(deleteObjectResponse.RawResponse)
		return
	}

	return
}

func (bp *ocibp) HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, ecode int, err error) {
	var (
		headBucketRequest  ociObjectstorage.HeadBucketRequest
		headBucketResponse ociObjectstorage.HeadBucketResponse
	)

	headBucketRequest = ociObjectstorage.HeadBucketRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &bck.Name,
	}

	headBucketResponse, err = bp.client.HeadBucket(ctx, headBucketRequest)
	if err != nil {
		ecode = ociRawRequestStatusToAISError(headBucketResponse.RawResponse)
		return
	}

	bckProps = make(cos.StrKVs, 2)
	bckProps[apc.HdrBackendProvider] = apc.OCI
	bckProps[apc.HdrBucketVerEnabled] = "false" // [TODO] At some point, if needed, add support for bucket versioning

	return
}

func (bp *ocibp) HeadObj(ctx context.Context, lom *core.LOM, _ *http.Request) (objAttrs *cmn.ObjAttrs, ecode int, err error) {
	var (
		headObjectRequest  ociObjectstorage.HeadObjectRequest
		headObjectResponse ociObjectstorage.HeadObjectResponse
	)

	headObjectRequest = ociObjectstorage.HeadObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &lom.Bck().Name,
		ObjectName:    &lom.ObjName,
	}

	headObjectResponse, err = bp.client.HeadObject(ctx, headObjectRequest)
	if err != nil {
		ecode = ociRawRequestStatusToAISError(headObjectResponse.RawResponse)
		return
	}

	objAttrs = &cmn.ObjAttrs{
		CustomMD: make(cos.StrKVs, 3),
		Size:     headObjectResponse.RawResponse.ContentLength,
	}
	objAttrs.CustomMD[apc.HdrBackendProvider] = apc.OCI
	if headObjectResponse.ETag != nil {
		objAttrs.CustomMD[cmn.ETag] = *headObjectResponse.ETag
	}
	if headObjectResponse.ContentMd5 != nil {
		objAttrs.CustomMD[cmn.MD5ObjMD] = *headObjectResponse.ContentMd5
	}

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

	putParams = allocPutParams(res, owt)
	err = bp.t.PutObject(lom, putParams)
	core.FreePutParams(putParams)

	return
}

func (bp *ocibp) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
	var (
		err                 error
		getObjectRequest    ociObjectstorage.GetObjectRequest
		getObjectResponse   ociObjectstorage.GetObjectResponse
		matchingETag        string
		matchingETagDesired bool
		rangeHeader         string
	)

	// [TODO] Need to implement multi-threaded GET when "length" exceeds bp.multiPartDownloadThreshold

	matchingETag, matchingETagDesired = lom.GetCustomKey(cmn.ETag)

	getObjectRequest = ociObjectstorage.GetObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &lom.Bck().Name,
		ObjectName:    &lom.ObjName,
	}
	if length > 0 {
		rangeHeader = cmn.MakeRangeHdr(offset, length)
		getObjectRequest.Range = &rangeHeader
	}
	if matchingETagDesired && (matchingETag != "") {
		getObjectRequest.IfMatch = &matchingETag
	}

	getObjectResponse, err = bp.client.GetObject(ctx, getObjectRequest)
	if err != nil {
		res.Err = err
		res.ErrCode = ociRawRequestStatusToAISError(getObjectResponse.RawResponse)
		return
	}

	res.R = getObjectResponse.Content
	res.Size = *getObjectResponse.ContentLength

	return
}
