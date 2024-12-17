//go:build oci

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

// Outstanding [TODO] items:
//   1) Need to parse OCI's ~/.oci/config file for non-ENV defaults (for req'd settings)
//   2) Validate ListObjects() should only return Name & Size in all cases (or improve)
//   3) Handle non-descending ListObjects() case (including listing of "virtual" directories)
//   4) Multi-Segment-Upload utilization (for fast/large object PUTs)... if practical
//   6) Multi-Segment-Download utilization (for fast/large object GETs)... if practical
//   5) Add support for object versioning

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
	ocicmn "github.com/oracle/oci-go-sdk/v65/common"
	ocios "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

const (
	maxPageSizeMin           = 1
	maxPageSizeMax           = 1000
	maxPageSizeDefault       = maxPageSizeMax
	mpdSegmentMaxSizeMin     = 4 * cos.KiB
	mpdSegmentMaxSizeMax     = 256 * cos.MiB
	mpdSegmentMaxSizeDefault = mpdSegmentMaxSizeMax
	mpdThresholdMin          = 8 * cos.KiB
	mpdThresholdMax          = 512 * cos.MiB
	mpdThresholdDefault      = mpdThresholdMax
	mpdMaxThreadsMin         = 4
	mpdMaxThreadsMax         = 64
	mpdMaxThreadsDefault     = 16
	mpuSegmentMaxSizeMin     = 4 * cos.KiB
	mpuSegmentMaxSizeMax     = 256 * cos.MiB
	mpuSegmentMaxSizeDefault = mpuSegmentMaxSizeMax
	mpuThresholdMin          = 8 * cos.KiB
	mpuThresholdMax          = 512 * cos.MiB
	mpuThresholdDefault      = mpuThresholdMax
	mpuMaxThreadsMin         = 4
	mpuMaxThreadsMax         = 64
	mpuMaxThreadsDefault     = 16
)

type ocibp struct {
	t                     core.TargetPut
	configurationProvider ocicmn.ConfigurationProvider
	compartmentOCID       string
	maxPageSize           int64
	mpdSegmentMaxSize     int64
	mpdThreshold          int64
	mpdMaxThreads         int64
	mpuSegmentMaxSize     int64
	mpuThreshold          int64
	mpuMaxThreads         int64
	client                ocios.ObjectStorageClient
	namespace             string
	base
}

func NewOCI(t core.TargetPut, tstats stats.Tracker) (core.Backend, error) {
	bp := &ocibp{
		t:    t,
		base: base{provider: apc.AWS},
	}
	bp.configurationProvider = ocicmn.NewRawConfigurationProvider(
		os.Getenv(env.OCI.TenancyOCID),
		os.Getenv(env.OCI.UserOCID),
		os.Getenv(env.OCI.Region),
		os.Getenv(env.OCI.Fingerprint),
		os.Getenv(env.OCI.PrivateKey),
		nil,
	)
	bp.compartmentOCID = os.Getenv(env.OCI.CompartmentOCID)

	if err := bp.set(env.OCI.MaxPageSize, maxPageSizeMin, maxPageSizeMax, maxPageSizeDefault, &bp.maxPageSize); err != nil {
		return nil, err
	}
	if err := bp.set(env.OCI.MaxDownloadSegmentSize, mpdSegmentMaxSizeMin, mpdSegmentMaxSizeMax,
		mpdSegmentMaxSizeDefault, &bp.mpdSegmentMaxSize); err != nil {
		return nil, err
	}
	if err := bp.set(env.OCI.MultiPartDownloadThreshold, mpdThresholdMin, mpdThresholdMax,
		mpdThresholdDefault, &bp.mpdThreshold); err != nil {
		return nil, err
	}
	if err := bp.set(env.OCI.MultiPartDownloadMaxThreads, mpdMaxThreadsMin, mpdMaxThreadsMax,
		mpdMaxThreadsDefault, &bp.mpdMaxThreads); err != nil {
		return nil, err
	}
	if err := bp.set(env.OCI.MaxUploadSegmentSize, mpuSegmentMaxSizeMin, mpuSegmentMaxSizeMax,
		mpuSegmentMaxSizeDefault, &bp.mpuSegmentMaxSize); err != nil {
		return nil, err
	}
	if err := bp.set(env.OCI.MultiPartUploadThreshold, mpuThresholdMin, mpuThresholdMax,
		mpuThresholdDefault, &bp.mpuThreshold); err != nil {
		return nil, err
	}
	if err := bp.set(env.OCI.MultiPartUploadMaxThreads, mpuMaxThreadsMin, mpuMaxThreadsMax,
		mpuMaxThreadsDefault, &bp.mpuMaxThreads); err != nil {
		return nil, err
	}

	client, err := ocios.NewObjectStorageClientWithConfigurationProvider(bp.configurationProvider)
	if err != nil {
		return nil, err
	}
	bp.client = client
	resp, err := bp.client.GetNamespace(context.Background(), ocios.GetNamespaceRequest{})
	if err != nil {
		return nil, err
	}
	bp.namespace = *resp.Value

	bp.base.init(t.Snode(), tstats)

	return bp, nil
}

func (*ocibp) set(envName string, envMin, envMax, envDefault int64, out *int64) error {
	s := os.Getenv(envName)
	if s == "" {
		*out = envDefault
		return nil
	}
	val, err := cos.ParseSize(s, "")
	switch {
	case err != nil:
		return fmt.Errorf("env '%s=%s' not parse-able: %v", envName, s, err)
	case val < 0:
		return fmt.Errorf("env '%s=%s' cannot be negative", envName, s)
	case val < envMin:
		return fmt.Errorf("env '%s=%s' cannot be less than %d", envName, s, envMin)
	case val > envMax:
		return fmt.Errorf("env '%s=%s' cannot be greater than %d", envName, s, envMax)
	}
	*out = val
	return nil
}

func ociStatus(rawResponse *http.Response) (ecode int) {
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
		cloudBck      = bck.RemoteBck()
		delimiter     = string("/")
		fields        string
		limitAsInt    int
		lsoEnt        *cmn.LsoEnt
		objectSummary ocios.ObjectSummary
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

	req := ocios.ListObjectsRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
		Limit:         &limitAsInt,
		Fields:        &fields,
	}
	if msg.Prefix != "" {
		req.Prefix = &msg.Prefix
	}
	if msg.ContinuationToken != "" {
		req.Start = &msg.ContinuationToken
	}
	if msg.IsFlagSet(apc.LsNoRecursion) {
		// [TODO] Need to handle case where I need to enumerate directories (while not "decending")
		req.Delimiter = &delimiter
	}

	resp, err := bp.client.ListObjects(context.Background(), req)
	if err != nil {
		ecode = ociStatus(resp.RawResponse)
		return
	}

	if resp.NextStartWith == nil {
		lst.ContinuationToken = ""
	} else {
		lst.ContinuationToken = *resp.NextStartWith
	}

	lst.Entries = make(cmn.LsoEntries, 0, len(resp.Objects))
	for _, objectSummary = range resp.Objects {
		lsoEnt = &cmn.LsoEnt{}
		lsoEnt.Name = *objectSummary.Name
		if objectSummary.Size != nil {
			lsoEnt.Size = *objectSummary.Size
		}
		lst.Entries = append(lst.Entries, lsoEnt)
	}

	return
}

func (bp *ocibp) ListBuckets(_ cmn.QueryBcks) (bcks cmn.Bcks, ecode int, _ error) {
	req := ocios.ListBucketsRequest{
		NamespaceName: &bp.namespace,
		CompartmentId: &bp.compartmentOCID,
	}
	resp, err := bp.client.ListBuckets(context.Background(), req)
	if err != nil {
		return bcks, ociStatus(resp.RawResponse), err
	}

	bcks = make(cmn.Bcks, len(resp.Items))
	for idx, item := range resp.Items {
		bcks[idx] = cmn.Bck{
			Name:     *item.Name,
			Provider: apc.OCI,
		}
	}
	return bcks, 0, nil
}

// [TODO] Need to implement multi-threaded PUT when "length" exceeds bp.mpuThreshold
func (bp *ocibp) PutObj(r io.ReadCloser, lom *core.LOM, _ *http.Request) (int, error) {
	cloudBck := lom.Bck().RemoteBck()
	req := ocios.PutObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
		ObjectName:    &lom.ObjName,
		PutObjectBody: r,
	}
	resp, err := bp.client.PutObject(context.Background(), req)
	// Note: in case PutObject() failed to close r...
	_ = r.Close()
	if err != nil {
		return ociStatus(resp.RawResponse), err
	}

	lom.SetCustomKey(apc.HdrBackendProvider, apc.OCI)
	if resp.ETag != nil {
		lom.SetCustomKey(cmn.ETag, *resp.ETag)
	}
	if resp.OpcContentMd5 != nil {
		lom.SetCustomKey(cmn.MD5ObjMD, *resp.OpcContentMd5)
	}

	return 0, nil
}

func (bp *ocibp) DeleteObj(lom *core.LOM) (ecode int, err error) {
	cloudBck := lom.Bck().RemoteBck()
	req := ocios.DeleteObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
		ObjectName:    &lom.ObjName,
	}

	resp, err := bp.client.DeleteObject(context.Background(), req)
	if err != nil {
		ecode = ociStatus(resp.RawResponse)
		return
	}

	return
}

func (bp *ocibp) HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, ecode int, err error) {
	cloudBck := bck.RemoteBck()
	req := ocios.HeadBucketRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
	}

	resp, err := bp.client.HeadBucket(ctx, req)
	if err != nil {
		ecode = ociStatus(resp.RawResponse)
		return
	}

	bckProps = make(cos.StrKVs, 2)
	bckProps[apc.HdrBackendProvider] = apc.OCI
	bckProps[apc.HdrBucketVerEnabled] = "false" // [TODO] At some point, if needed, add support for bucket versioning

	return
}

func (bp *ocibp) HeadObj(ctx context.Context, lom *core.LOM, _ *http.Request) (objAttrs *cmn.ObjAttrs, ecode int, err error) {
	cloudBck := lom.Bck().RemoteBck()
	req := ocios.HeadObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
		ObjectName:    &lom.ObjName,
	}

	resp, err := bp.client.HeadObject(ctx, req)
	if err != nil {
		ecode = ociStatus(resp.RawResponse)
		return
	}

	objAttrs = &cmn.ObjAttrs{
		CustomMD: make(cos.StrKVs, 3),
		Size:     resp.RawResponse.ContentLength,
	}
	objAttrs.CustomMD[apc.HdrBackendProvider] = apc.OCI
	if resp.ETag != nil {
		objAttrs.CustomMD[cmn.ETag] = *resp.ETag
	}
	if resp.ContentMd5 != nil {
		objAttrs.CustomMD[cmn.MD5ObjMD] = *resp.ContentMd5
	}

	return
}

func (bp *ocibp) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT, _ *http.Request) (int, error) {
	res := bp.GetObjReader(ctx, lom, 0, 0)
	if res.Err != nil {
		return res.ErrCode, res.Err
	}

	putParams := allocPutParams(res, owt)
	err := bp.t.PutObject(lom, putParams)
	core.FreePutParams(putParams)

	return 0, err
}

// [TODO]
//  1. Need to implement multi-threaded GET when "length" exceeds bp.mpdThreshold
//  2. Consider setting req.IfMatch to lom.GetCustomKey(cmn.ETag) if present
func (bp *ocibp) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
	var (
		cloudBck    = lom.Bck().RemoteBck()
		rangeHeader string
	)
	req := ocios.GetObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
		ObjectName:    &lom.ObjName,
	}
	if length > 0 {
		rangeHeader = cmn.MakeRangeHdr(offset, length)
		req.Range = &rangeHeader
	}

	resp, err := bp.client.GetObject(ctx, req)
	if err != nil {
		res.Err = err
		res.ErrCode = ociStatus(resp.RawResponse)
		return
	}

	res.R = resp.Content
	res.Size = *resp.ContentLength
	return res
}
