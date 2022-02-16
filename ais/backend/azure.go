//go:build azure
// +build azure

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
)

type (
	azureProvider struct {
		u string
		c *azblob.SharedKeyCredential
		t cluster.Target
		s azblob.ServiceURL
	}
)

const (
	azureDefaultProto = "https://"
	// Azure simulator(Azurite) consts
	azureDevAccName = "devstoreaccount1"
	azureDevAccKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	// Azurite is always HTTP
	azureDevHost = "http://127.0.0.1:10000/" + azureDevAccName

	// real Azure server constants
	azureHost = ".blob.core.windows.net"
	// AZ CLI compatible env vars
	azureAccNameEnvVar = "AZURE_STORAGE_ACCOUNT"
	azureAccKeyEnvVar  = "AZURE_STORAGE_KEY"
	// AZ AIS internal env vars
	azureURLEnvVar   = "AIS_AZURE_URL"
	azureProtoEnvVar = "AIS_AZURE_PROTO"
	// Object lease time for PUT/DEL operations, in seconds.
	// Must be within 15..60 range or -1(infinity).
	leaseTime = 60
)

var (
	// context placeholder
	azctx context.Context

	// TODO: client provided key by name and/or by value to encrypt/decrypt data.
	defaultKeyOptions azblob.ClientProvidedKeyOptions

	// interface guard
	_ cluster.BackendProvider = (*azureProvider)(nil)
)

func azureProto() string {
	proto := os.Getenv(azureProtoEnvVar)
	if proto == "" {
		return azureDefaultProto
	}
	return proto
}

func azureUserName() string {
	name := os.Getenv(azureAccNameEnvVar)
	if name == "" {
		return azureDevAccName
	}
	return name
}

func azureUserKey() string {
	key := os.Getenv(azureAccKeyEnvVar)
	if key == "" && azureUserName() == azureDevAccName {
		return azureDevAccKey
	}
	return key
}

// Detects development mode by checking the user name. It is a standalone
// function because there can be a better way to detect developer mode
func isAzureDevMode(user string) bool {
	return user == azureDevAccName
}

// URL is empty:
//    Dev -> http://127.0.0.1:1000/devstoreaccount1
//    Prod -> http://<account_name>.blob.core.windows.net
// URL is not empty
//    URL starts with protocol
//		-> URL
//    URL does not contain protocol
//		-> http://<account_name>URL/
func azureURL() string {
	url := os.Getenv(azureURLEnvVar)
	if url != "" {
		if !strings.HasPrefix(url, "http") {
			if !strings.HasPrefix(url, ".") {
				url = "." + url
			}
			url = azureProto() + azureUserName() + url
		}
		return url
	}
	user := azureUserName()
	if isAzureDevMode(user) {
		return azureDevHost
	}
	return azureProto() + user + azureHost
}

// Only one authentication way is supported: with Shared Credentials that
// requires Account name and key.
func NewAzure(t cluster.Target) (cluster.BackendProvider, error) {
	path := azureURL()
	u, err := url.Parse(path)
	if err != nil {
		return nil, fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderAzure, "parse", "URL", err)
	}
	name := azureUserName()
	key := azureUserKey()
	creds, err := azblob.NewSharedKeyCredential(name, key)
	if err != nil {
		return nil, fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderAzure, "init", "credentials", err)
	}

	azctx = context.Background()
	p := azblob.NewPipeline(creds, azblob.PipelineOptions{})
	return &azureProvider{
		t: t,
		u: path,
		c: creds,
		s: azblob.NewServiceURL(*u, p),
	}, nil
}

func azureErrorToAISError(azureError error, bck *cmn.Bck, objName string) (int, error) {
	stgErr, ok := azureError.(azblob.StorageError)
	if !ok {
		return http.StatusInternalServerError, azureError
	}
	switch stgErr.ServiceCode() {
	case azblob.ServiceCodeContainerNotFound:
		return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(*bck)
	case azblob.ServiceCodeBlobNotFound:
		msg := fmt.Sprintf("%s/%s not found", bck, objName)
		return http.StatusNotFound, cmn.NewErrHTTP(nil, msg, http.StatusNotFound)
	case azblob.ServiceCodeInvalidResourceName:
		msg := fmt.Sprintf("%s/%s not found", bck, objName)
		return http.StatusNotFound, cmn.NewErrHTTP(nil, msg, http.StatusNotFound)
	default:
		resp := stgErr.Response()
		if resp != nil {
			resp.Body.Close()
			return resp.StatusCode, azureError
		}
		return http.StatusInternalServerError, azureError
	}
}

func (*azureProvider) Provider() string { return cmn.ProviderAzure }

// https://docs.microsoft.com/en-us/connectors/azureblob/#general-limits
func (*azureProvider) MaxPageSize() uint { return 5000 }

///////////////////
// CREATE BUCKET //
///////////////////

func (ap *azureProvider) CreateBucket(_ *cluster.Bck) (errCode int, err error) {
	return creatingBucketNotSupportedErr(ap.Provider())
}

/////////////////
// HEAD BUCKET //
/////////////////

func (ap *azureProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cos.SimpleKVs,
	errCode int, err error) {
	var (
		cloudBck = bck.RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
	)
	resp, err := cntURL.GetProperties(ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		status, err := azureErrorToAISError(err, cloudBck, "")
		return bckProps, status, err
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		err := fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderAzure, "read bucket",
			cloudBck.Name, strconv.Itoa(resp.StatusCode()))
		return bckProps, resp.StatusCode(), err
	}
	bckProps = make(cos.SimpleKVs, 2)
	bckProps[cmn.HdrBackendProvider] = cmn.ProviderAzure
	bckProps[cmn.HdrBucketVerEnabled] = "true"
	return bckProps, http.StatusOK, nil
}

//////////////////
// LIST OBJECTS //
//////////////////

func (ap *azureProvider) ListObjects(bck *cluster.Bck, msg *cmn.ListObjsMsg) (bckList *cmn.BucketList, errCode int, err error) {
	msg.PageSize = calcPageSize(msg.PageSize, ap.MaxPageSize())

	var (
		h        = cmn.BackendHelpers.Azure
		cloudBck = bck.RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		marker   = azblob.Marker{}
		opts     = azblob.ListBlobsSegmentOptions{
			Prefix:     msg.Prefix,
			MaxResults: int32(msg.PageSize),
		}
	)
	if verbose {
		glog.Infof("list_objects %s", cloudBck.Name)
	}
	if msg.ContinuationToken != "" {
		marker.Val = api.String(msg.ContinuationToken)
	}

	resp, err := cntURL.ListBlobsFlatSegment(azctx, marker, opts)
	if err != nil {
		status, err := azureErrorToAISError(err, cloudBck, "")
		return nil, status, err
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		err := fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderAzure, "list objects of",
			cloudBck.Name, strconv.Itoa(resp.StatusCode()))
		return nil, resp.StatusCode(), err
	}
	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, len(resp.Segment.BlobItems))}
	for idx := range resp.Segment.BlobItems {
		var (
			blob  = &resp.Segment.BlobItems[idx]
			entry = &cmn.BucketEntry{Name: blob.Name}
		)
		if blob.Properties.ContentLength != nil && msg.WantProp(cmn.GetPropsSize) {
			entry.Size = *blob.Properties.ContentLength
		}
		if msg.WantProp(cmn.GetPropsVersion) {
			// NOTE: here and elsewhere (below), use Etag as the version
			if v, ok := h.EncodeVersion(string(blob.Properties.Etag)); ok {
				entry.Version = v
			}
		}
		if msg.WantProp(cmn.GetPropsChecksum) {
			if v, ok := h.EncodeCksum(blob.Properties.ContentMD5); ok {
				entry.Checksum = v
			}
		}

		bckList.Entries = append(bckList.Entries, entry)
	}
	if resp.NextMarker.Val != nil {
		bckList.ContinuationToken = *resp.NextMarker.Val
	}
	if verbose {
		glog.Infof("[list_bucket] count %d(marker: %s)", len(bckList.Entries), bckList.ContinuationToken)
	}
	return
}

//////////////////
// LIST BUCKETS //
//////////////////

func (ap *azureProvider) ListBuckets(_ cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	var (
		o          azblob.ListContainersSegmentOptions
		marker     azblob.Marker
		containers *azblob.ListContainersSegmentResponse
	)
	for marker.NotDone() {
		containers, err = ap.s.ListContainersSegment(azctx, marker, o)
		if err != nil {
			errCode, err = azureErrorToAISError(err, &cmn.Bck{Provider: cmn.ProviderAzure}, "")
			return
		}

		for idx := range containers.ContainerItems {
			bcks = append(bcks, cmn.Bck{
				Name:     containers.ContainerItems[idx].Name,
				Provider: cmn.ProviderAzure,
			})
		}
		marker = containers.NextMarker
	}
	return
}

/////////////////
// HEAD OBJECT //
/////////////////

func (ap *azureProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (oa *cmn.ObjAttrs, errCode int, err error) {
	var (
		resp     *azblob.BlobGetPropertiesResponse
		h        = cmn.BackendHelpers.Azure
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		blobURL  = cntURL.NewBlobURL(lom.ObjName)
	)
	if resp, err = blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, defaultKeyOptions); err != nil {
		errCode, err = azureErrorToAISError(err, cloudBck, lom.ObjName)
		return
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		err = fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderAzure, "get object props of",
			cloudBck.Name+"/"+lom.ObjName, strconv.Itoa(resp.StatusCode()))
		errCode = resp.StatusCode()
		return
	}
	oa = &cmn.ObjAttrs{}
	oa.SetCustomKey(cmn.SourceObjMD, cmn.ProviderAzure)
	oa.Size = resp.ContentLength()
	if v, ok := h.EncodeVersion(string(resp.ETag())); ok {
		oa.Ver = v // NOTE: using ETag as _the_ version
		oa.SetCustomKey(cmn.ETag, v)
	}
	if v, ok := h.EncodeCksum(resp.ContentMD5()); ok {
		oa.SetCustomKey(cmn.MD5ObjMD, v)
	}
	if verbose {
		glog.Infof("[head_object] %s", lom)
	}
	return
}

////////////////
// GET OBJECT //
////////////////

func (ap *azureProvider) GetObj(ctx context.Context, lom *cluster.LOM, owt cmn.OWT) (errCode int, err error) {
	reader, cksumToUse, errCode, err := ap.GetObjReader(ctx, lom)
	if err != nil {
		return errCode, err
	}
	params := cluster.AllocPutObjParams()
	{
		params.WorkTag = fs.WorkfileColdget
		params.Reader = reader
		params.OWT = owt
		params.Cksum = cksumToUse
		params.Atime = time.Now()
	}
	err = ap.t.PutObject(lom, params)
	if err != nil {
		return
	}
	if verbose {
		glog.Infof("[get_object] %s", lom)
	}
	return
}

////////////////////
// GET OBJ READER //
////////////////////

func (ap *azureProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (reader io.ReadCloser, expCksum *cos.Cksum,
	errCode int, err error) {
	var (
		h        = cmn.BackendHelpers.Azure
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		blobURL  = cntURL.NewBlobURL(lom.ObjName)
	)
	// Get checksum
	respProps, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, defaultKeyOptions)
	if err != nil {
		status, err := azureErrorToAISError(err, cloudBck, lom.ObjName)
		return nil, nil, status, err
	}
	if respProps.StatusCode() >= http.StatusBadRequest {
		err := fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderAzure, "get object props of",
			cloudBck.Name+"/"+lom.ObjName, strconv.Itoa(respProps.StatusCode()))
		return nil, nil, respProps.StatusCode(), err
	}
	// 0, 0 = read range: the whole object
	resp, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, defaultKeyOptions)
	if err != nil {
		errCode, err = azureErrorToAISError(err, cloudBck, lom.ObjName)
		return nil, nil, errCode, err
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		err := fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderAzure, "get object",
			cloudBck.Name+"/"+lom.ObjName, strconv.Itoa(respProps.StatusCode()))
		return nil, nil, resp.StatusCode(), err
	}

	// custom metadata
	lom.SetCustomKey(cmn.SourceObjMD, cmn.ProviderAzure)
	if v, ok := h.EncodeVersion(string(respProps.ETag())); ok {
		lom.SetVersion(v)
		lom.SetCustomKey(cmn.ETag, v)
	}
	if v, ok := h.EncodeCksum(respProps.ContentMD5()); ok {
		lom.SetCustomKey(cmn.MD5ObjMD, v)
		expCksum = cos.NewCksum(cos.ChecksumMD5, v)
	}

	setSize(ctx, resp.ContentLength())

	retryOpts := azblob.RetryReaderOptions{MaxRetryRequests: 3}
	return wrapReader(ctx, resp.Body(retryOpts)), expCksum, 0, nil
}

////////////////
// PUT OBJECT //
////////////////

func (ap *azureProvider) PutObj(r io.ReadCloser, lom *cluster.LOM) (int, error) {
	defer cos.Close(r)

	var (
		leaseID  string
		h        = cmn.BackendHelpers.Azure
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		blobURL  = cntURL.NewBlockBlobURL(lom.ObjName)
		cond     = azblob.ModifiedAccessConditions{}
	)
	// Try to lease: if object does not exist, leasing fails with NotFound
	acqResp, err := blobURL.AcquireLease(azctx, "", leaseTime, cond)
	if err == nil {
		leaseID = acqResp.LeaseID()
		defer blobURL.ReleaseLease(azctx, acqResp.LeaseID(), cond)
	}
	if err != nil {
		code, errLease := azureErrorToAISError(err, cloudBck, lom.ObjName)
		if code != http.StatusNotFound {
			return code, errLease
		}
	}
	// Use BlockBlob instead of PageBlob because the latter requires
	// object size to be divisible by 512.
	// Without buffer options(with 0's) UploadStreamToBlockBlob hangs up
	opts := azblob.UploadStreamToBlockBlobOptions{
		BufferSize: 64 * 1024,
		MaxBuffers: 3,
	}
	if leaseID != "" {
		opts.AccessConditions = azblob.BlobAccessConditions{
			LeaseAccessConditions: azblob.LeaseAccessConditions{LeaseID: leaseID},
		}
	}
	putResp, err := azblob.UploadStreamToBlockBlob(azctx, r, blobURL, opts)
	if err != nil {
		status, err := azureErrorToAISError(err, cloudBck, lom.ObjName)
		return status, err
	}
	resp := putResp.Response()
	resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		err := fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderAzure, "put object",
			cloudBck.Name+"/"+lom.ObjName, strconv.Itoa(resp.StatusCode))
		return resp.StatusCode, err
	}
	if v, ok := h.EncodeVersion(string(putResp.ETag())); ok {
		lom.SetCustomKey(cmn.ETag, v) // NOTE: using ETag as version
		lom.SetVersion(v)
	}
	if verbose {
		glog.Infof("[put_object] %s", lom)
	}
	return http.StatusOK, nil
}

///////////////////
// DELETE OBJECT //
///////////////////

// Delete looks complex because according to docs, it needs acquiring
// an object beforehand and releasing the lease after
func (ap *azureProvider) DeleteObj(lom *cluster.LOM) (int, error) {
	var (
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(lom.Bck().Name)
		blobURL  = cntURL.NewBlobURL(lom.ObjName)
		cond     = azblob.ModifiedAccessConditions{}
	)

	acqResp, err := blobURL.AcquireLease(azctx, "", leaseTime, cond)
	if err != nil {
		return azureErrorToAISError(err, cloudBck, lom.ObjName)
	}
	if acqResp.StatusCode() >= http.StatusBadRequest {
		err := fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderAzure, "acquire object",
			cloudBck.Name+"/"+lom.ObjName, strconv.Itoa(acqResp.StatusCode()))
		return acqResp.StatusCode(), err
	}

	delCond := azblob.BlobAccessConditions{
		LeaseAccessConditions: azblob.LeaseAccessConditions{LeaseID: acqResp.LeaseID()},
	}
	defer blobURL.ReleaseLease(azctx, acqResp.LeaseID(), cond)
	delResp, err := blobURL.Delete(azctx, azblob.DeleteSnapshotsOptionInclude, delCond)
	if err != nil {
		return azureErrorToAISError(err, cloudBck, lom.ObjName)
	}
	if delResp.StatusCode() >= http.StatusBadRequest {
		err := fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderAzure, "delete object",
			cloudBck.Name+"/"+lom.ObjName, strconv.Itoa(delResp.StatusCode()))
		return delResp.StatusCode(), err
	}
	return http.StatusOK, nil
}
