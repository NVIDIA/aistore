// +build azure

// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
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

// interface guard
var _ cluster.CloudProvider = (*azureProvider)(nil)

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
func NewAzure(t cluster.Target) (cluster.CloudProvider, error) {
	path := azureURL()
	u, err := url.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %v", err)
	}
	name := azureUserName()
	key := azureUserKey()
	creds, err := azblob.NewSharedKeyCredential(name, key)
	if err != nil {
		return nil, fmt.Errorf("failed to init credentials %v", err)
	}
	p := azblob.NewPipeline(creds, azblob.PipelineOptions{})
	return &azureProvider{
		t: t,
		u: path,
		c: creds,
		s: azblob.NewServiceURL(*u, p),
	}, nil
}

func (ap *azureProvider) azureErrorToAISError(azureError error, bck *cmn.Bck, objName string) (int, error) {
	stgErr, ok := azureError.(azblob.StorageError)
	if !ok {
		return http.StatusInternalServerError, azureError
	}
	switch stgErr.ServiceCode() {
	case azblob.ServiceCodeContainerNotFound:
		return http.StatusNotFound, cmn.NewErrorRemoteBucketDoesNotExist(*bck, ap.t.Snode().Name())
	case azblob.ServiceCodeBlobNotFound:
		msg := fmt.Sprintf("%s/%s not found", bck, objName)
		return http.StatusNotFound, &cmn.HTTPError{Status: http.StatusNotFound, Message: msg}
	case azblob.ServiceCodeInvalidResourceName:
		msg := fmt.Sprintf("%s/%s not found", bck, objName)
		return http.StatusNotFound, &cmn.HTTPError{Status: http.StatusNotFound, Message: msg}
	default:
		resp := stgErr.Response()
		if resp != nil {
			resp.Body.Close()
			return resp.StatusCode, azureError
		}
		return http.StatusInternalServerError, azureError
	}
}

func (ap *azureProvider) Provider() string { return cmn.ProviderAzure }

// https://docs.microsoft.com/en-us/connectors/azureblob/#general-limits
func (ap *azureProvider) MaxPageSize() uint { return 5000 }

func (ap *azureProvider) ListBuckets(ctx context.Context, _ cmn.QueryBcks) (buckets cmn.BucketNames, errCode int, err error) {
	var (
		o          azblob.ListContainersSegmentOptions
		marker     azblob.Marker
		containers *azblob.ListContainersSegmentResponse
	)
	for marker.NotDone() {
		containers, err = ap.s.ListContainersSegment(ctx, marker, o)
		if err != nil {
			errCode, err = ap.azureErrorToAISError(err, &cmn.Bck{Provider: cmn.ProviderAzure}, "")
			return
		}

		for idx := range containers.ContainerItems {
			buckets = append(buckets, cmn.Bck{
				Name:     containers.ContainerItems[idx].Name,
				Provider: cmn.ProviderAzure,
			})
		}
		marker = containers.NextMarker
	}
	return
}

// Delete looks complex because according to docs, it needs acquiring
// an object beforehand and releasing the lease after
func (ap *azureProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (int, error) {
	var (
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(lom.BckName())
		blobURL  = cntURL.NewBlobURL(lom.ObjName)
		cond     = azblob.ModifiedAccessConditions{}
	)

	acqResp, err := blobURL.AcquireLease(ctx, "", leaseTime, cond)
	if err != nil {
		return ap.azureErrorToAISError(err, cloudBck, lom.ObjName)
	}
	if acqResp.StatusCode() >= http.StatusBadRequest {
		return acqResp.StatusCode(), fmt.Errorf("failed to acquire %s/%s", cloudBck, lom.ObjName)
	}

	delCond := azblob.BlobAccessConditions{
		LeaseAccessConditions: azblob.LeaseAccessConditions{LeaseID: acqResp.LeaseID()},
	}
	defer blobURL.ReleaseLease(ctx, acqResp.LeaseID(), cond)
	delResp, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, delCond)
	if err != nil {
		return ap.azureErrorToAISError(err, cloudBck, lom.ObjName)
	}
	if delResp.StatusCode() >= http.StatusBadRequest {
		return delResp.StatusCode(), fmt.Errorf("failed to delete object %s/%s", cloudBck, lom.ObjName)
	}
	return http.StatusOK, nil
}

func (ap *azureProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, errCode int, err error) {
	var (
		cloudBck = bck.RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
	)
	resp, err := cntURL.GetProperties(ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		status, err := ap.azureErrorToAISError(err, cloudBck, "")
		return bckProps, status, err
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		return bckProps, resp.StatusCode(), fmt.Errorf("failed to read bucket %q props", cloudBck.Name)
	}
	bckProps = make(cmn.SimpleKVs, 2)
	bckProps[cmn.HeaderCloudProvider] = cmn.ProviderAzure
	bckProps[cmn.HeaderBucketVerEnabled] = "true"
	return bckProps, http.StatusOK, nil
}

func (ap *azureProvider) ListObjects(ctx context.Context, bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, errCode int, err error) {
	msg.PageSize = calcPageSize(msg.PageSize, ap.MaxPageSize())

	var (
		h        = cmn.CloudHelpers.Azure
		cloudBck = bck.RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		marker   = azblob.Marker{}
		opts     = azblob.ListBlobsSegmentOptions{
			Prefix:     msg.Prefix,
			MaxResults: int32(msg.PageSize),
		}
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("list_objects %s", cloudBck.Name)
	}
	if msg.ContinuationToken != "" {
		marker.Val = api.String(msg.ContinuationToken)
	}

	resp, err := cntURL.ListBlobsFlatSegment(ctx, marker, opts)
	if err != nil {
		status, err := ap.azureErrorToAISError(err, cloudBck, "")
		return nil, status, err
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		return nil, resp.StatusCode(), fmt.Errorf("failed to list objects %q", cloudBck.Name)
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
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[list_bucket] count %d(marker: %s)", len(bckList.Entries), bckList.ContinuationToken)
	}
	return
}

func (ap *azureProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, errCode int, err error) {
	objMeta = make(cmn.SimpleKVs)
	var (
		h        = cmn.CloudHelpers.Azure
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		blobURL  = cntURL.NewBlobURL(lom.ObjName)
	)
	resp, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		status, err := ap.azureErrorToAISError(err, cloudBck, lom.ObjName)
		return objMeta, status, err
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		return objMeta, resp.StatusCode(), fmt.Errorf("failed to get object props %s/%s", cloudBck, lom.ObjName)
	}
	objMeta[cmn.HeaderObjSize] = strconv.FormatInt(resp.ContentLength(), 10)
	objMeta[cmn.HeaderCloudProvider] = cmn.ProviderAzure
	// Simulate object versioning:
	// Azure provider does not have real versioning, but it has ETag.
	if v, ok := h.EncodeVersion(string(resp.ETag())); ok {
		objMeta[cmn.HeaderObjVersion] = v
	}
	if v, ok := h.EncodeCksum(resp.ContentMD5()); ok {
		objMeta[cluster.MD5ObjMD] = v
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_object] %s/%s", cloudBck, lom.ObjName)
	}
	return
}

func (ap *azureProvider) GetObj(ctx context.Context, lom *cluster.LOM) (workFQN string, errCode int, err error) {
	reader, cksumToCheck, errCode, err := ap.GetObjReader(ctx, lom)
	if err != nil {
		return "", errCode, err
	}
	params := cluster.PutObjectParams{
		Tag:          fs.WorkfileColdget,
		Reader:       reader,
		RecvType:     cluster.ColdGet,
		Cksum:        cksumToCheck,
		WithFinalize: false,
	}
	workFQN, err = ap.t.PutObject(lom, params)
	if err != nil {
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[get_object] %s", lom)
	}
	return
}

func (ap *azureProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (reader io.ReadCloser, expectedCksm *cmn.Cksum, errCode int, err error) {
	var (
		h        = cmn.CloudHelpers.Azure
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		blobURL  = cntURL.NewBlobURL(lom.ObjName)
	)

	// Get checksum
	respProps, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		status, err := ap.azureErrorToAISError(err, cloudBck, lom.ObjName)
		return nil, nil, status, err
	}
	if respProps.StatusCode() >= http.StatusBadRequest {
		return nil, nil, respProps.StatusCode(), fmt.Errorf("failed to get object props %s/%s", cloudBck, lom.ObjName)
	}
	// 0, 0 = read range: the whole object
	resp, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		errCode, err = ap.azureErrorToAISError(err, cloudBck, lom.ObjName)
		return nil, nil, errCode, err
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		return nil, nil, resp.StatusCode(), fmt.Errorf("failed to GET object %s/%s", cloudBck, lom.ObjName)
	}

	var (
		cksumToCheck *cmn.Cksum
		retryOpts    = azblob.RetryReaderOptions{MaxRetryRequests: 3}
		customMD     = cmn.SimpleKVs{
			cluster.SourceObjMD: cluster.SourceAzureObjMD,
		}
	)
	if v, ok := h.EncodeVersion(string(respProps.ETag())); ok {
		lom.SetVersion(v)
		customMD[cluster.VersionObjMD] = v
	}
	if v, ok := h.EncodeCksum(respProps.ContentMD5()); ok {
		customMD[cluster.MD5ObjMD] = v
		cksumToCheck = cmn.NewCksum(cmn.ChecksumMD5, v)
	}

	lom.SetCustomMD(customMD)
	setSize(ctx, resp.ContentLength())

	return wrapReader(ctx, resp.Body(retryOpts)), cksumToCheck, 0, nil
}

func (ap *azureProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, errCode int, err error) {
	var (
		leaseID  string
		h        = cmn.CloudHelpers.Azure
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		blobURL  = cntURL.NewBlockBlobURL(lom.ObjName)
		cond     = azblob.ModifiedAccessConditions{}
	)
	// Try to lease: if object does not exist, leasing fails with NotFound
	acqResp, err := blobURL.AcquireLease(ctx, "", leaseTime, cond)
	if err == nil {
		leaseID = acqResp.LeaseID()
		defer blobURL.ReleaseLease(ctx, acqResp.LeaseID(), cond)
	}
	if err != nil {
		code, errLease := ap.azureErrorToAISError(err, cloudBck, lom.ObjName)
		if code != http.StatusNotFound {
			return "", code, errLease
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
		opts.AccessConditions = azblob.BlobAccessConditions{LeaseAccessConditions: azblob.LeaseAccessConditions{LeaseID: leaseID}}
	}
	putResp, err := azblob.UploadStreamToBlockBlob(ctx, r, blobURL, opts)
	if err != nil {
		status, err := ap.azureErrorToAISError(err, cloudBck, lom.ObjName)
		return "", status, err
	}
	resp := putResp.Response()
	resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return "", resp.StatusCode, fmt.Errorf("failed to put object %s/%s", cloudBck, lom.ObjName)
	}
	if v, ok := h.EncodeVersion(string(putResp.ETag())); ok {
		version = v
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[put_object] %s, version: %s", lom, version)
	}
	return version, http.StatusOK, nil
}
