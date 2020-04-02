// +build azure

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	azureProvider struct {
		u string
		c *azblob.SharedKeyCredential
		t *targetrunner
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
	_ cloudProvider = &azureProvider{}
)

// Simple path.Join does not work, because it removes duplicated
// separators making the URL invalid
func urlJoin(parts ...string) (*url.URL, error) {
	cmn.Assert(len(parts) != 0)
	u, err := url.Parse(parts[0])
	if err != nil {
		return nil, err
	}
	// Replace full URI with its path: http:://a.com/obj -> a.com/obj
	parts[0] = u.Path
	u.Path = path.Join(parts...)
	return u, nil
}

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
func newAzureProvider(t *targetrunner) (*azureProvider, error) {
	path := azureURL()
	u, err := url.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %v", err)
	}
	name := azureUserName()
	key := azureUserKey()
	creds, err := azblob.NewSharedKeyCredential(name, key)
	if err != nil {
		return nil, fmt.Errorf("failed to init credentials", err)
	}
	p := azblob.NewPipeline(creds, azblob.PipelineOptions{})
	return &azureProvider{
		t: t,
		u: path,
		c: creds,
		s: azblob.NewServiceURL(*u, p),
	}, nil
}

func (ap *azureProvider) azureErrorToAISError(azureError error, bucket, objName string) (error, int) {
	stgErr, ok := azureError.(azblob.StorageError)
	if !ok {
		return azureError, http.StatusInternalServerError
	}
	switch stgErr.ServiceCode() {
	case azblob.ServiceCodeContainerNotFound:
		bck := cmn.Bck{Name: bucket, Provider: cmn.ProviderAzure}
		return cmn.NewErrorCloudBucketDoesNotExist(bck, ap.t.Snode().Name()), http.StatusNotFound
	case azblob.ServiceCodeBlobNotFound:
		msg := fmt.Sprintf("%s/%s not found", bucket, objName)
		return &cmn.HTTPError{Status: http.StatusNotFound, Message: msg}, http.StatusNotFound
	case azblob.ServiceCodeInvalidResourceName:
		msg := fmt.Sprintf("%s/%s not found", bucket, objName)
		return &cmn.HTTPError{Status: http.StatusNotFound, Message: msg}, http.StatusNotFound
	default:
		if stgErr.Response() != nil {
			return azureError, stgErr.Response().StatusCode
		}
		return azureError, http.StatusInternalServerError
	}
}

func (ap *azureProvider) listBuckets(ctx context.Context) (buckets []string, err error, errCode int) {
	var (
		o          azblob.ListContainersSegmentOptions
		marker     azblob.Marker
		containers *azblob.ListContainersSegmentResponse
	)
	for marker.NotDone() {
		containers, err = ap.s.ListContainersSegment(ctx, marker, o)
		if err != nil {
			err, errCode = ap.azureErrorToAISError(err, "", "")
			return
		}

		for _, container := range containers.ContainerItems {
			buckets = append(buckets, container.Name)
		}
		marker = containers.NextMarker
	}
	return
}

// Delete looks complex because according to docs, it needs acquiring
// an object beforehand and releasing the lease after
func (ap *azureProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (error, int) {
	cntURL := ap.s.NewContainerURL(lom.BckName())
	blobURL := cntURL.NewBlobURL(lom.ObjName)

	cond := azblob.ModifiedAccessConditions{}
	acqResp, err := blobURL.AcquireLease(ctx, "", leaseTime, cond)
	if err != nil {
		return ap.azureErrorToAISError(err, lom.BckName(), lom.ObjName)
	}
	if acqResp.StatusCode() >= http.StatusBadRequest {
		return fmt.Errorf("failed to acquire %s/%s", lom.Bck(), lom.ObjName), acqResp.StatusCode()
	}

	delCond := azblob.BlobAccessConditions{
		LeaseAccessConditions: azblob.LeaseAccessConditions{LeaseID: acqResp.LeaseID()},
	}
	defer blobURL.ReleaseLease(ctx, acqResp.LeaseID(), cond)
	delResp, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, delCond)
	if err != nil {
		return ap.azureErrorToAISError(err, lom.BckName(), lom.ObjName)
	}
	if delResp.StatusCode() >= http.StatusBadRequest {
		return fmt.Errorf("failed to delete object %s/%s", lom.Bck(), lom.ObjName), delResp.StatusCode()
	}
	return nil, http.StatusOK
}

func (ap *azureProvider) headBucket(ctx context.Context, bucket string) (bucketProps cmn.SimpleKVs, err error, errCode int) {
	bckProps := make(cmn.SimpleKVs)
	cntURL := ap.s.NewContainerURL(bucket)
	resp, err := cntURL.GetProperties(ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		err, status := ap.azureErrorToAISError(err, bucket, "")
		return bckProps, err, status
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		return bckProps, fmt.Errorf("failed to read bucket %s props", bucket), resp.StatusCode()
	}
	bckProps[cmn.HeaderCloudProvider] = cmn.ProviderAzure
	bckProps[cmn.HeaderBucketVerEnabled] = "true"
	return bckProps, nil, http.StatusOK
}

// Default page size for Azure is 5000 blobs a page.
func (ap *azureProvider) ListObjects(ctx context.Context, bucket string, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	cntURL := ap.s.NewContainerURL(bucket)
	marker := azblob.Marker{}
	if msg.PageMarker != "" {
		marker.Val = &msg.PageMarker
	}
	// TODO: MaxResults limits the total, not the page size.
	// So, even if a bucket has more objects, Azure returns
	// MaxResults and sets Marker to empty string
	opts := azblob.ListBlobsSegmentOptions{Prefix: msg.Prefix}
	if msg.PageSize != 0 {
		opts.MaxResults = int32(msg.PageSize)
	}
	resp, err := cntURL.ListBlobsFlatSegment(ctx, marker, opts)
	if err != nil {
		err, status := ap.azureErrorToAISError(err, bucket, "")
		return nil, err, status
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		return nil, fmt.Errorf("failed to list objects %s", bucket), resp.StatusCode()
	}
	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, initialBucketListSize)}
	for _, blob := range resp.Segment.BlobItems {
		entry := &cmn.BucketEntry{Name: blob.Name}
		if blob.Properties.ContentLength != nil && strings.Contains(msg.Props, cmn.GetPropsSize) {
			entry.Size = *blob.Properties.ContentLength
		}
		if strings.Contains(msg.Props, cmn.GetPropsVersion) {
			entry.Version = strings.Trim(string(blob.Properties.Etag), "\"")
		}
		if strings.Contains(msg.Props, cmn.GetPropsChecksum) {
			entry.Checksum = hex.EncodeToString(blob.Properties.ContentMD5)
		}

		bckList.Entries = append(bckList.Entries, entry)
	}
	if resp.NextMarker.Val != nil {
		msg.PageMarker = *resp.NextMarker.Val
		bckList.PageMarker = msg.PageMarker
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[list_bucket] count %d(marker: %s)", len(bckList.Entries), bckList.PageMarker)
	}

	return
}

func (ap *azureProvider) headObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	objMeta = make(cmn.SimpleKVs)
	cntURL := ap.s.NewContainerURL(lom.BckName())
	blobURL := cntURL.NewBlobURL(lom.ObjName)
	resp, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		err, status := ap.azureErrorToAISError(err, lom.BckName(), lom.ObjName)
		return objMeta, err, status
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		return objMeta, fmt.Errorf("failed to get object props %s/%s", lom.Bck(), lom.ObjName), resp.StatusCode()
	}
	objMeta[cmn.HeaderObjSize] = strconv.FormatInt(resp.ContentLength(), 10)
	objMeta[cmn.HeaderCloudProvider] = cmn.ProviderAzure
	// Simulate object versioning:
	// Azure provider does not have real versioning, but it has ETag.
	objMeta[cmn.HeaderObjVersion] = strings.Trim(string(resp.ETag()), "\"")
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_object] %s", lom)
	}
	return
}

func (ap *azureProvider) getObj(ctx context.Context, workFQN string, lom *cluster.LOM) (err error, errCode int) {
	cntURL := ap.s.NewContainerURL(lom.BckName())
	blobURL := cntURL.NewBlobURL(lom.ObjName)

	// Get checksum
	respProps, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		err, status := ap.azureErrorToAISError(err, lom.BckName(), lom.ObjName)
		return err, status
	}
	if respProps.StatusCode() >= http.StatusBadRequest {
		return fmt.Errorf("failed to get object props %s/%s", lom.Bck(), lom.ObjName), respProps.StatusCode()
	}
	cksumToCheck := cmn.NewCksum(cmn.ChecksumMD5, hex.EncodeToString(respProps.ContentMD5()))

	// 0, 0 = read range: the whole object
	resp, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return ap.azureErrorToAISError(err, lom.BckName(), lom.ObjName)
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		return fmt.Errorf("failed to GET object %s/%s", lom.Bck(), lom.ObjName), resp.StatusCode()
	}

	retryOpts := azblob.RetryReaderOptions{MaxRetryRequests: 3}
	poi := &putObjInfo{
		t:            ap.t,
		lom:          lom,
		r:            resp.Body(retryOpts),
		cksumToCheck: cksumToCheck,
		workFQN:      workFQN,
		cold:         true,
	}
	if err = poi.writeToFile(); err != nil {
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[get_object] %s", lom)
	}
	return
}

func (ap *azureProvider) putObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	var leaseID string
	cntURL := ap.s.NewContainerURL(lom.BckName())
	blobURL := cntURL.NewBlockBlobURL(lom.ObjName)
	// Try to lease: if object does not exist, leasing fails with NotFound
	cond := azblob.ModifiedAccessConditions{}
	acqResp, err := blobURL.AcquireLease(ctx, "", leaseTime, cond)
	if err == nil {
		leaseID = acqResp.LeaseID()
		defer blobURL.ReleaseLease(ctx, acqResp.LeaseID(), cond)
	}
	if err != nil {
		errLease, code := ap.azureErrorToAISError(err, lom.BckName(), lom.ObjName)
		if code != http.StatusNotFound {
			return "", errLease, code
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
		err, status := ap.azureErrorToAISError(err, lom.BckName(), lom.ObjName)
		return "", err, status
	}
	if putResp.Response().StatusCode >= http.StatusBadRequest {
		return "", fmt.Errorf("failed to put object %s/%s", lom.Bck(), lom.ObjName), putResp.Response().StatusCode
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[put_object] %s", lom)
	}
	return strings.Trim(string(putResp.ETag()), "\""), nil, http.StatusOK
}
