//go:build azure

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
)

type (
	azureProvider struct {
		u string
		c *azblob.SharedKeyCredential
		t core.TargetPut
		s azblob.ServiceURL
	}
)

const (
	azDefaultProto = "https://"
	azBackend      = "azure-backend"

	// Azure server constants
	azHost = ".blob.core.windows.net"
	// AZ CLI compatible env vars
	azAccNameEnvVar = "AZURE_STORAGE_ACCOUNT"
	azAccKeyEnvVar  = "AZURE_STORAGE_KEY"

	// AZ AIS internal env vars
	azURLEnvVar   = "AIS_AZURE_URL"
	azProtoEnvVar = "AIS_AZURE_PROTO"
	// Object lease time for PUT/DEL operations, in seconds.
	// Must be within 15..60 range or -1(infinity).
	leaseTime = 60
)

// used to parse azure errors
const (
	azErrDesc = "Description"
	azErrCode = "Code: "
)

var (
	azctx        context.Context                 // context placeholder
	azKeyOptions azblob.ClientProvidedKeyOptions // TODO: encryption

	azCleanErrRegex = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)
)

// interface guard
var _ core.BackendProvider = (*azureProvider)(nil)

func azProto() string {
	proto := os.Getenv(azProtoEnvVar)
	if proto == "" {
		proto = azDefaultProto
	}
	return proto
}

func azUserName() string { return os.Getenv(azAccNameEnvVar) }
func azUserKey() string  { return os.Getenv(azAccKeyEnvVar) }

func azErrStatus(status int) error { return fmt.Errorf("http-status=%d", status) }

// NOTE: not using cmn/backend helpers foir azure
func azEncodeChecksum(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	return hex.EncodeToString(v)
}

// NOTE: ditto
func azEncodeEtag(etag azblob.ETag) string {
	return strings.Trim(string(etag), "\"")
}

// 1. URL is empty: http://<account_name>.blob.core.windows.net
// 2. URL is not empty, starts with protocol: URL
// 3. URL does not contain protocol: http://<account_name>URL/
func azURL() string {
	url := os.Getenv(azURLEnvVar)
	if url != "" {
		if !strings.HasPrefix(url, "http") {
			if !strings.HasPrefix(url, ".") {
				url = "." + url
			}
			url = azProto() + azUserName() + url
		}
		return url
	}
	user := azUserName()
	return azProto() + user + azHost
}

// Only one authentication way is supported: with Shared Credentials that
// requires Account name and key.
func NewAzure(t core.TargetPut) (core.BackendProvider, error) {
	path := azURL()
	u, err := url.Parse(path)
	if err != nil {
		return nil, cmn.NewErrFailedTo(nil, azBackend+": parse", "URL", err)
	}
	name := azUserName()
	key := azUserKey()
	creds, err := azblob.NewSharedKeyCredential(name, key)
	if err != nil {
		return nil, cmn.NewErrFailedTo(nil, azBackend+": init", "credentials", err)
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
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("azureError:", bck.Cname(objName))
		nlog.Infoln(azureError)
	}
	bckNotFound, status, err := _toErr(azureError, bck, objName)
	if bckNotFound {
		return status, err
	}
	return status, errors.New("azure-error[" + err.Error() + "]")
}

func _toErr(azureError error, bck *cmn.Bck, objName string) (bool, int, error) {
	stgErr, ok := azureError.(azblob.StorageError)
	if !ok {
		return false, http.StatusInternalServerError, azureError
	}
	switch stgErr.ServiceCode() {
	case azblob.ServiceCodeContainerNotFound:
		return true, http.StatusNotFound, cmn.NewErrRemoteBckNotFound(bck)
	case azblob.ServiceCodeBlobNotFound:
		err := fmt.Errorf("%s not found", bck.Cname(objName))
		return false, http.StatusNotFound, cmn.NewErrHTTP(nil, err, http.StatusNotFound)
	case azblob.ServiceCodeInvalidResourceName:
		err := fmt.Errorf("%s not found", bck.Cname(objName))
		return false, http.StatusNotFound, cmn.NewErrHTTP(nil, err, http.StatusNotFound)
	}

	status := http.StatusInternalServerError
	if resp := stgErr.Response(); resp != nil {
		resp.Body.Close()
		status = resp.StatusCode
	}

	// azure error is usually a multi-line text with items including
	// request ID, authorization, variery of x-ms-* headers, server and user agent, etc. etc.
	var (
		code        string
		description string
		lines       = strings.Split(azureError.Error(), "\n")
	)
	for _, line := range lines {
		if strings.HasPrefix(line, azErrDesc) {
			description = azCleanErrRegex.ReplaceAllString(line[len(azErrDesc):], "")
		} else if i := strings.Index(line, azErrCode); i > 0 {
			code = azCleanErrRegex.ReplaceAllString(line[i+len(azErrCode):], "")
		}
	}
	if code != "" && description != "" {
		return false, status, errors.New(code + ": " + description)
	}
	return false, status, azureError
}

func (*azureProvider) Provider() string { return apc.Azure }

// https://docs.microsoft.com/en-us/connectors/azureblob/#general-limits
func (*azureProvider) MaxPageSize() uint { return 5000 }

///////////////////
// CREATE BUCKET //
///////////////////

func (*azureProvider) CreateBucket(_ *meta.Bck) (int, error) {
	return http.StatusNotImplemented, cmn.NewErrNotImpl("create", "azure:// bucket")
}

//
// HEAD BUCKET
//

func (ap *azureProvider) HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs,
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
		err := cmn.NewErrFailedTo(nil, azBackend+": read bucket", cloudBck.Name, azErrStatus(resp.StatusCode()))
		return bckProps, resp.StatusCode(), err
	}
	bckProps = make(cos.StrKVs, 2)
	bckProps[apc.HdrBackendProvider] = apc.Azure
	bckProps[apc.HdrBucketVerEnabled] = "true"
	return bckProps, http.StatusOK, nil
}

//
// LIST OBJECTS
//

func (ap *azureProvider) ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoResult) (errCode int, err error) {
	msg.PageSize = calcPageSize(msg.PageSize, ap.MaxPageSize())
	var (
		cloudBck = bck.RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		marker   = azblob.Marker{}
		opts     = azblob.ListBlobsSegmentOptions{Prefix: msg.Prefix, MaxResults: int32(msg.PageSize)}
	)
	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infof("list_objects %s", cloudBck.Name)
	}
	if msg.ContinuationToken != "" {
		marker.Val = apc.String(msg.ContinuationToken)
	}

	resp, err := cntURL.ListBlobsFlatSegment(azctx, marker, opts)
	if err != nil {
		return azureErrorToAISError(err, cloudBck, "")
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		err := cmn.NewErrFailedTo(nil, azBackend+": list objects of", cloudBck.Name, azErrStatus(resp.StatusCode()))
		return resp.StatusCode(), err
	}

	l := len(resp.Segment.BlobItems)
	for i := len(lst.Entries); i < l; i++ {
		lst.Entries = append(lst.Entries, &cmn.LsoEntry{}) // add missing empty
	}
	for idx := range resp.Segment.BlobItems {
		var (
			custom = cos.StrKVs{}
			blob   = &resp.Segment.BlobItems[idx]
			entry  = lst.Entries[idx]
		)
		entry.Name = blob.Name
		entry.Size = *blob.Properties.ContentLength
		if msg.IsFlagSet(apc.LsNameOnly) || msg.IsFlagSet(apc.LsNameSize) {
			continue
		}
		etag := azEncodeEtag(blob.Properties.Etag)

		if blob.VersionID != nil {
			// ref: https://learn.microsoft.com/en-us/azure/storage/blobs/versioning-overview#how-blob-versioning-works
			entry.Version = *blob.VersionID
		} else {
			entry.Version = etag // NOTE: for non-versioned blobs default to using Etag as the version
		}
		entry.Checksum = azEncodeChecksum(blob.Properties.ContentMD5)

		// custom
		if msg.WantProp(apc.GetPropsCustom) {
			custom[cmn.ETag] = etag
			if !blob.Properties.LastModified.IsZero() {
				custom[cmn.LastModified] = fmtTime(blob.Properties.LastModified)
			}
			if blob.Properties.ContentType != nil {
				custom[cos.HdrContentType] = *blob.Properties.ContentType
			}
			entry.Custom = cmn.CustomMD2S(custom)
		}
	}
	lst.Entries = lst.Entries[:l]

	if resp.NextMarker.Val != nil {
		lst.ContinuationToken = *resp.NextMarker.Val
	}
	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infof("[list_objects] count %d(marker: %s)", len(lst.Entries), lst.ContinuationToken)
	}
	return
}

//
// LIST BUCKETS
//

func (ap *azureProvider) ListBuckets(_ cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	var (
		o          azblob.ListContainersSegmentOptions
		marker     azblob.Marker
		containers *azblob.ListContainersSegmentResponse
	)
	for marker.NotDone() {
		containers, err = ap.s.ListContainersSegment(azctx, marker, o)
		if err != nil {
			errCode, err = azureErrorToAISError(err, &cmn.Bck{Provider: apc.Azure}, "")
			return
		}

		for idx := range containers.ContainerItems {
			bcks = append(bcks, cmn.Bck{
				Name:     containers.ContainerItems[idx].Name,
				Provider: apc.Azure,
			})
		}
		marker = containers.NextMarker
	}
	return
}

//
// HEAD OBJECT
//

func (ap *azureProvider) HeadObj(ctx context.Context, lom *core.LOM) (oa *cmn.ObjAttrs, errCode int, err error) {
	var (
		resp     *azblob.BlobGetPropertiesResponse
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		blobURL  = cntURL.NewBlobURL(lom.ObjName)
	)
	if resp, err = blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azKeyOptions); err != nil {
		errCode, err = azureErrorToAISError(err, cloudBck, lom.ObjName)
		return
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		err = cmn.NewErrFailedTo(nil, azBackend+": get object props of", cloudBck.Cname(lom.ObjName),
			azErrStatus(resp.StatusCode()))
		errCode = resp.StatusCode()
		return
	}

	oa = &cmn.ObjAttrs{}
	oa.CustomMD = make(cos.StrKVs, 6)
	oa.SetCustomKey(cmn.SourceObjMD, apc.Azure)
	oa.Size = resp.ContentLength()

	etag := azEncodeEtag(resp.ETag())
	oa.SetCustomKey(cmn.ETag, etag)

	if v := resp.VersionID(); v != "" {
		oa.Ver = v
	} else {
		oa.Ver = etag // NOTE: (ListObjects and elsewhere)
	}
	if md5 := azEncodeChecksum(resp.ContentMD5()); md5 != "" {
		oa.SetCustomKey(cmn.MD5ObjMD, md5)
	}
	if v := resp.LastModified(); !v.IsZero() {
		oa.SetCustomKey(cmn.LastModified, fmtTime(v))
	}
	if v := resp.ContentType(); v != "" {
		// unlike other custom attrs, "Content-Type" is not getting stored w/ LOM
		// - only shown via list-objects and HEAD when not present
		oa.SetCustomKey(cos.HdrContentType, v)
	}
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infof("[head_object] %s", lom)
	}
	return
}

//
// GET OBJECT
//

func (ap *azureProvider) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT) (int, error) {
	res := ap.GetObjReader(ctx, lom)
	if res.Err != nil {
		return res.ErrCode, res.Err
	}
	params := allocPutParams(res, owt)
	err := ap.t.PutObject(lom, params)
	core.FreePutParams(params)
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("[get_object]", lom.String(), err)
	}
	return 0, err
}

func (ap *azureProvider) GetObjReader(ctx context.Context, lom *core.LOM) (res core.GetReaderResult) {
	var (
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		blobURL  = cntURL.NewBlobURL(lom.ObjName)
	)
	// Get checksum
	respProps, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azKeyOptions)
	if err != nil {
		res.ErrCode, res.Err = azureErrorToAISError(err, cloudBck, lom.ObjName)
		return
	}
	if respProps.StatusCode() >= http.StatusBadRequest {
		res.Err = cmn.NewErrFailedTo(nil, azBackend+": get object props of", cloudBck.Cname(lom.ObjName),
			azErrStatus(respProps.StatusCode()))
		res.ErrCode = respProps.StatusCode()
		return
	}
	// (0, 0) read range: whole object
	resp, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azKeyOptions)
	if err != nil {
		res.ErrCode, res.Err = azureErrorToAISError(err, cloudBck, lom.ObjName)
		return
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		res.Err = cmn.NewErrFailedTo(nil, azBackend+": get object", cloudBck.Cname(lom.ObjName),
			azErrStatus(respProps.StatusCode()))
		res.ErrCode = resp.StatusCode()
		return
	}

	res.Size = resp.ContentLength()
	//
	// custom metadata
	//
	lom.SetCustomKey(cmn.SourceObjMD, apc.Azure)
	etag := azEncodeEtag(respProps.ETag())
	lom.SetCustomKey(cmn.ETag, etag)
	if v := respProps.VersionID(); v != "" {
		lom.SetVersion(v)
	} else {
		lom.SetVersion(etag) // (ListObjects and elsewhere)
	}
	if md5 := azEncodeChecksum(respProps.ContentMD5()); md5 != "" {
		lom.SetCustomKey(cmn.MD5ObjMD, md5)
		res.ExpCksum = cos.NewCksum(cos.ChecksumMD5, md5)
	}

	retryOpts := azblob.RetryReaderOptions{MaxRetryRequests: 1} // NOTE: just once
	res.R = resp.Body(retryOpts)
	return
}

//
// PUT OBJECT
//

func (ap *azureProvider) PutObj(r io.ReadCloser, lom *core.LOM) (int, error) {
	defer cos.Close(r)

	var (
		leaseID  string
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

	// NOTE:
	// - use BlockBlob instead of PageBlob because the latter requires object size to be divisible by 512.
	// - without buffer options(with 0's) UploadStreamToBlockBlob appears to hang
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
		err := cmn.NewErrFailedTo(nil, azBackend+": PUT", cloudBck.Cname(lom.ObjName),
			azErrStatus(resp.StatusCode))
		return resp.StatusCode, err
	}

	etag := azEncodeEtag(putResp.ETag())
	lom.SetCustomKey(cmn.ETag, etag)
	if v := putResp.Version(); v != "" {
		lom.SetVersion(v)
	} else {
		lom.SetVersion(etag)
	}
	if v := putResp.LastModified(); !v.IsZero() {
		lom.SetCustomKey(cmn.LastModified, fmtTime(v))
	}
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infof("[put_object] %s", lom)
	}
	return http.StatusOK, nil
}

//
// DELETE OBJECT
//

func (ap *azureProvider) DeleteObj(lom *core.LOM) (int, error) {
	var (
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(lom.Bck().Name)
		blobURL  = cntURL.NewBlobURL(lom.ObjName)
	)
	delCond := azblob.BlobAccessConditions{
		LeaseAccessConditions: azblob.LeaseAccessConditions{},
	}
	delResp, err := blobURL.Delete(azctx, azblob.DeleteSnapshotsOptionInclude, delCond)

	if err != nil {
		return azureErrorToAISError(err, cloudBck, lom.ObjName)
	}
	if delResp.StatusCode() >= http.StatusBadRequest {
		err := cmn.NewErrFailedTo(nil, azBackend+": delete object", cloudBck.Cname(lom.ObjName), azErrStatus(delResp.StatusCode()))
		return delResp.StatusCode(), err
	}
	return http.StatusOK, nil
}
