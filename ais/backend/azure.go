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
	"github.com/NVIDIA/aistore/cmn/debug"
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
	azHost         = ".blob.core.windows.net"

	azAccNameEnvVar = "AZURE_STORAGE_ACCOUNT"
	azAccKeyEnvVar  = "AZURE_STORAGE_KEY"

	// ais
	azURLEnvVar   = "AIS_AZURE_URL"
	azProtoEnvVar = "AIS_AZURE_PROTO"
)

// used to parse azure errors
const (
	azErrDesc = "Description"
	azErrCode = "Code: "

	azErrPrefix = "azure-error["
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
		return nil, cmn.NewErrFailedTo(core.T, azErrPrefix+": parse]", "URL", err)
	}
	name := azUserName()
	key := azUserKey()
	creds, err := azblob.NewSharedKeyCredential(name, key)
	if err != nil {
		return nil, cmn.NewErrFailedTo(nil, azErrPrefix+": init]", "credentials", err)
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

// (compare w/ cmn/backend)
func azEncodeEtag(etag azblob.ETag) string { return cmn.UnquoteCEV(string(etag)) }

func azEncodeChecksum(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	return hex.EncodeToString(v)
}

//
// format and parse errors
//

func azureErrorToAISError(azureError error, bck *cmn.Bck, objName string) (int, error) {
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("begin azure error =========================")
		nlog.Infoln(azureError)
		nlog.Infoln("end azure error ===========================")
	}

	stgErr, ok := azureError.(azblob.StorageError)
	if !ok {
		return http.StatusInternalServerError, azureError
	}

	switch stgErr.ServiceCode() {
	case azblob.ServiceCodeContainerNotFound:
		return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(bck)
	case azblob.ServiceCodeBlobNotFound:
		return http.StatusNotFound, errors.New(azErrPrefix + "NotFound: " + bck.Cname(objName) + "]")
	case azblob.ServiceCodeInvalidResourceName:
		if objName != "" {
			return http.StatusNotFound, errors.New(azErrPrefix + "NotFound: " + bck.Cname(objName) + "]")
		}
	}

	// azure error is usually a sizeable multi-line text with items including:
	// request ID, authorization, variery of x-ms-* headers, server and user agent, and more

	var (
		status      = http.StatusInternalServerError
		code        string
		description string
		lines       = strings.Split(azureError.Error(), "\n")
	)
	if resp := stgErr.Response(); resp != nil {
		resp.Body.Close()
		status = resp.StatusCode
	}
	for _, line := range lines {
		if strings.HasPrefix(line, azErrDesc) {
			description = azCleanErrRegex.ReplaceAllString(line[len(azErrDesc):], "")
		} else if i := strings.Index(line, azErrCode); i > 0 {
			code = azCleanErrRegex.ReplaceAllString(line[i+len(azErrCode):], "")
		}
	}
	if code != "" && description != "" {
		return status, errors.New(azErrPrefix + code + ": " + description + "]")
	}
	debug.Assert(false, azureError) // expecting to parse
	return status, azureError
}

// as core.BackendProvider --------------------------------------------------------------

func (*azureProvider) Provider() string { return apc.Azure }

// ref: https://docs.microsoft.com/en-us/connectors/azureblob/#general-limits
func (*azureProvider) MaxPageSize() uint { return 5000 }

//
// CREATE BUCKET
//

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
		err := cmn.NewErrFailedTo(core.T, azErrPrefix+": get bucket props]", cloudBck.Name, nil, resp.StatusCode())
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
		err := cmn.NewErrFailedTo(core.T, azErrPrefix+": list objects]", cloudBck.Name, nil, resp.StatusCode())
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
		err = cmn.NewErrFailedTo(core.T, azErrPrefix+": get object props]", cloudBck.Cname(lom.ObjName), nil, resp.StatusCode())
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
	res := ap.GetObjReader(ctx, lom, 0, 0)
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

func (ap *azureProvider) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
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
		res.Err = cmn.NewErrFailedTo(core.T, azErrPrefix+": get blob props]", cloudBck.Cname(lom.ObjName),
			nil, respProps.StatusCode())
		res.ErrCode = respProps.StatusCode()
		return res
	}
	// (0, 0) range indicates "whole object"
	resp, err := blobURL.Download(ctx, offset, length, azblob.BlobAccessConditions{}, false, azKeyOptions)
	if err != nil {
		res.ErrCode, res.Err = azureErrorToAISError(err, cloudBck, lom.ObjName)
		if res.ErrCode == http.StatusRequestedRangeNotSatisfiable {
			res.Err = cmn.NewErrRangeNotSatisfiable(res.Err, nil, 0)
		}
		return res
	}
	if status := resp.StatusCode(); status >= http.StatusBadRequest {
		res.Err = cmn.NewErrFailedTo(core.T, azErrPrefix+": GET]", cloudBck.Cname(lom.ObjName), nil, status)
		res.ErrCode = status
		return res
	}

	res.Size = resp.ContentLength()

	if length == 0 {
		// custom metadata
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
	}

	retryOpts := azblob.RetryReaderOptions{MaxRetryRequests: 1} // NOTE: just once
	res.R = resp.Body(retryOpts)
	return res
}

//
// PUT OBJECT
//

func (ap *azureProvider) PutObj(r io.ReadCloser, lom *core.LOM, _ *core.ExtraArgsPut) (int, error) {
	defer cos.Close(r)

	var (
		cloudBck = lom.Bck().RemoteBck()
		cntURL   = ap.s.NewContainerURL(cloudBck.Name)
		blobURL  = cntURL.NewBlockBlobURL(lom.ObjName)
	)
	// NOTE:
	// - use BlockBlob instead of PageBlob - the latter requires size to be a multiple of 512
	// - without buffer options(with 0's) UploadStreamToBlockBlob appears to hang
	opts := azblob.UploadStreamToBlockBlobOptions{
		BufferSize: 64 * 1024,
		MaxBuffers: 3,
	}
	putResp, err := azblob.UploadStreamToBlockBlob(azctx, r, blobURL, opts)
	if err != nil {
		status, err := azureErrorToAISError(err, cloudBck, lom.ObjName)
		return status, err
	}

	resp := putResp.Response()
	resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		err := cmn.NewErrFailedTo(core.T, azErrPrefix+": PUT]", cloudBck.Cname(lom.ObjName), nil, resp.StatusCode)
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
	rsp, err := blobURL.Delete(azctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	if err != nil {
		return azureErrorToAISError(err, cloudBck, lom.ObjName)
	}
	if status := rsp.StatusCode(); status >= http.StatusBadRequest {
		err := cmn.NewErrFailedTo(core.T, azErrPrefix+": delete object]", cloudBck.Cname(lom.ObjName), nil, status)
		return rsp.StatusCode(), err
	}
	return http.StatusOK, nil
}
