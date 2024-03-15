//go:build gcp

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

const (
	gcpChecksumType = "x-goog-meta-ais-cksum-type"
	gcpChecksumVal  = "x-goog-meta-ais-cksum-val"

	projectIDField  = "project_id"
	projectIDEnvVar = "GOOGLE_CLOUD_PROJECT"
	credPathEnvVar  = "GOOGLE_APPLICATION_CREDENTIALS" //nolint:gosec // false positive G101
)

type (
	gcpProvider struct {
		t         core.TargetPut
		projectID string
	}
)

var (
	// quoting Google SDK:
	//    "Clients should be reused instead of created as needed. The methods of Client
	//     are safe for concurrent use by multiple goroutines.
	//     The default scope is ScopeFullControl."
	gcpClient *storage.Client

	// context placeholder
	gctx context.Context

	// interface guard
	_ core.BackendProvider = (*gcpProvider)(nil)
)

func NewGCP(t core.TargetPut) (bp core.BackendProvider, err error) {
	var (
		projectID     string
		credProjectID = readCredFile()
		envProjectID  = os.Getenv(projectIDEnvVar)
	)
	if credProjectID != "" && envProjectID != "" && credProjectID != envProjectID {
		err = fmt.Errorf("both %q and %q env vars cannot be defined (and not equal %s)",
			projectIDEnvVar, credPathEnvVar, projectIDField)
		return
	}
	switch {
	case credProjectID != "":
		projectID = credProjectID
		nlog.Infof("%s: %q (using %q env)", projectIDField, projectID, credPathEnvVar)
	case envProjectID != "":
		projectID = envProjectID
		nlog.Infof("%s: %q (using %q env)", projectIDField, projectID, projectIDEnvVar)
	default:
		nlog.Warningln("unauthenticated client")
	}
	gcpp := &gcpProvider{t: t, projectID: projectID}
	bp = gcpp

	gctx = context.Background()
	gcpClient, err = gcpp.createClient(gctx)
	return
}

func (gcpp *gcpProvider) createClient(ctx context.Context) (*storage.Client, error) {
	opts := []option.ClientOption{option.WithScopes(storage.ScopeFullControl)}
	if gcpp.projectID == "" {
		opts = append(opts, option.WithoutAuthentication())
	}
	// create HTTP transport
	transport, err := htransport.NewTransport(ctx, cmn.NewTransport(cmn.TransportArgs{}), opts...)
	if err != nil {
		if strings.Contains(err.Error(), "credentials") {
			details := fmt.Sprintf("%s Hint: check your %q and %q environment settings for project ID=%q.",
				err, projectIDEnvVar, credPathEnvVar, gcpp.projectID)
			return nil, errors.New(details)
		}
		return nil, cmn.NewErrFailedTo(nil, "gcp-backend: create", "http transport", err)
	}
	opts = append(opts, option.WithHTTPClient(&http.Client{Transport: transport}))
	// create HTTP client
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, cmn.NewErrFailedTo(nil, "gcp-backend: create", "client", err)
	}
	return client, nil
}

// as core.BackendProvider --------------------------------------------------------------

func (*gcpProvider) Provider() string { return apc.GCP }

//
// CREATE BUCKET
//

func (*gcpProvider) CreateBucket(_ *meta.Bck) (int, error) {
	return http.StatusNotImplemented, cmn.NewErrNotImpl("create", "gs:// bucket")
}

//
// HEAD BUCKET
//

func (*gcpProvider) HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, errCode int, err error) {
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infof("head_bucket %s", bck.Name)
	}
	cloudBck := bck.RemoteBck()
	_, err = gcpClient.Bucket(cloudBck.Name).Attrs(ctx)
	if err != nil {
		errCode, err = gcpErrorToAISError(err, cloudBck)
		return
	}
	//
	// NOTE: return a few assorted fields, specifically to fill-in vendor-specific `cmn.ExtraProps`
	//
	bckProps = make(cos.StrKVs)
	bckProps[apc.HdrBackendProvider] = apc.GCP
	// GCP always generates a versionid for an object even if versioning is disabled.
	// So, return that we can detect versionid change on getobj etc
	bckProps[apc.HdrBucketVerEnabled] = "true"
	return
}

//
// LIST OBJECTS
//

func (gcpp *gcpProvider) ListObjectsInv(*meta.Bck, *apc.LsoMsg, *cmn.LsoResult, *core.LsoInventoryCtx) (int, error) {
	return 0, newErrInventory(gcpp.Provider())
}

func (*gcpProvider) ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoResult) (errCode int, err error) {
	var (
		query    *storage.Query
		h        = cmn.BackendHelpers.Google
		cloudBck = bck.RemoteBck()
	)
	msg.PageSize = calcPageSize(msg.PageSize, bck.MaxPageSize())

	if prefix := msg.Prefix; prefix != "" {
		var delim string
		if msg.IsFlagSet(apc.LsNoRecursion) {
			// NOTE: important to indicate subdirectory with trailing '/'
			if cos.IsLastB(prefix, '/') {
				delim = "/"
			}
		}
		query = &storage.Query{Prefix: prefix, Delimiter: delim}
	}

	var (
		it    = gcpClient.Bucket(cloudBck.Name).Objects(gctx, query)
		pager = iterator.NewPager(it, int(msg.PageSize), msg.ContinuationToken)
		objs  = make([]*storage.ObjectAttrs, 0, msg.PageSize)
	)
	nextPageToken, errPage := pager.NextPage(&objs)
	if errPage != nil {
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infof("list_objects %s: %v", cloudBck.Name, errPage)
		}
		errCode, err = gcpErrorToAISError(errPage, cloudBck)
		return
	}

	lst.ContinuationToken = nextPageToken

	var (
		custom = cos.StrKVs{}
		l      = len(objs)
		i      int
	)
	for j := len(lst.Entries); j < l; j++ {
		lst.Entries = append(lst.Entries, &cmn.LsoEntry{}) // add missing empty
	}
	for _, attrs := range objs {
		if msg.IsFlagSet(apc.LsNoRecursion) {
			if attrs.Name == "" {
				entry := lst.Entries[i]
				entry.Name = attrs.Prefix
				entry.Flags = apc.EntryIsDir
				i++
				continue
			}
		}
		entry := lst.Entries[i]
		i++
		entry.Name, entry.Size = attrs.Name, attrs.Size
		if msg.IsFlagSet(apc.LsNameOnly) || msg.IsFlagSet(apc.LsNameSize) {
			continue
		}
		if v, ok := h.EncodeCksum(attrs.MD5); ok {
			entry.Checksum = v
		}
		if v, ok := h.EncodeVersion(attrs.Generation); ok {
			entry.Version = v
		}
		// custom
		if msg.WantProp(apc.GetPropsCustom) {
			custom[cmn.ETag], _ = h.EncodeCksum(attrs.Etag)
			custom[cmn.LastModified] = fmtTime(attrs.Updated)
			custom[cos.HdrContentType] = attrs.ContentType
			entry.Custom = cmn.CustomMD2S(custom)
		}
	}
	lst.Entries = lst.Entries[:i]

	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infof("[list_objects] count %d", len(lst.Entries))
	}
	return
}

//
// LIST BUCKETS
//

func (gcpp *gcpProvider) ListBuckets(_ cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	if gcpp.projectID == "" {
		// NOTE: empty `projectID` results in obscure: "googleapi: Error 400: Invalid argument"
		return nil, http.StatusBadRequest,
			errors.New("empty project ID: cannot list GCP buckets with no authentication")
	}
	bcks = make(cmn.Bcks, 0, 16)
	it := gcpClient.Buckets(gctx, gcpp.projectID)
	for {
		var battrs *storage.BucketAttrs

		battrs, err = it.Next()
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			errCode, err = gcpErrorToAISError(err, &cmn.Bck{Provider: apc.GCP})
			return
		}
		bcks = append(bcks, cmn.Bck{
			Name:     battrs.Name,
			Provider: apc.GCP,
		})
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infof("[bucket_names] %s: created %v, versioning %t",
				battrs.Name, battrs.Created, battrs.VersioningEnabled)
		}
	}
	return
}

//
// HEAD OBJECT
//

func (*gcpProvider) HeadObj(ctx context.Context, lom *core.LOM) (oa *cmn.ObjAttrs, errCode int, err error) {
	var (
		attrs    *storage.ObjectAttrs
		h        = cmn.BackendHelpers.Google
		cloudBck = lom.Bck().RemoteBck()
	)
	attrs, err = gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName).Attrs(ctx)
	if err != nil {
		errCode, err = handleObjectError(ctx, gcpClient, err, cloudBck)
		return
	}
	oa = &cmn.ObjAttrs{}
	oa.CustomMD = make(cos.StrKVs, 6)
	oa.SetCustomKey(cmn.SourceObjMD, apc.GCP)
	oa.Size = attrs.Size
	if v, ok := h.EncodeVersion(attrs.Generation); ok {
		oa.SetCustomKey(cmn.VersionObjMD, v)
		oa.Ver = v
	}
	if v, ok := h.EncodeCksum(attrs.MD5); ok {
		oa.SetCustomKey(cmn.MD5ObjMD, v)
	}
	if v, ok := h.EncodeCksum(attrs.CRC32C); ok {
		oa.SetCustomKey(cmn.CRC32CObjMD, v)
	}
	if v, ok := h.EncodeCksum(attrs.Etag); ok {
		oa.SetCustomKey(cmn.ETag, v)
	}

	if cksumType, ok := attrs.Metadata[gcpChecksumType]; ok {
		if cksumValue, ok := attrs.Metadata[gcpChecksumVal]; ok {
			oa.SetCksum(cksumType, cksumValue)
		}
	}

	oa.SetCustomKey(cmn.LastModified, fmtTime(attrs.Updated))
	// unlike other custom attrs, "Content-Type" is not getting stored w/ LOM
	// - only shown via list-objects and HEAD when not present
	oa.SetCustomKey(cos.HdrContentType, attrs.ContentType)
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infof("[head_object] %s", cloudBck.Cname(lom.ObjName))
	}
	return
}

//
// GET OBJECT
//

func (gcpp *gcpProvider) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT) (int, error) {
	res := gcpp.GetObjReader(ctx, lom, 0, 0)
	if res.Err != nil {
		return res.ErrCode, res.Err
	}
	params := allocPutParams(res, owt)
	err := gcpp.t.PutObject(lom, params)
	core.FreePutParams(params)
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("[get_object]", lom.String(), err)
	}
	return 0, err
}

func (*gcpProvider) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
	var (
		attrs    *storage.ObjectAttrs
		rc       *storage.Reader
		cloudBck = lom.Bck().RemoteBck()
		o        = gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName)
	)
	attrs, res.Err = o.Attrs(ctx)
	if res.Err != nil {
		res.ErrCode, res.Err = gcpErrorToAISError(res.Err, cloudBck)
		return res
	}
	if length > 0 {
		rc, res.Err = o.NewRangeReader(ctx, offset, length)
		if res.Err != nil {
			if res.ErrCode == http.StatusRequestedRangeNotSatisfiable {
				res.Err = cmn.NewErrRangeNotSatisfiable(res.Err, nil, 0)
			}
			return res
		}
	} else {
		rc, res.Err = o.NewReader(ctx)
		if res.Err != nil {
			return res
		}
		// custom metadata
		lom.SetCustomKey(cmn.SourceObjMD, apc.GCP)
		if cksumType, ok := attrs.Metadata[gcpChecksumType]; ok {
			if cksumValue, ok := attrs.Metadata[gcpChecksumVal]; ok {
				lom.SetCksum(cos.NewCksum(cksumType, cksumValue))
			}
		}
		res.ExpCksum = setCustomGs(lom, attrs)
	}

	res.Size = rc.Attrs.Size
	res.R = rc
	return res
}

func setCustomGs(lom *core.LOM, attrs *storage.ObjectAttrs) (expCksum *cos.Cksum) {
	h := cmn.BackendHelpers.Google
	if v, ok := h.EncodeVersion(attrs.Generation); ok {
		lom.SetVersion(v)
		lom.SetCustomKey(cmn.VersionObjMD, v)
	}
	if v, ok := h.EncodeCksum(attrs.MD5); ok {
		lom.SetCustomKey(cmn.MD5ObjMD, v)
		expCksum = cos.NewCksum(cos.ChecksumMD5, v)
	}
	if v, ok := h.EncodeCksum(attrs.CRC32C); ok {
		lom.SetCustomKey(cmn.CRC32CObjMD, v)
		if expCksum == nil {
			expCksum = cos.NewCksum(cos.ChecksumCRC32C, v)
		}
	}
	if v, ok := h.EncodeCksum(attrs.Etag); ok {
		lom.SetCustomKey(cmn.ETag, v)
	}
	lom.SetCustomKey(cmn.LastModified, fmtTime(attrs.Updated))
	return
}

//
// PUT OBJECT
//

func (gcpp *gcpProvider) PutObj(r io.ReadCloser, lom *core.LOM, _ *http.Request) (errCode int, err error) {
	var (
		attrs    *storage.ObjectAttrs
		written  int64
		cloudBck = lom.Bck().RemoteBck()
		md       = make(cos.StrKVs, 2)
		gcpObj   = gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName)
		wc       = gcpObj.NewWriter(gctx)
	)
	md[gcpChecksumType], md[gcpChecksumVal] = lom.Checksum().Get()

	wc.Metadata = md
	buf, slab := gcpp.t.PageMM().Alloc()
	written, err = io.CopyBuffer(wc, r, buf)
	slab.Free(buf)
	cos.Close(r)
	if err != nil {
		return
	}
	if err = wc.Close(); err != nil {
		errCode, err = gcpErrorToAISError(err, cloudBck)
		return
	}
	attrs, err = gcpObj.Attrs(gctx)
	if err != nil {
		errCode, err = handleObjectError(gctx, gcpClient, err, cloudBck)
		return
	}
	_ = setCustomGs(lom, attrs)
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infof("[put_object] %s, size %d", lom, written)
	}
	return
}

//
// DELETE OBJECT
//

func (*gcpProvider) DeleteObj(lom *core.LOM) (errCode int, err error) {
	var (
		cloudBck = lom.Bck().RemoteBck()
		o        = gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName)
	)
	if err = o.Delete(gctx); err != nil {
		errCode, err = handleObjectError(gctx, gcpClient, err, cloudBck)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infof("[delete_object] %s", lom)
	}
	return
}

//
// static helpers
//

func readCredFile() (projectID string) {
	credFile, err := os.Open(os.Getenv(credPathEnvVar))
	if err != nil {
		return
	}
	b, err := io.ReadAll(credFile)
	credFile.Close()
	if err != nil {
		return
	}
	projectID, _ = jsoniter.Get(b, projectIDField).GetInterface().(string)
	return
}

const gcpErrPrefix = "gcp-error"

func gcpErrorToAISError(gcpError error, bck *cmn.Bck) (int, error) {
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.InfoDepth(1, "begin "+gcpErrPrefix+" =========================")
		nlog.InfoDepth(1, gcpError)
		nlog.InfoDepth(1, "end "+gcpErrPrefix+" ===========================")
	}
	if gcpError == storage.ErrBucketNotExist {
		return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(bck)
	}
	err := _gcpErr(gcpError)
	if gcpError == storage.ErrObjectNotExist {
		return http.StatusNotFound, err
	}
	apiErr, ok := gcpError.(*googleapi.Error)
	if !ok {
		return http.StatusInternalServerError, err
	}
	if apiErr.Code == http.StatusForbidden && strings.Contains(apiErr.Error(), "may not exist") {
		// HACK: "not found or misspelled" vs  "service not paid for" (the latter less likely)
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln(err)
		}
		return http.StatusNotFound, err
	}
	return apiErr.Code, err
}

// (compare w/ _awsErr)
func _gcpErr(gcpError error) error {
	return errors.New(gcpErrPrefix + "[" + gcpError.Error() + "]")
}

func handleObjectError(ctx context.Context, gcpClient *storage.Client, objErr error, bck *cmn.Bck) (int, error) {
	if objErr != storage.ErrObjectNotExist {
		return http.StatusBadRequest, _gcpErr(objErr)
	}

	// Object does not exist but in GCP it doesn't necessarily mean that the bucket does.
	if _, err := gcpClient.Bucket(bck.Name).Attrs(ctx); err != nil {
		return gcpErrorToAISError(err, bck)
	}
	return http.StatusNotFound, cos.NewErrNotFound(nil, _gcpErr(objErr).Error())
}
