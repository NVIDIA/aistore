//go:build gcp

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
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
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tracing"

	"cloud.google.com/go/storage"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

const (
	gcpXMLEndpoint  = "https://storage.googleapis.com"
	gcpChecksumType = "x-goog-meta-ais-cksum-type"
	gcpChecksumVal  = "x-goog-meta-ais-cksum-val"

	projectIDField  = "project_id"
	projectIDEnvVar = "GOOGLE_CLOUD_PROJECT"
	credPathEnvVar  = "GOOGLE_APPLICATION_CREDENTIALS" //nolint:gosec // false positive G101
)

type (
	gsbp struct {
		t        core.TargetPut
		dfltSess gcpSess
		base
	}
	gcpSess struct {
		client     *storage.Client
		httpClient *http.Client
		projectID  string
	}
)

var (
	appSess sync.Map // additional [credPath => *gcpSess]
)

// interface guard
var _ core.Backend = (*gsbp)(nil)

func NewGCP(t core.TargetPut, tstats stats.Tracker, startingUp bool) (core.Backend, error) {
	var (
		projectID     string
		credProjectID string
		envProjectID  = os.Getenv(projectIDEnvVar)
	)
	credPath := os.Getenv(credPathEnvVar)
	if credPath != "" {
		var err error
		if credProjectID, err = readCredFile(credPath); err != nil {
			nlog.Warningf("failed to load application creds %q: %v", credPath, err)
		}
	}

	if credProjectID != "" && envProjectID != "" && credProjectID != envProjectID {
		return nil, fmt.Errorf("both %q and %q env vars cannot be defined (and not equal %s)",
			projectIDEnvVar, credPathEnvVar, projectIDField)
	}

	switch {
	case credProjectID != "":
		projectID = credProjectID
		nlog.Infof("%s: %q (using %q env)", projectIDField, projectID, credPathEnvVar)
	case envProjectID != "":
		projectID = envProjectID
		nlog.Infof("%s: %q (using %q env)", projectIDField, projectID, projectIDEnvVar)
	}

	bp := &gsbp{
		t:        t,
		dfltSess: gcpSess{projectID: projectID},
		base:     base{provider: apc.GCP},
	}

	opts := []option.ClientOption{option.WithScopes(storage.ScopeFullControl)}
	if projectID == "" {
		nlog.Warningln("unauthenticated client")
		opts = append(opts, option.WithoutAuthentication())
	}
	if err := bp.dfltSess.init(context.Background(), opts); err != nil {
		return nil, err
	}

	// register metrics
	bp.base.init(t.Snode(), tstats, startingUp)

	return bp, nil
}

// as core.Backend --------------------------------------------------------------

//
// HEAD BUCKET
//

func (gsbp *gsbp) HeadBucket(ctx context.Context, bck *meta.Bck) (cos.StrKVs, int, error) {
	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("head_bucket %s", bck.Name)
	}
	cloudBck := bck.RemoteBck()
	client, e := gsbp.getClient(ctx, cloudBck)
	if e != nil {
		return nil, 0, e
	}
	if _, err := client.Bucket(cloudBck.Name).Attrs(ctx); err != nil {
		ecode, err := gcpErrorToAISError(err, cloudBck)
		return nil, ecode, err
	}
	//
	// NOTE: return a few assorted fields, specifically to fill-in vendor-specific `cmn.ExtraProps`
	//
	bckProps := make(cos.StrKVs)
	bckProps[apc.HdrBackendProvider] = apc.GCP
	// GCP always generates a versionid for an object even if versioning is disabled.
	// So, return that we can detect versionid change on getobj etc
	bckProps[apc.HdrBucketVerEnabled] = "true"
	return bckProps, 0, nil
}

//
// LIST OBJECTS
//

func (gsbp *gsbp) ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (int, error) {
	var (
		query    *storage.Query
		h        = cmn.BackendHelpers.Google
		cloudBck = bck.RemoteBck()
	)
	client, e := gsbp.getClient(context.Background(), cloudBck)
	if e != nil {
		return 0, e
	}

	msg.PageSize = calcPageSize(msg.PageSize, bck.MaxPageSize())

	// in re: `apc.LsNoDirs` and `apc.LsNoRecursion`, see:
	// https://github.com/NVIDIA/aistore/blob/main/docs/howto_virt_dirs.md

	if prefix := msg.Prefix; prefix != "" {
		query = &storage.Query{Prefix: prefix}
		if msg.IsFlagSet(apc.LsNoRecursion) {
			query.Delimiter = "/"
		}
	} else if msg.IsFlagSet(apc.LsNoRecursion) {
		query = &storage.Query{Delimiter: "/"}
	}

	var (
		it    = client.Bucket(cloudBck.Name).Objects(context.Background(), query)
		pager = iterator.NewPager(it, int(msg.PageSize), msg.ContinuationToken)
		objs  = make([]*storage.ObjectAttrs, 0, msg.PageSize)
	)
	nextPageToken, errPage := pager.NextPage(&objs)
	if errPage != nil {
		if cmn.Rom.V(4, cos.ModBackend) {
			nlog.Infof("list_objects %s: %v", cloudBck.Name, errPage)
		}
		return gcpErrorToAISError(errPage, cloudBck)
	}

	lst.ContinuationToken = nextPageToken

	var (
		wantCustom = msg.WantProp(apc.GetPropsCustom)
	)
	lst.Entries = lst.Entries[:0]
	for _, attrs := range objs {
		en := cmn.LsoEnt{Name: attrs.Name, Size: attrs.Size}
		if attrs.Prefix != "" {
			// see "Prefix"
			// ref: https://github.com/googleapis/google-cloud-go/blob/main/storage/storage.go#L1407-L1411
			debug.Assert(attrs.Name == "", attrs.Prefix, " vs ", attrs.Name)
			debug.Assert(query != nil && query.Delimiter != "")

			if msg.IsFlagSet(apc.LsNoDirs) { // do not return virtual subdirectories
				continue
			}
			en.Name = attrs.Prefix
			en.Flags = apc.EntryIsDir
		} else if !msg.IsFlagSet(apc.LsNameOnly) && !msg.IsFlagSet(apc.LsNameSize) {
			if v, ok := h.EncodeCksum(attrs.MD5); ok {
				en.Checksum = v
			}
			if v, ok := h.EncodeVersion(attrs.Generation); ok {
				en.Version = v
			}
			if wantCustom {
				etag, _ := h.EncodeETag(attrs.Etag)
				en.Custom = cmn.CustomProps2S(cmn.ETag, etag, cmn.LsoLastModified, fmtLsoTime(attrs.Updated),
					cos.HdrContentType, attrs.ContentType)
			}
		}
		lst.Entries = append(lst.Entries, &en)
	}

	if cmn.Rom.V(4, cos.ModBackend) {
		nlog.Infof("[list_objects] count %d", len(lst.Entries))
	}

	return 0, nil
}

//
// LIST BUCKETS
//

func (gsbp *gsbp) ListBuckets(_ cmn.QueryBcks) (cmn.Bcks, int, error) {
	if gsbp.dfltSess.projectID == "" {
		// NOTE: empty `projectID` results in obscure: "googleapi: Error 400: Invalid argument"
		return nil, http.StatusBadRequest,
			errors.New("empty project ID: cannot list GCP buckets with no authentication")
	}
	var (
		bcks = make(cmn.Bcks, 0, 16)
		it   = gsbp.dfltSess.client.Buckets(context.Background(), gsbp.dfltSess.projectID)
	)
	for {
		battrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				return bcks, 0, nil
			}
			ecode, errV := gcpErrorToAISError(err, &cmn.Bck{Provider: apc.GCP})
			return bcks, ecode, errV
		}

		bcks = append(bcks, cmn.Bck{Name: battrs.Name, Provider: apc.GCP})
		if cmn.Rom.V(4, cos.ModBackend) {
			nlog.Infof("[bucket_names] %s: created %v, versioning %t", battrs.Name, battrs.Created, battrs.VersioningEnabled)
		}
	}
}

//
// HEAD OBJECT
//

func (gsbp *gsbp) HeadObj(ctx context.Context, lom *core.LOM, _ *http.Request) (*cmn.ObjAttrs, int, error) {
	var (
		h        = cmn.BackendHelpers.Google
		cloudBck = lom.Bck().RemoteBck()
	)
	client, e := gsbp.getClient(ctx, cloudBck)
	if e != nil {
		return nil, 0, e
	}
	attrs, err := client.Bucket(cloudBck.Name).Object(lom.ObjName).Attrs(ctx)
	if err != nil {
		ecode, errV := handleObjectError(ctx, client, err, cloudBck)
		return nil, ecode, errV
	}

	oa := &cmn.ObjAttrs{}
	oa.CustomMD = make(cos.StrKVs, 6)
	oa.SetCustomKey(cmn.SourceObjMD, apc.GCP)
	oa.Size = attrs.Size
	if v, ok := h.EncodeVersion(attrs.Generation); ok {
		oa.SetCustomKey(cmn.VersionObjMD, v)
		oa.SetVersion(v)
	}
	if v, ok := h.EncodeCksum(attrs.MD5); ok {
		oa.SetCustomKey(cmn.MD5ObjMD, v)
	}
	if v, ok := h.EncodeCksum(attrs.CRC32C); ok {
		oa.SetCustomKey(cmn.CRC32CObjMD, v)
	}
	if v, ok := h.EncodeETag(attrs.Etag); ok {
		oa.SetCustomKey(cmn.ETag, v)
	}

	if cksumType, ok := attrs.Metadata[gcpChecksumType]; ok {
		if cksumValue, ok := attrs.Metadata[gcpChecksumVal]; ok {
			oa.SetCksum(cksumType, cksumValue)
		}
	}

	oa.SetCustomKey(cos.HdrLastModified, fmtHdrTime(attrs.Updated))

	// unlike other custom attrs, "Content-Type" is not getting stored w/ LOM
	// - only shown via list-objects and HEAD when not present
	oa.SetCustomKey(cos.HdrContentType, attrs.ContentType)
	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("[head_object] %s", cloudBck.Cname(lom.ObjName))
	}

	return oa, 0, nil
}

//
// GET OBJECT
//

//nolint:dupl // GCP vs Azure: similar code, different BPs
func (gsbp *gsbp) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT, _ *http.Request) (int, error) {
	res := gsbp.GetObjReader(ctx, lom, 0, 0)
	if res.Err != nil {
		return res.ErrCode, res.Err
	}
	params := allocPutParams(res, owt)
	err := gsbp.t.PutObject(lom, params)
	core.FreePutParams(params)
	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infoln("[get_object]", lom.String(), err)
	}
	return 0, err
}

func (gsbp *gsbp) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
	var (
		attrs    *storage.ObjectAttrs
		rc       *storage.Reader
		cloudBck = lom.Bck().RemoteBck()
	)
	client, e := gsbp.getClient(ctx, cloudBck)
	if e != nil {
		return core.GetReaderResult{Err: e}
	}
	o := client.Bucket(cloudBck.Name).Object(lom.ObjName)
	attrs, res.Err = o.Attrs(ctx)
	if res.Err != nil {
		res.ErrCode, res.Err = gcpErrorToAISError(res.Err, cloudBck)
		return res
	}

	// range read
	if length > 0 {
		rc, res.Err = o.NewRangeReader(ctx, offset, length)
		if res.Err != nil {
			if res.ErrCode == http.StatusRequestedRangeNotSatisfiable {
				res.Err = cmn.NewErrRangeNotSatisfiable(res.Err, nil, 0)
			}
			return res
		}
		// NOTE: for range reads, use the requested length, not rc.Attrs.Size (which is the full object size)
		rsize := rc.Remain()
		if rsize < 0 {
			res.Err = errors.New("gcp: returned length is less than 0")
			return res
		}
		if length < rsize {
			res.Err = errors.New("gcp: returned length is more than the requested range-read length")
			return res
		}
		debug.Assertf(offset+rsize <= attrs.Size, "offset + rsize %d > attrs.Size %d", offset+rsize, attrs.Size)
		res.Size = rsize
		res.R = rc
		return res
	}

	// full read
	rc, res.Err = o.NewReader(ctx)
	if res.Err != nil {
		return res
	}
	// custom metadata
	lom.SetCustomKey(cmn.SourceObjMD, apc.GCP)
	if cksumType, ok := attrs.Metadata[gcpChecksumType]; ok {
		if cksumValue, ok := attrs.Metadata[gcpChecksumVal]; ok {
			cksum := cos.NewCksum(cksumType, cksumValue)
			lom.SetCksum(cksum)
			res.ExpCksum = cksum // Use custom checksum as expected checksum
		}
	}

	expCksum := setCustomGs(lom, attrs)
	if res.ExpCksum == nil {
		res.ExpCksum = expCksum
	}

	// For full reads, use rc.Attrs.Size
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
	if v, ok := h.EncodeETag(attrs.Etag); ok {
		lom.SetCustomKey(cmn.ETag, v)
	}

	lom.SetCustomKey(cmn.LsoLastModified, fmtLsoTime(attrs.Updated))
	lom.SetCustomKey(cos.HdrLastModified, fmtHdrTime(attrs.Updated))

	return expCksum
}

//
// PUT OBJECT
//

func (gsbp *gsbp) PutObj(ctx context.Context, r io.ReadCloser, lom *core.LOM, _ *http.Request) (int, error) {
	var (
		cloudBck            = lom.Bck().RemoteBck()
		cksumType, cksumVal = lom.Checksum().Get()
	)
	client, e := gsbp.getClient(ctx, cloudBck)
	if e != nil {
		return 0, e
	}

	o := client.Bucket(cloudBck.Name).Object(lom.ObjName)
	wc := o.NewWriter(ctx)
	wc.Metadata = map[string]string{
		gcpChecksumType: cksumType,
		gcpChecksumVal:  cksumVal,
	}

	buf, slab := gsbp.t.PageMM().Alloc()
	written, err := io.CopyBuffer(wc, r, buf)
	slab.Free(buf)
	cos.Close(r)

	if err != nil {
		return 0, err
	}
	if err := wc.Close(); err != nil {
		return gcpErrorToAISError(err, cloudBck)
	}

	attrs, errV := o.Attrs(ctx)
	if errV != nil {
		return handleObjectError(ctx, client, errV, cloudBck)
	}

	_ = setCustomGs(lom, attrs)
	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("[put_object] %s, size %d", lom, written)
	}
	return 0, nil
}

//
// DELETE OBJECT
//

func (gsbp *gsbp) DeleteObj(ctx context.Context, lom *core.LOM) (int, error) {
	cloudBck := lom.Bck().RemoteBck()
	client, e := gsbp.getClient(ctx, cloudBck)
	if e != nil {
		return 0, e
	}
	o := client.Bucket(cloudBck.Name).Object(lom.ObjName)
	if err := o.Delete(ctx); err != nil {
		return handleObjectError(ctx, client, err, cloudBck)
	}
	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("[delete_object] %s", lom)
	}
	return 0, nil
}

//
// static helpers
//

func readCredFile(path string) (string, error) {
	fh, e := os.Open(path)
	if e != nil {
		return "", e
	}
	b, err := cos.ReadAll(fh)
	fh.Close()
	if err != nil {
		return "", err
	}
	v := jsoniter.Get(b, projectIDField).GetInterface()
	projectID, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("unexpected projectID type (%T)", v)
	}
	debug.Assert(projectID != "")
	return projectID, nil
}

func createSess(ctx context.Context, credPath string, bck *cmn.Bck) (*gcpSess, error) {
	debug.Assert(credPath != "") // via bprops "extra.gcp.application_creds"
	var (
		projectID string
		opts      = []option.ClientOption{option.WithScopes(storage.ScopeFullControl)}
		err       error
	)
	projectID, err = readCredFile(credPath)
	if projectID == "" {
		var s string
		if bck != nil {
			s = bck.Cname("")
		}
		return nil, fmt.Errorf("failed to load application creds %q for bucket %q: %v", credPath, s, err)
	}
	opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, credPath))

	sess := &gcpSess{projectID: projectID}
	return sess, sess.init(ctx, opts)
}

func (sess *gcpSess) init(ctx context.Context, opts []option.ClientOption) error {
	// HTTP transport
	transport, err := htransport.NewTransport(ctx, cmn.NewTransport(cmn.TransportArgs{}), opts...)
	if err != nil {
		return cmn.NewErrFailedTo(nil, "gcp-backend: create", "http transport", err)
	}

	// HTTP client for raw (multipart upload) requests
	sess.httpClient = tracing.NewTraceableClient(&http.Client{Transport: transport})
	opts = append(opts, option.WithHTTPClient(sess.httpClient))

	// cloud.google.com/go/storage client
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return cmn.NewErrFailedTo(nil, "gcp-backend: create", "client", err)
	}

	sess.client = client
	return nil
}

func (gsbp *gsbp) getClient(ctx context.Context, bck *cmn.Bck) (*storage.Client, error) {
	sess, err := gsbp.getSess(ctx, bck)
	if err != nil {
		return nil, err
	}
	return sess.client, nil
}

func (gsbp *gsbp) getSess(ctx context.Context, bck *cmn.Bck) (*gcpSess, error) {
	var credPath string
	if bck != nil && bck.Props != nil {
		credPath = bck.Props.Extra.GCP.ApplicationCreds
	}
	if credPath == "" {
		return &gsbp.dfltSess, nil
	}

	// fast path
	if v, loaded := appSess.Load(credPath); loaded {
		return v.(*gcpSess), nil
	}

	// slow path - once per unique creds
	sess, err := createSess(ctx, credPath, bck)
	if err != nil {
		return nil, err
	}
	appSess.Store(credPath, sess)
	return sess, nil
}

//
// errors
//

const gcpErrPrefix = "gcp-error"

func gcpErrorToAISError(gcpError error, bck *cmn.Bck) (int, error) {
	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.InfoDepth(1, "begin "+gcpErrPrefix+" =========================")
		nlog.InfoDepth(1, gcpError)
		nlog.InfoDepth(1, "end "+gcpErrPrefix+" ===========================")
	}
	if gcpError == storage.ErrBucketNotExist {
		return http.StatusNotFound, cmn.NewErrRemBckNotFound(bck)
	}
	err := _gcpErr(gcpError)
	if gcpError == storage.ErrObjectNotExist {
		return http.StatusNotFound, err
	}
	apiErr, ok := gcpError.(*googleapi.Error)
	switch {
	case !ok:
		return http.StatusInternalServerError, err
	case apiErr.Code == http.StatusForbidden && strings.Contains(apiErr.Error(), "may not exist"):
		// HACK: "not found or misspelled" vs  "service not paid for" (the latter less likely)
		if cmn.Rom.V(4, cos.ModBackend) {
			nlog.Infoln(err)
		}
		return http.StatusNotFound, err
	case apiErr.Code == http.StatusTooManyRequests || apiErr.Code == http.StatusServiceUnavailable:
		return apiErr.Code, cmn.NewErrTooManyRequests(err, apiErr.Code)
	default:
		return apiErr.Code, err
	}
}

// (compare w/ _awsErr)
func _gcpErr(gcpError error) error {
	return errors.New(gcpErrPrefix + "[" + gcpError.Error() + "]")
}

func handleObjectError(ctx context.Context, gcpClient *storage.Client, objErr error, bck *cmn.Bck) (int, error) {
	// TODO: consider additionally checking for `*googleapi.Error{Code:404}`
	if !errors.Is(objErr, storage.ErrObjectNotExist) {
		return http.StatusBadRequest, _gcpErr(objErr)
	}

	// Object does not exist but in GCP it doesn't necessarily mean that the bucket does.
	if _, err := gcpClient.Bucket(bck.Name).Attrs(ctx); err != nil {
		return gcpErrorToAISError(err, bck)
	}
	return http.StatusNotFound, cos.NewErrNotFound(nil, _gcpErr(objErr).Error())
}
