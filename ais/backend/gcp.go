//go:build gcp
// +build gcp

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"strings"

	"cloud.google.com/go/storage"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
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
	credPathEnvVar  = "GOOGLE_APPLICATION_CREDENTIALS"
)

type (
	gcpProvider struct {
		t         cluster.Target
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
	_ cluster.BackendProvider = (*gcpProvider)(nil)
)

func NewGCP(t cluster.Target) (bp cluster.BackendProvider, err error) {
	var (
		projectID     string
		credProjectID = readCredFile()
		envProjectID  = os.Getenv(projectIDEnvVar)
	)
	if credProjectID != "" && envProjectID != "" && credProjectID != envProjectID {
		err = fmt.Errorf("both %q and %q env vars cannot be defined (and not equal %s)",
			projectIDEnvVar, credPathEnvVar, projectIDField)
		return
	} else if credProjectID != "" {
		projectID = credProjectID
		glog.Infof("[cloud_gcp] %s: %q (using %q env variable)", projectIDField, projectID, credPathEnvVar)
	} else if envProjectID != "" {
		projectID = envProjectID
		glog.Infof("[cloud_gcp] %s: %q (using %q env variable)", projectIDField, projectID, projectIDEnvVar)
	} else {
		glog.Warningf("[cloud_gcp] unable to determine %q (%q and %q env vars are empty) - using unauthenticated client",
			projectIDField, projectIDEnvVar, credPathEnvVar)
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
		return nil, fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderGoogle, "create", "http transport", err)
	}
	opts = append(opts, option.WithHTTPClient(&http.Client{Transport: transport}))
	// create HTTP client
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf(cmn.FmtErrFailed, cmn.ProviderGoogle, "create", "client", err)
	}
	return client, nil
}

func (*gcpProvider) Provider() string { return cmn.ProviderGoogle }

// https://cloud.google.com/storage/docs/json_api/v1/objects/list#parameters
func (*gcpProvider) MaxPageSize() uint { return 1000 }

///////////////////
// CREATE BUCKET //
///////////////////

func (gcpp *gcpProvider) CreateBucket(_ *cluster.Bck) (errCode int, err error) {
	return creatingBucketNotSupportedErr(gcpp.Provider())
}

/////////////////
// HEAD BUCKET //
/////////////////

func (*gcpProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cos.SimpleKVs, errCode int, err error) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("head_bucket %s", bck.Name)
	}
	cloudBck := bck.RemoteBck()
	_, err = gcpClient.Bucket(cloudBck.Name).Attrs(ctx)
	if err != nil {
		errCode, err = gcpErrorToAISError(err, cloudBck)
		return
	}
	bckProps = make(cos.SimpleKVs)
	bckProps[cmn.HdrBackendProvider] = cmn.ProviderGoogle
	// GCP always generates a versionid for an object even if versioning is disabled.
	// So, return that we can detect versionid change on getobj etc
	bckProps[cmn.HdrBucketVerEnabled] = "true"
	return
}

//////////////////
// LIST OBJECTS //
//////////////////

func (gcpp *gcpProvider) ListObjects(bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList,
	errCode int, err error) {
	msg.PageSize = calcPageSize(msg.PageSize, gcpp.MaxPageSize())
	var (
		query    *storage.Query
		h        = cmn.BackendHelpers.Google
		cloudBck = bck.RemoteBck()
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("list_objects %s", cloudBck.Name)
	}

	if msg.Prefix != "" {
		query = &storage.Query{Prefix: msg.Prefix}
	}

	var (
		it    = gcpClient.Bucket(cloudBck.Name).Objects(gctx, query)
		pager = iterator.NewPager(it, int(msg.PageSize), msg.ContinuationToken)
		objs  = make([]*storage.ObjectAttrs, 0, msg.PageSize)
	)
	nextPageToken, err := pager.NextPage(&objs)
	if err != nil {
		errCode, err = gcpErrorToAISError(err, cloudBck)
		return
	}

	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, len(objs))}
	bckList.ContinuationToken = nextPageToken
	for _, attrs := range objs {
		entry := &cmn.BucketEntry{}
		entry.Name = attrs.Name
		if msg.WantProp(cmn.GetPropsSize) {
			entry.Size = attrs.Size
		}
		if msg.WantProp(cmn.GetPropsChecksum) {
			if v, ok := h.EncodeCksum(attrs.MD5); ok {
				entry.Checksum = v
			}
		}
		if msg.WantProp(cmn.GetPropsVersion) {
			if v, ok := h.EncodeVersion(attrs.Generation); ok {
				entry.Version = v
			}
		}
		bckList.Entries = append(bckList.Entries, entry)
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[list_bucket] count %d", len(bckList.Entries))
	}

	return
}

//////////////////
// LIST BUCKETS //
//////////////////

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
			errCode, err = gcpErrorToAISError(err, &cmn.Bck{Provider: cmn.ProviderGoogle})
			return
		}
		bcks = append(bcks, cmn.Bck{
			Name:     battrs.Name,
			Provider: cmn.ProviderGoogle,
		})
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("[bucket_names] %s: created %v, versioning %t",
				battrs.Name, battrs.Created, battrs.VersioningEnabled)
		}
	}
	return
}

/////////////////
// HEAD OBJECT //
/////////////////

func (*gcpProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cos.SimpleKVs, errCode int, err error) {
	var (
		h        = cmn.BackendHelpers.Google
		cloudBck = lom.Bck().RemoteBck()
	)
	attrs, err := gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName).Attrs(ctx)
	if err != nil {
		errCode, err = handleObjectError(ctx, gcpClient, err, cloudBck)
		return
	}
	objMeta = make(cos.SimpleKVs)
	objMeta[cmn.HdrBackendProvider] = cmn.ProviderGoogle
	objMeta[cmn.HdrObjSize] = strconv.FormatInt(attrs.Size, 10)
	if v, ok := h.EncodeVersion(attrs.Generation); ok {
		objMeta[cmn.HdrObjVersion] = v
	}
	if v, ok := h.EncodeCksum(attrs.MD5); ok {
		objMeta[cluster.MD5ObjMD] = v
	}
	if v, ok := h.EncodeCksum(attrs.CRC32C); ok {
		objMeta[cluster.CRC32CObjMD] = v
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_object] %s/%s", cloudBck, lom.ObjName)
	}
	return
}

////////////////
// GET OBJECT //
////////////////

func (gcpp *gcpProvider) GetObj(ctx context.Context, lom *cluster.LOM) (errCode int, err error) {
	reader, cksumToUse, errCode, err := gcpp.GetObjReader(ctx, lom)
	if err != nil {
		return errCode, err
	}
	params := cluster.PutObjectParams{
		Tag:      fs.WorkfileColdget,
		Reader:   reader,
		RecvType: cluster.ColdGet,
		Cksum:    cksumToUse,
	}
	err = gcpp.t.PutObject(lom, params)
	if err != nil {
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[get_object] %s", lom)
	}
	return
}

///////////////////////
// GET OBJECT READER //
///////////////////////

func (*gcpProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (r io.ReadCloser, expectedCksm *cos.Cksum,
	errCode int, err error) {
	var (
		h        = cmn.BackendHelpers.Google
		cloudBck = lom.Bck().RemoteBck()
		o        = gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName)
	)
	attrs, err := o.Attrs(ctx)
	if err != nil {
		errCode, err = gcpErrorToAISError(err, cloudBck)
		return nil, nil, errCode, err
	}

	cksum := cos.NewCksum(attrs.Metadata[gcpChecksumType], attrs.Metadata[gcpChecksumVal])
	rc, err := o.NewReader(ctx)
	if err != nil {
		return nil, nil, 0, err
	}

	customMD := cos.SimpleKVs{
		cluster.SourceObjMD: cluster.SourceGoogleObjMD,
	}

	if v, ok := h.EncodeVersion(attrs.Generation); ok {
		lom.SetVersion(v)
		customMD[cluster.VersionObjMD] = v
	}
	if v, ok := h.EncodeCksum(attrs.MD5); ok {
		expectedCksm = cos.NewCksum(cos.ChecksumMD5, v)
		customMD[cluster.MD5ObjMD] = v
	}
	if v, ok := h.EncodeCksum(attrs.CRC32C); ok {
		customMD[cluster.CRC32CObjMD] = v
	}

	lom.SetCksum(cksum)
	lom.SetCustom(customMD)
	setSize(ctx, rc.Attrs.Size)
	r = wrapReader(ctx, rc)
	return
}

////////////////
// PUT OBJECT //
////////////////

func (gcpp *gcpProvider) PutObj(r io.ReadCloser, lom *cluster.LOM) (version string, errCode int, err error) {
	var (
		h        = cmn.BackendHelpers.Google
		cloudBck = lom.Bck().RemoteBck()
		md       = make(cos.SimpleKVs, 2)
		gcpObj   = gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName)
		wc       = gcpObj.NewWriter(gctx)
	)
	md[gcpChecksumType], md[gcpChecksumVal] = lom.Checksum().Get()

	wc.Metadata = md
	buf, slab := gcpp.t.MMSA().Alloc()
	written, err := io.CopyBuffer(wc, r, buf)
	slab.Free(buf)
	cos.Close(r)
	if err != nil {
		return
	}
	if err = wc.Close(); err != nil {
		errCode, err = gcpErrorToAISError(err, cloudBck)
		return
	}
	attr, err := gcpObj.Attrs(gctx)
	if err != nil {
		errCode, err = handleObjectError(gctx, gcpClient, err, cloudBck)
		return
	}
	if v, ok := h.EncodeVersion(attr.Generation); ok {
		version = v
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[put_object] %s, size %d, version %s", lom, written, version)
	}
	return
}

///////////////////
// DELETE OBJECT //
///////////////////

func (*gcpProvider) DeleteObj(lom *cluster.LOM) (errCode int, err error) {
	var (
		cloudBck = lom.Bck().RemoteBck()
		o        = gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName)
	)
	if err = o.Delete(gctx); err != nil {
		errCode, err = handleObjectError(gctx, gcpClient, err, cloudBck)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[delete_object] %s", lom)
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

func gcpErrorToAISError(gcpError error, bck *cmn.Bck) (int, error) {
	if gcpError == storage.ErrBucketNotExist {
		return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(*bck)
	}
	status := http.StatusBadRequest
	if apiErr, ok := gcpError.(*googleapi.Error); ok {
		status = apiErr.Code
	} else if gcpError == storage.ErrObjectNotExist {
		status = http.StatusNotFound
	}
	return status, gcpError
}

func handleObjectError(ctx context.Context, gcpClient *storage.Client, objErr error, bck *cmn.Bck) (int, error) {
	if objErr != storage.ErrObjectNotExist {
		return http.StatusBadRequest, objErr
	}

	// Object does not exist, but in GCP it doesn't mean that the bucket existed.
	// Check if the buckets exists.
	if _, err := gcpClient.Bucket(bck.Name).Attrs(ctx); err != nil {
		return gcpErrorToAISError(err, bck)
	}
	return http.StatusNotFound, cmn.NewErrNotFound(objErr.Error())
}
