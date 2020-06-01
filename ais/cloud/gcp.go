// +build gcp

// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"google.golang.org/api/iterator"
)

const (
	gcpChecksumType = "x-goog-meta-ais-cksum-type"
	gcpChecksumVal  = "x-goog-meta-ais-cksum-val"

	gcpPageSize = cmn.DefaultListPageSize
)

type (
	gcpProvider struct {
		t cluster.Target
	}
)

var (
	_ cluster.CloudProvider = &gcpProvider{}
)

func NewGCP(t cluster.Target) (cluster.CloudProvider, error) { return &gcpProvider{t: t}, nil }

func getProjID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

// GCP settings are read from environment variables.
// The function returns:
//   connection to the cloud, GCP context, project_id, error_string
// project_id is used only by listBuckets function

func createClient(ctx context.Context) (*storage.Client, context.Context, string, error) {
	if glog.V(5) {
		glog.Info("Creating default google cloud session")
	}
	if getProjID() == "" {
		return nil, nil, "", errors.New("failed to get ProjectID from GCP")
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to create client, err: %v", err)
	}
	return client, ctx, getProjID(), nil
}

func gcpErrorToAISError(gcpError error, bck cmn.Bck, node string) (error, int) {
	if gcpError == storage.ErrBucketNotExist {
		return cmn.NewErrorRemoteBucketDoesNotExist(bck, node), http.StatusNotFound
	}
	return gcpError, http.StatusBadRequest
}

func (gcpp *gcpProvider) handleObjectError(objErr error, bck *cluster.Bck, gcpBucket *storage.BucketHandle, gctx context.Context) (error, int) {
	if objErr != storage.ErrObjectNotExist {
		return objErr, http.StatusBadRequest
	}

	// Object does not exist, but in gcp it doesn't mean that the bucket existed. Check if the buckets exists
	if _, err := gcpBucket.Attrs(gctx); err != nil {
		return gcpErrorToAISError(err, bck.CloudBck(), gcpp.t.Snode().Name())
	}
	return objErr, http.StatusBadRequest
}

func (gcpp *gcpProvider) Provider() string {
	return cmn.ProviderGoogle
}

//////////////////
// LIST OBJECTS //
//////////////////

func (gcpp *gcpProvider) ListObjects(ctx context.Context, bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("list_bucket %s", bck.Name)
	}
	gcpClient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}
	var (
		query     *storage.Query
		pageToken string
		h         = cmn.CloudHelpers.Google
		cloudBck  = bck.CloudBck()
	)

	if msg.Prefix != "" {
		query = &storage.Query{Prefix: msg.Prefix}
	}
	if msg.PageMarker != "" {
		pageToken = msg.PageMarker
	}

	it := gcpClient.Bucket(cloudBck.Name).Objects(gctx, query)
	pageSize := gcpPageSize
	if msg.PageSize != 0 {
		pageSize = msg.PageSize
	}
	pager := iterator.NewPager(it, pageSize, pageToken)
	objs := make([]*storage.ObjectAttrs, 0)
	nextPageToken, err := pager.NextPage(&objs)
	if err != nil {
		err, errCode = gcpErrorToAISError(err, cloudBck, "")
		return
	}

	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, initialBucketListSize)}
	bckList.PageMarker = nextPageToken
	for _, attrs := range objs {
		entry := &cmn.BucketEntry{}
		entry.Name = attrs.Name
		if strings.Contains(msg.Props, cmn.GetPropsSize) {
			entry.Size = attrs.Size
		}
		if strings.Contains(msg.Props, cmn.GetPropsChecksum) {
			if v, ok := h.EncodeCksum(attrs.MD5); ok {
				entry.Checksum = v
			}
		}
		if strings.Contains(msg.Props, cmn.GetPropsVersion) {
			if v, ok := h.EncodeVersion(attrs.Generation); ok {
				entry.Version = v
			}
		}
		// TODO: other cmn.SelectMsg props TBD
		bckList.Entries = append(bckList.Entries, entry)
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[list_bucket] count %d", len(bckList.Entries))
	}

	return
}

func (gcpp *gcpProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("head_bucket %s", bck.Name)
	}
	bckProps = make(cmn.SimpleKVs)

	gcpClient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}
	var (
		cloudBck = bck.CloudBck()
	)
	_, err = gcpClient.Bucket(cloudBck.Name).Attrs(gctx)
	if err != nil {
		err, errCode = gcpErrorToAISError(err, cloudBck, "")
		return
	}
	bckProps[cmn.HeaderCloudProvider] = cmn.ProviderGoogle
	// GCP always generates a versionid for an object even if versioning is disabled.
	// So, return that we can detect versionid change on getobj etc
	bckProps[cmn.HeaderBucketVerEnabled] = "true"
	return
}

//////////////////
// BUCKET NAMES //
//////////////////

func (gcpp *gcpProvider) ListBuckets(ctx context.Context, _ cmn.QueryBcks) (buckets cmn.BucketNames, err error, errCode int) {
	gcpClient, gctx, projectID, err := createClient(ctx)
	if err != nil {
		return
	}
	buckets = make(cmn.BucketNames, 0, 16)
	it := gcpClient.Buckets(gctx, projectID)
	for {
		var battrs *storage.BucketAttrs

		battrs, err = it.Next()
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			err, errCode = gcpErrorToAISError(err, cmn.Bck{Provider: cmn.ProviderGoogle}, "")
			return
		}
		buckets = append(buckets, cmn.Bck{
			Name:     battrs.Name,
			Provider: cmn.ProviderGoogle,
		})
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("[bucket_names] %s: created %v, versioning %t", battrs.Name, battrs.Created, battrs.VersioningEnabled)
		}
	}
	return
}

/////////////////
// HEAD OBJECT //
/////////////////

func (gcpp *gcpProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	gcpClient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}
	var (
		h        = cmn.CloudHelpers.Google
		cloudBck = lom.Bck().CloudBck()
	)
	attrs, err := gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName).Attrs(gctx)
	if err != nil {
		err, errCode = gcpp.handleObjectError(err, lom.Bck(), gcpClient.Bucket(cloudBck.Name), gctx)
		return
	}
	objMeta = make(cmn.SimpleKVs)
	objMeta[cmn.HeaderCloudProvider] = cmn.ProviderGoogle
	if v, ok := h.EncodeVersion(attrs.Generation); ok {
		objMeta[cmn.HeaderObjVersion] = v
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

func (gcpp *gcpProvider) GetObj(ctx context.Context, workFQN string, lom *cluster.LOM) (err error, errCode int) {
	gcpClient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}
	var (
		h        = cmn.CloudHelpers.Google
		cloudBck = lom.Bck().CloudBck()
		o        = gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName)
	)
	attrs, err := o.Attrs(gctx)
	if err != nil {
		err, errCode = gcpp.handleObjectError(err, lom.Bck(), gcpClient.Bucket(cloudBck.Name), gctx)
		return
	}

	cksum := cmn.NewCksum(attrs.Metadata[gcpChecksumType], attrs.Metadata[gcpChecksumVal])
	rc, err := o.NewReader(gctx)
	if err != nil {
		return
	}

	var (
		cksumToCheck *cmn.Cksum
		customMD     = cmn.SimpleKVs{
			cluster.SourceObjMD: cluster.SourceGoogleObjMD,
		}
	)

	if v, ok := h.EncodeVersion(attrs.Generation); ok {
		lom.SetVersion(v)
		customMD[cluster.VersionObjMD] = v
	}
	if v, ok := h.EncodeCksum(attrs.MD5); ok {
		cksumToCheck = cmn.NewCksum(cmn.ChecksumMD5, v)
		customMD[cluster.MD5ObjMD] = v
	}
	if v, ok := h.EncodeCksum(attrs.CRC32C); ok {
		customMD[cluster.CRC32CObjMD] = v
	}

	lom.SetCksum(cksum)
	lom.SetCustomMD(customMD)
	setSize(ctx, rc.Attrs.Size)
	err = gcpp.t.PutObject(cluster.PutObjectParams{
		LOM:          lom,
		Reader:       wrapReader(ctx, rc),
		WorkFQN:      workFQN,
		RecvType:     cluster.ColdGet,
		Cksum:        cksumToCheck,
		WithFinalize: false,
	})
	if err != nil {
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[get_object] %s", lom)
	}
	return
}

////////////////
// PUT OBJECT //
////////////////

func (gcpp *gcpProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	gcpClient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}

	var (
		h        = cmn.CloudHelpers.Google
		cloudBck = lom.Bck().CloudBck()
		md       = make(cmn.SimpleKVs, 2)
		gcpObj   = gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName)
		wc       = gcpObj.NewWriter(gctx)
	)

	md[gcpChecksumType], md[gcpChecksumVal] = lom.Cksum().Get()

	wc.Metadata = md
	buf, slab := gcpp.t.GetMMSA().Alloc()
	written, err := io.CopyBuffer(wc, r, buf)
	slab.Free(buf)
	if err != nil {
		return
	}
	if err = wc.Close(); err != nil {
		err = fmt.Errorf("failed to close, err: %v", err)
		return
	}
	attr, err := gcpObj.Attrs(gctx)
	if err != nil {
		err, errCode = gcpp.handleObjectError(err, lom.Bck(), gcpClient.Bucket(cloudBck.Name), gctx)
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

func (gcpp *gcpProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int) {
	gcpClient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}
	var (
		cloudBck = lom.Bck().CloudBck()
		o        = gcpClient.Bucket(cloudBck.Name).Object(lom.ObjName)
	)

	if err = o.Delete(gctx); err != nil {
		err, errCode = gcpp.handleObjectError(err, lom.Bck(), gcpClient.Bucket(cloudBck.Name), gctx)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[delete_object] %s", lom)
	}
	return
}
