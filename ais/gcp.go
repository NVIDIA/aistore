// +build gcp

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	gcpChecksumType = "x-goog-meta-ais-cksum-type"
	gcpChecksumVal  = "x-goog-meta-ais-cksum-val"

	gcpPageSize = 1000
)

// To get projectID from gcp auth json file, to get rid of reading projectID
// from environment variable
type gcpAuthRec struct {
	ProjectID string `json:"project_id"`
}

//======
//
// implements cloudif
//
//======
type (
	gcpCreds struct {
		projectID string
		creds     string
	}

	gcpimpl struct {
		t *targetrunner
	}
)

var (
	_ cloudif = &gcpimpl{}
)

//======
//
// global - FIXME: environ
//
//======
func newGCPProvider(t *targetrunner) *gcpimpl { return &gcpimpl{t} }

func getProjID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

// If extractGCPCreds returns no error and gcpCreds is nil then the default
//   GCP client is used (that loads credentials from dir ~/.config/gcloud/ -
//   the directory is created after the first successful login with gsutil)
func extractGCPCreds(credsList map[string]string) (*gcpCreds, error) {
	if len(credsList) == 0 {
		return nil, nil
	}
	raw, ok := credsList[cmn.ProviderGoogle]
	if raw == "" || !ok {
		return nil, nil
	}
	rec := &gcpAuthRec{}
	if err := jsoniter.Unmarshal([]byte(raw), rec); err != nil {
		return nil, err
	}

	return &gcpCreds{rec.ProjectID, raw}, nil
}

func defaultClient(gctx context.Context) (*storage.Client, context.Context, string, error) {
	if glog.V(5) {
		glog.Info("Creating default google cloud session")
	}
	if getProjID() == "" {
		return nil, nil, "", errors.New("failed to get ProjectID from GCP")
	}
	client, err := storage.NewClient(gctx)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to create client, err: %v", err)
	}
	return client, gctx, getProjID(), nil
}

func saveCredentialsToFile(baseDir, userID, userCreds string) (string, error) {
	dir := filepath.Join(baseDir, cmn.ProviderGoogle)
	filePath := filepath.Join(dir, userID+".json")

	if _, err := os.Stat(filePath); err == nil {
		// credentials already saved, no need to do anything
		// TODO: keep the list of stored creds in-memory instead of calling os functions
		return "", nil
	}

	if err := cmn.CreateDir(dir); err != nil {
		return "", fmt.Errorf("failed to create directory %s: %v", dir, err)
	}

	if err := ioutil.WriteFile(filePath, []byte(userCreds), 0755); err != nil {
		return "", fmt.Errorf("failed to save to file: %v", err)
	}

	return filePath, nil
}

// createClient support two ways of creating a connection to cloud:
// 1. With Authn server disabled (old way):
//    In this case all are read from environment variables and a user
//    should be logged in to the cloud
// 2. If Authn is enabled and directory with user credentials is set:
//    The directory contains credentials for every user who want to connect
//    storage. A file per a user.  A userID is retrieved from a token - it the
//    file name with the user's credentials. Full path to user credentials is
//    CredDir + userID + ".json"
//    The file is standard GCP credentials file (e.g, check ~/gcp_creds.json
//    for details). If the file does not include project_id, the function reads
//    it from environment variable GOOGLE_CLOUD_PROJECT
// The function returns:
//   connection to the cloud, GCP context, project_id, error_string
// project_id is used only by getbucketnames function

func createClient(ctx context.Context) (*storage.Client, context.Context, string, error) {
	userID := getStringFromContext(ctx, ctxUserID)
	userCreds := userCredsFromContext(ctx)
	credsDir := getStringFromContext(ctx, ctxCredsDir)
	if userID == "" || userCreds == nil || credsDir == "" {
		return defaultClient(ctx)
	}

	creds, err := extractGCPCreds(userCreds)
	if err != nil || creds == nil {
		glog.Errorf("Failed to retrieve %s credentials %s: %v", cmn.ProviderGoogle, userID, err)
		return defaultClient(ctx)
	}

	filePath, err := saveCredentialsToFile(credsDir, userID, creds.creds)
	if err != nil {
		glog.Errorf("Failed to save credentials: %v", err)
		return defaultClient(ctx)
	}

	client, err := storage.NewClient(ctx, option.WithCredentialsFile(filePath))
	if err != nil {
		glog.Errorf("Failed to create storage client for %s: %v", userID, err)
		return defaultClient(ctx)
	}

	return client, ctx, creds.projectID, nil
}

func gcpErrorToAISError(bucket string, gcpError error) (error, int) {
	if gcpError == storage.ErrBucketNotExist {
		return cmn.NewErrorCloudBucketDoesNotExist(bucket), http.StatusNotFound
	}

	return gcpError, http.StatusBadRequest
}

func handleObjectError(objErr error, bckName string, bucket *storage.BucketHandle, gctx context.Context) (error, int) {
	if objErr != storage.ErrObjectNotExist {
		return objErr, http.StatusBadRequest
	}

	// Object does not exist, but in gcp it doesn't mean that the bucket existed. Check if the buckets exists
	_, err := bucket.Attrs(gctx)
	if err != nil {
		return gcpErrorToAISError(bckName, err)
	}

	return objErr, http.StatusBadRequest
}

//==================
//
// bucket operations
//
//==================
func (gcpimpl *gcpimpl) ListBucket(ctx context.Context, bucket string, msg *cmn.SelectMsg) (reslist *cmn.BucketList, err error, errcode int) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("listbucket %s", bucket)
	}
	gcpclient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}
	var query *storage.Query
	var pageToken string

	if msg.Prefix != "" {
		query = &storage.Query{Prefix: msg.Prefix}
	}
	if msg.PageMarker != "" {
		pageToken = msg.PageMarker
	}

	it := gcpclient.Bucket(bucket).Objects(gctx, query)
	pageSize := gcpPageSize
	if msg.PageSize != 0 {
		pageSize = msg.PageSize
	}
	pager := iterator.NewPager(it, pageSize, pageToken)
	objs := make([]*storage.ObjectAttrs, 0)
	nextPageToken, err := pager.NextPage(&objs)
	if err != nil {
		err, errcode = gcpErrorToAISError(bucket, err)
		return
	}

	reslist = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, InitialBucketListSize)}
	reslist.PageMarker = nextPageToken
	for _, attrs := range objs {
		entry := &cmn.BucketEntry{}
		entry.Name = attrs.Name
		if strings.Contains(msg.Props, cmn.GetPropsSize) {
			entry.Size = attrs.Size
		}
		if strings.Contains(msg.Props, cmn.GetPropsChecksum) {
			entry.Checksum = hex.EncodeToString(attrs.MD5)
		}
		if strings.Contains(msg.Props, cmn.GetPropsVersion) {
			entry.Version = fmt.Sprintf("%d", attrs.Generation)
		}
		// TODO: other cmn.SelectMsg props TBD

		reslist.Entries = append(reslist.Entries, entry)
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("listbucket count %d", len(reslist.Entries))
	}

	return
}

func (gcpimpl *gcpimpl) headbucket(ctx context.Context, bucket string) (bucketprops cmn.SimpleKVs, err error, errcode int) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("headbucket %s", bucket)
	}
	bucketprops = make(cmn.SimpleKVs)

	gcpclient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}
	_, err = gcpclient.Bucket(bucket).Attrs(gctx)
	if err != nil {
		err, errcode = gcpErrorToAISError(bucket, err)
		return
	}
	bucketprops[cmn.HeaderCloudProvider] = cmn.ProviderGoogle
	// GCP always generates a versionid for an object even if versioning is disabled.
	// So, return that we can detect versionid change on getobj etc
	bucketprops[cmn.HeaderBucketVerEnabled] = "true"
	return
}

func (gcpimpl *gcpimpl) getbucketnames(ctx context.Context) (buckets []string, err error, errcode int) {
	gcpclient, gctx, projectID, err := createClient(ctx)
	if err != nil {
		return
	}
	buckets = make([]string, 0, 16)
	it := gcpclient.Buckets(gctx, projectID)
	for {
		var battrs *storage.BucketAttrs

		battrs, err = it.Next()
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			err, errcode = gcpErrorToAISError(battrs.Name, err)
			return
		}
		buckets = append(buckets, battrs.Name)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: created %v, versioning %t", battrs.Name, battrs.Created, battrs.VersioningEnabled)
		}
	}
	return
}

//============
//
// object meta
//
//============
func (gcpimpl *gcpimpl) headobject(ctx context.Context, lom *cluster.LOM) (objmeta cmn.SimpleKVs, err error, errcode int) {
	objmeta = make(cmn.SimpleKVs)

	gcpclient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}
	attrs, err := gcpclient.Bucket(lom.Bucket).Object(lom.Objname).Attrs(gctx)
	if err != nil {
		err, errcode = handleObjectError(err, lom.Bucket, gcpclient.Bucket(lom.Bucket), gctx)
		return
	}
	objmeta[cmn.HeaderCloudProvider] = cmn.ProviderGoogle
	objmeta[cmn.HeaderObjVersion] = fmt.Sprintf("%d", attrs.Generation)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("HEAD %s", lom)
	}
	return
}

//=======================
//
// object data operations
//
//=======================
func (gcpimpl *gcpimpl) getobj(ctx context.Context, workFQN string, lom *cluster.LOM) (err error, errcode int) {
	gcpclient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}
	o := gcpclient.Bucket(lom.Bucket).Object(lom.Objname)
	attrs, err := o.Attrs(gctx)
	if err != nil {
		err, errcode = handleObjectError(err, lom.Bucket, gcpclient.Bucket(lom.Bucket), gctx)
		return
	}

	cksum := cmn.NewCksum(attrs.Metadata[gcpChecksumType], attrs.Metadata[gcpChecksumVal])
	cksumToCheck := cmn.NewCksum(cmn.ChecksumMD5, hex.EncodeToString(attrs.MD5))

	rc, err := o.NewReader(gctx)
	if err != nil {
		return
	}
	// hashtype and hash could be empty for legacy objects.
	bckProvider, _ := cmn.BckProviderFromStr(cmn.CloudBs)
	lom, errstr := cluster.LOM{T: gcpimpl.t, Bucket: lom.Bucket, Objname: lom.Objname, BucketProvider: bckProvider}.Init()
	if errstr != "" {
		err = errors.New(errstr)
		return
	}
	lom.SetCksum(cksum)
	lom.SetVersion(strconv.FormatInt(attrs.Generation, 10))
	poi := &putObjInfo{
		t:            gcpimpl.t,
		cold:         true,
		r:            rc,
		cksumToCheck: cksumToCheck,
		lom:          lom,
		workFQN:      workFQN,
	}

	if err = poi.writeToFile(); err != nil {
		errstr = err.Error()
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("GET %s", lom)
	}
	return
}

func (gcpimpl *gcpimpl) putobj(ctx context.Context, file *os.File, lom *cluster.LOM) (version string, err error, errcode int) {
	gcpclient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}

	md := make(cmn.SimpleKVs)
	md[gcpChecksumType], md[gcpChecksumVal] = lom.Cksum().Get()

	gcpObj := gcpclient.Bucket(lom.Bucket).Object(lom.Objname)
	wc := gcpObj.NewWriter(gctx)
	wc.Metadata = md
	buf, slab := gmem2.AllocEstimated(0)
	written, err := io.CopyBuffer(wc, file, buf)
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
		err, errcode = handleObjectError(err, lom.Bucket, gcpclient.Bucket(lom.Bucket), gctx)
		return
	}
	version = fmt.Sprintf("%d", attr.Generation)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("PUT %s, size %d, version %s", lom, written, version)
	}
	return
}

func (gcpimpl *gcpimpl) deleteobj(ctx context.Context, lom *cluster.LOM) (err error, errcode int) {
	gcpclient, gctx, _, err := createClient(ctx)
	if err != nil {
		return
	}
	o := gcpclient.Bucket(lom.Bucket).Object(lom.Objname)
	err = o.Delete(gctx)
	if err != nil {
		err, errcode = handleObjectError(err, lom.Bucket, gcpclient.Bucket(lom.Bucket), gctx)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("DELETE %s", lom)
	}
	return
}
