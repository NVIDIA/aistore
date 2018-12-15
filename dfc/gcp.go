// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.  *
 */
package dfc

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/json-iterator/go"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	gcsURL = "http://storage.googleapis.com"

	gcpDfcHashType = "x-goog-meta-dfc-hash-type"
	gcpDfcHashVal  = "x-goog-meta-dfc-hash-val"

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

//======
//
// global - FIXME: environ
//
//======
func getProjID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func gcpErrorToHTTP(gcpError error) int {
	if gcperror, ok := gcpError.(*googleapi.Error); ok {
		return gcperror.Code
	}

	return http.StatusInternalServerError
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

func defaultClient(gctx context.Context) (*storage.Client, context.Context, string, string) {
	if glog.V(5) {
		glog.Info("Creating default google cloud session")
	}
	if getProjID() == "" {
		return nil, nil, "", "Failed to get ProjectID from GCP"
	}
	client, err := storage.NewClient(gctx)
	if err != nil {
		return nil, nil, "", fmt.Sprintf("Failed to create client, err: %v", err)
	}
	return client, gctx, getProjID(), ""
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
		return "", fmt.Errorf("Failed to create directory %s: %v", dir, err)
	}

	if err := ioutil.WriteFile(filePath, []byte(userCreds), 0755); err != nil {
		return "", fmt.Errorf("Failed to save to file: %v", err)
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

func createClient(ct context.Context) (*storage.Client, context.Context, string, string) {
	gctx := context.Background()
	userID := getStringFromContext(ct, ctxUserID)
	userCreds := userCredsFromContext(ct)
	credsDir := getStringFromContext(ct, ctxCredsDir)
	if userID == "" || userCreds == nil || credsDir == "" {
		return defaultClient(gctx)
	}

	creds, err := extractGCPCreds(userCreds)
	if err != nil || creds == nil {
		glog.Errorf("Failed to retrieve %s credentials %s: %v", cmn.ProviderGoogle, userID, err)
		return defaultClient(gctx)
	}

	filePath, err := saveCredentialsToFile(credsDir, userID, creds.creds)
	if err != nil {
		glog.Errorf("Failed to save credentials: %v", err)
		return defaultClient(gctx)
	}

	client, err := storage.NewClient(gctx, option.WithCredentialsFile(filePath))
	if err != nil {
		glog.Errorf("Failed to create storage client for %s: %v", userID, err)
		return defaultClient(gctx)
	}

	return client, gctx, creds.projectID, ""
}

//==================
//
// bucket operations
//
//==================
func (gcpimpl *gcpimpl) listbucket(ct context.Context, bucket string, msg *cmn.GetMsg) (jsbytes []byte, errstr string, errcode int) {
	if glog.V(4) {
		glog.Infof("listbucket %s", bucket)
	}
	gcpclient, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	var query *storage.Query
	var pageToken string

	if msg.GetPrefix != "" {
		query = &storage.Query{Prefix: msg.GetPrefix}
	}
	if msg.GetPageMarker != "" {
		pageToken = msg.GetPageMarker
	}

	it := gcpclient.Bucket(bucket).Objects(gctx, query)
	pageSize := gcpPageSize
	if msg.GetPageSize != 0 {
		pageSize = msg.GetPageSize
	}
	pager := iterator.NewPager(it, pageSize, pageToken)
	objs := make([]*storage.ObjectAttrs, 0)
	nextPageToken, err := pager.NextPage(&objs)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to list objects of bucket %s, err: %v", bucket, err)
	}

	var reslist = cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, initialBucketListSize)}
	reslist.PageMarker = nextPageToken
	for _, attrs := range objs {
		entry := &cmn.BucketEntry{}
		entry.Name = attrs.Name
		if strings.Contains(msg.GetProps, cmn.GetPropsSize) {
			entry.Size = attrs.Size
		}
		if strings.Contains(msg.GetProps, cmn.GetPropsBucket) {
			entry.Bucket = attrs.Bucket
		}
		if strings.Contains(msg.GetProps, cmn.GetPropsCtime) {
			t := attrs.Created
			if !attrs.Updated.IsZero() {
				t = attrs.Updated
			}
			switch msg.GetTimeFormat {
			case "":
				fallthrough
			case cmn.RFC822:
				entry.Ctime = t.Format(time.RFC822)
			default:
				entry.Ctime = t.Format(msg.GetTimeFormat)
			}
		}
		if strings.Contains(msg.GetProps, cmn.GetPropsChecksum) {
			entry.Checksum = hex.EncodeToString(attrs.MD5)
		}
		if strings.Contains(msg.GetProps, cmn.GetPropsVersion) {
			entry.Version = fmt.Sprintf("%d", attrs.Generation)
		}
		// TODO: other cmn.GetMsg props TBD

		reslist.Entries = append(reslist.Entries, entry)
	}

	if glog.V(4) {
		glog.Infof("listbucket count %d", len(reslist.Entries))
	}

	jsbytes, err = jsoniter.Marshal(reslist)
	cmn.Assert(err == nil, err)
	return
}

func (gcpimpl *gcpimpl) headbucket(ct context.Context, bucket string) (bucketprops cmn.SimpleKVs, errstr string, errcode int) {
	if glog.V(4) {
		glog.Infof("headbucket %s", bucket)
	}
	bucketprops = make(cmn.SimpleKVs)

	gcpclient, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	_, err := gcpclient.Bucket(bucket).Attrs(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to get attributes (bucket %s), err: %v", bucket, err)
		return
	}
	bucketprops[cmn.HeaderCloudProvider] = cmn.ProviderGoogle
	// GCP always generates a versionid for an object even if versioning is disabled.
	// So, return that we can detect versionid change on getobj etc
	bucketprops[cmn.HeaderVersioning] = cmn.VersionCloud
	return
}

func (gcpimpl *gcpimpl) getbucketnames(ct context.Context) (buckets []string, errstr string, errcode int) {
	gcpclient, gctx, projectID, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	buckets = make([]string, 0, 16)
	it := gcpclient.Buckets(gctx, projectID)
	for {
		battrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			errcode = gcpErrorToHTTP(err)
			errstr = fmt.Sprintf("Failed to list all buckets, err: %v", err)
			return
		}
		buckets = append(buckets, battrs.Name)
		if glog.V(4) {
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
func (gcpimpl *gcpimpl) headobject(ct context.Context, bucket string, objname string) (objmeta cmn.SimpleKVs, errstr string, errcode int) {
	if glog.V(4) {
		glog.Infof("headobject %s/%s", bucket, objname)
	}
	objmeta = make(cmn.SimpleKVs)

	gcpclient, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	attrs, err := gcpclient.Bucket(bucket).Object(objname).Attrs(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to retrieve %s/%s metadata, err: %v", bucket, objname, err)
		return
	}
	objmeta[cmn.HeaderCloudProvider] = cmn.ProviderGoogle
	objmeta["version"] = fmt.Sprintf("%d", attrs.Generation)
	return
}

//=======================
//
// object data operations
//
//=======================
func (gcpimpl *gcpimpl) getobj(ct context.Context, fqn string, bucket string, objname string) (props *cluster.LOM, errstr string, errcode int) {
	var v cmn.CksumValue
	gcpclient, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	o := gcpclient.Bucket(bucket).Object(objname)
	attrs, err := o.Attrs(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to retrieve %s/%s metadata, err: %v", bucket, objname, err)
		return
	}
	v = cmn.NewCksum(attrs.Metadata[gcpDfcHashType], attrs.Metadata[gcpDfcHashVal])
	md5 := hex.EncodeToString(attrs.MD5)
	rc, err := o.NewReader(gctx)
	if err != nil {
		errstr = fmt.Sprintf("The object %s/%s either %s or is not accessible, err: %v", bucket, objname, cmn.DoesNotExist, err)
		return
	}
	// hashtype and hash could be empty for legacy objects.
	props = &cluster.LOM{T: gcpimpl.t, Bucket: bucket, Objname: objname, Nhobj: v, Version: fmt.Sprintf("%d", attrs.Generation)}
	if _, props.Nhobj, props.Size, errstr = gcpimpl.t.receive(fqn, props, md5, rc); errstr != "" {
		rc.Close()
		return
	}
	if glog.V(4) {
		glog.Infof("GET %s/%s", bucket, objname)
	}
	rc.Close()
	return
}

func (gcpimpl *gcpimpl) putobj(ct context.Context, file *os.File, bucket, objname string, ohash cmn.CksumValue) (version string, errstr string, errcode int) {
	var (
		htype, hval string
		md          cmn.SimpleKVs
	)
	gcpclient, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	if ohash != nil {
		htype, hval = ohash.Get()
		md = make(cmn.SimpleKVs)
		md[gcpDfcHashType] = htype
		md[gcpDfcHashVal] = hval
	}
	gcpObj := gcpclient.Bucket(bucket).Object(objname)
	wc := gcpObj.NewWriter(gctx)
	wc.Metadata = md
	buf, slab := gmem2.AllocFromSlab2(0)
	written, err := io.CopyBuffer(wc, file, buf)
	slab.Free(buf)
	if err != nil {
		errstr = fmt.Sprintf("PUT %s/%s: failed to copy, err: %v", bucket, objname, err)
		return
	}
	if err := wc.Close(); err != nil {
		errstr = fmt.Sprintf("PUT %s/%s: failed to close wc, err: %v", bucket, objname, err)
		return
	}
	attr, err := gcpObj.Attrs(gctx)
	if err != nil {
		errstr = fmt.Sprintf("PUT %s/%s: failed to read updated object attributes, err: %v", bucket, objname, err)
		return
	}
	version = fmt.Sprintf("%d", attr.Generation)
	if glog.V(4) {
		glog.Infof("PUT %s/%s, size %d, version %s", bucket, objname, written, version)
	}
	return
}

func (gcpimpl *gcpimpl) deleteobj(ct context.Context, bucket, objname string) (errstr string, errcode int) {
	gcpclient, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	o := gcpclient.Bucket(bucket).Object(objname)
	err := o.Delete(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to DELETE %s/%s, err: %v", bucket, objname, err)
		return
	}
	if glog.V(4) {
		glog.Infof("DELETE %s/%s", bucket, objname)
	}
	return
}
