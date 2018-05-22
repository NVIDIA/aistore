// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.  *
 */
package dfc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
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
type gcpimpl struct {
	t *targetrunner
}

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

func defaultClient(gctx context.Context) (*storage.Client, context.Context, string, string) {
	if getProjID() == "" {
		return nil, nil, "", "Failed to get ProjectID from GCP"
	}
	client, err := storage.NewClient(gctx)
	if err != nil {
		return nil, nil, "", fmt.Sprintf("Failed to create client, err: %v", err)
	}
	return client, gctx, getProjID(), ""
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
	userID := userIDFromContext(ct)
	if userID == "" {
		return defaultClient(gctx)
	}

	credPath, err := userCredsPathFromContext(ct, userID)
	if err != nil {
		glog.Errorf("Failed to read user credentials: %v", err)
		return defaultClient(gctx)
	}

	// read credentials file to retreive projectID
	bytes, err := ioutil.ReadFile(credPath)
	gcprec := &gcpAuthRec{}
	if err != nil {
		glog.Errorf("Failed to read credentials file: %v", err)
		return nil, nil, "", "Failed to read credentials file"
	}
	err = json.Unmarshal(bytes, gcprec)
	if err != nil {
		glog.Errorf("Failed to unmarshal credentials file: %v", err)
		return nil, nil, "", "Invalid credentials file format"
	}

	// create a new session
	client, err := storage.NewClient(gctx, option.WithCredentialsFile(credPath))
	if err != nil {
		return nil, nil, "", fmt.Sprintf("Failed to create client, err: %v", err)
	}

	return client, gctx, gcprec.ProjectID, ""
}

//==================
//
// bucket operations
//
//==================
func (gcpimpl *gcpimpl) listbucket(ct context.Context, bucket string, msg *GetMsg) (jsbytes []byte, errstr string, errcode int) {
	if glog.V(4) {
		glog.Infof("listbucket %s", bucket)
	}
	client, gctx, _, errstr := createClient(ct)
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

	it := client.Bucket(bucket).Objects(gctx, query)
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

	var reslist = BucketList{Entries: make([]*BucketEntry, 0, initialBucketListSize)}
	reslist.PageMarker = nextPageToken
	for _, attrs := range objs {
		entry := &BucketEntry{}
		entry.Name = attrs.Name
		if strings.Contains(msg.GetProps, GetPropsSize) {
			entry.Size = attrs.Size
		}
		if strings.Contains(msg.GetProps, GetPropsBucket) {
			entry.Bucket = attrs.Bucket
		}
		if strings.Contains(msg.GetProps, GetPropsCtime) {
			t := attrs.Created
			if !attrs.Updated.IsZero() {
				t = attrs.Updated
			}
			switch msg.GetTimeFormat {
			case "":
				fallthrough
			case RFC822:
				entry.Ctime = t.Format(time.RFC822)
			default:
				entry.Ctime = t.Format(msg.GetTimeFormat)
			}
		}
		if strings.Contains(msg.GetProps, GetPropsChecksum) {
			entry.Checksum = hex.EncodeToString(attrs.MD5)
		}
		if strings.Contains(msg.GetProps, GetPropsVersion) {
			entry.Version = fmt.Sprintf("%d", attrs.Generation)
		}
		// TODO: other GetMsg props TBD

		reslist.Entries = append(reslist.Entries, entry)
	}

	if glog.V(4) {
		glog.Infof("listbucket count %d", len(reslist.Entries))
	}

	jsbytes, err = json.Marshal(reslist)
	assert(err == nil, err)
	return
}

func (gcpimpl *gcpimpl) headbucket(ct context.Context, bucket string) (bucketprops map[string]string, errstr string, errcode int) {
	if glog.V(4) {
		glog.Infof("headbucket %s", bucket)
	}
	bucketprops = make(map[string]string)

	client, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	_, err := client.Bucket(bucket).Attrs(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to get attributes (bucket %s), err: %v", bucket, err)
		return
	}
	bucketprops[CloudProvider] = ProviderGoogle
	// GCP always generates a versionid for an object even if versioning is disabled.
	// So, return that we can detect versionid change on getobj etc
	bucketprops[Versioning] = VersionCloud
	return
}

func (gcpimpl *gcpimpl) getbucketnames(ct context.Context) (buckets []string, errstr string, errcode int) {
	client, gctx, projectID, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	buckets = make([]string, 0, 16)
	it := client.Buckets(gctx, projectID)
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
func (gcpimpl *gcpimpl) headobject(ct context.Context, bucket string, objname string) (objmeta map[string]string, errstr string, errcode int) {
	if glog.V(4) {
		glog.Infof("headobject %s/%s", bucket, objname)
	}
	objmeta = make(map[string]string)

	client, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	attrs, err := client.Bucket(bucket).Object(objname).Attrs(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to retrieve %s/%s metadata, err: %v", bucket, objname, err)
		return
	}
	objmeta[CloudProvider] = ProviderGoogle
	objmeta["version"] = fmt.Sprintf("%d", attrs.Generation)
	return
}

//=======================
//
// object data operations
//
//=======================
func (gcpimpl *gcpimpl) getobj(ct context.Context, fqn string, bucket string, objname string) (props *objectProps, errstr string, errcode int) {
	var v cksumvalue
	client, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	o := client.Bucket(bucket).Object(objname)
	attrs, err := o.Attrs(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to retrieve %s/%s metadata, err: %v", bucket, objname, err)
		return
	}
	v = newcksumvalue(attrs.Metadata[gcpDfcHashType], attrs.Metadata[gcpDfcHashVal])
	md5 := hex.EncodeToString(attrs.MD5)
	rc, err := o.NewReader(gctx)
	if err != nil {
		errstr = fmt.Sprintf("The object %s/%s either does not exist or is not accessible, err: %v", bucket, objname, err)
		return
	}
	defer rc.Close()
	// hashtype and hash could be empty for legacy objects.
	props = &objectProps{version: fmt.Sprintf("%d", attrs.Generation)}
	if _, props.nhobj, props.size, errstr = gcpimpl.t.receive(fqn, false, objname, md5, v, rc); errstr != "" {
		return
	}
	if glog.V(4) {
		glog.Infof("GET %s/%s", bucket, objname)
	}
	return
}

func (gcpimpl *gcpimpl) putobj(ct context.Context, file *os.File, bucket, objname string, ohash cksumvalue) (version string, errstr string, errcode int) {
	var (
		htype, hval string
		md          map[string]string
	)
	client, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	if ohash != nil {
		htype, hval = ohash.get()
		md = make(map[string]string)
		md[gcpDfcHashType] = htype
		md[gcpDfcHashVal] = hval
	}
	gcpObj := client.Bucket(bucket).Object(objname)
	wc := gcpObj.NewWriter(gctx)
	wc.Metadata = md
	slab := selectslab(0)
	buf := slab.alloc()
	defer slab.free(buf)
	written, err := io.CopyBuffer(wc, file, buf)
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
	client, gctx, _, errstr := createClient(ct)
	if errstr != "" {
		return
	}
	o := client.Bucket(bucket).Object(objname)
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
