// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.  *
 */
package dfc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

const (
	gcpDfcHashType = "x-goog-meta-dfc-hash-type"
	gcpDfcHashVal  = "x-goog-meta-dfc-hash-val"

	gcpPageSize = 1000
)

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

func createclient() (*storage.Client, context.Context, string) {
	if getProjID() == "" {
		return nil, nil, "Failed to get ProjectID from GCP"
	}
	gctx := context.Background()
	client, err := storage.NewClient(gctx)
	if err != nil {
		return nil, nil, fmt.Sprintf("Failed to create client, err: %v", err)
	}
	return client, gctx, ""
}

//======
//
// methods
//
//======
func (gcpimpl *gcpimpl) listbucket(bucket string, msg *GetMsg) (jsbytes []byte, errstr string, errcode int) {
	glog.Infof("gcp: listbucket %s", bucket)
	client, gctx, errstr := createclient()
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
		errstr = fmt.Sprintf("gcp: Failed to list objects of bucket %s, err: %v", bucket, err)
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
	if glog.V(3) {
		glog.Infof("listbucket count %d", len(reslist.Entries))
	}
	jsbytes, err = json.Marshal(reslist)
	assert(err == nil, err)
	return
}

func (gcpimpl *gcpimpl) headbucket(bucket string) (bucketprops map[string]string, errstr string, errcode int) {
	glog.Infof("gcp: headbucket %s", bucket)
	bucketprops = make(map[string]string)

	client, gctx, errstr := createclient()
	if errstr != "" {
		return
	}
	_, err := client.Bucket(bucket).Attrs(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("gcp: Failed to get attributes (bucket %s), err: %v", bucket, err)
		return
	}
	bucketprops[CloudProvider] = ProviderGoogle
	// GCP always generates a versionid for an object even if versioning is disabled.
	// So, return that we can detect versionid change on getobj etc
	bucketprops[Versioning] = VersionCloud
	return
}

func (gcpimpl *gcpimpl) headobject(bucket string, objname string) (objmeta map[string]string, errstr string, errcode int) {
	glog.Infof("gcp: headobject %s/%s", bucket, objname)
	objmeta = make(map[string]string)

	client, gctx, errstr := createclient()
	if errstr != "" {
		return
	}
	attrs, err := client.Bucket(bucket).Object(objname).Attrs(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("gcp: Failed to retrieve %s/%s metadata, err: %v", bucket, objname, err)
		return
	}
	objmeta[CloudProvider] = ProviderGoogle
	objmeta["version"] = fmt.Sprintf("%d", attrs.Generation)
	return
}

func (gcpimpl *gcpimpl) getobj(fqn string, bucket string, objname string) (props *objectProps, errstr string, errcode int) {
	var v cksumvalue
	client, gctx, errstr := createclient()
	if errstr != "" {
		return
	}
	o := client.Bucket(bucket).Object(objname)
	attrs, err := o.Attrs(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("gcp: Failed to retrieve %s/%s metadata, err: %v", bucket, objname, err)
		return
	}
	v = newcksumvalue(attrs.Metadata[gcpDfcHashType], attrs.Metadata[gcpDfcHashVal])
	md5 := hex.EncodeToString(attrs.MD5)
	rc, err := o.NewReader(gctx)
	if err != nil {
		errstr = fmt.Sprintf("gcp: The object %s/%s either does not exist or is not accessible, err: %v", bucket, objname, err)
		return
	}
	defer rc.Close()
	// hashtype and hash could be empty for legacy objects.
	props = &objectProps{version: fmt.Sprintf("%d", attrs.Generation)}
	if _, props.nhobj, props.size, errstr = gcpimpl.t.receive(fqn, false, objname, md5, v, rc); errstr != "" {
		return
	}
	if glog.V(4) {
		glog.Infof("gcp: GET %s/%s", bucket, objname)
	}
	return
}

func (gcpimpl *gcpimpl) putobj(file *os.File, bucket, objname string, ohash cksumvalue) (version string, errstr string, errcode int) {
	var (
		htype, hval string
		md          map[string]string
	)
	client, gctx, errstr := createclient()
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
		errstr = fmt.Sprintf("gcp: PUT %s/%s: failed to copy, err: %v", bucket, objname, err)
		return
	}
	if err := wc.Close(); err != nil {
		errstr = fmt.Sprintf("gcp: PUT %s/%s: failed to close wc, err: %v", bucket, objname, err)
		return
	}
	attr, err := gcpObj.Attrs(gctx)
	if err != nil {
		errstr = fmt.Sprintf("gcp: PUT %s/%s: failed to read updated object attributes, err: %v", bucket, objname, err)
		return
	}
	version = fmt.Sprintf("%d", attr.Generation)
	if glog.V(4) {
		glog.Infof("gcp: PUT %s/%s, size %d, version %s", bucket, objname, written, version)
	}
	return
}

func (gcpimpl *gcpimpl) deleteobj(bucket, objname string) (errstr string, errcode int) {
	client, gctx, errstr := createclient()
	if errstr != "" {
		return
	}
	o := client.Bucket(bucket).Object(objname)
	err := o.Delete(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("gcp: Failed to DELETE %s/%s, err: %v", bucket, objname, err)
		return
	}
	if glog.V(4) {
		glog.Infof("gcp: DELETE %s/%s", bucket, objname)
	}
	return
}
