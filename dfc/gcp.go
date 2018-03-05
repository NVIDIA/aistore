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
	glog.Infof("gcp listbucket %s", bucket)
	client, gctx, errstr := createclient()
	if errstr != "" {
		return
	}

	var query *storage.Query

	if msg.GetPrefix != "" {
		query = &storage.Query{Prefix: msg.GetPrefix}
	}
	it := client.Bucket(bucket).Objects(gctx, query)

	var reslist = BucketList{Entries: make([]*BucketEntry, 0, 1000)}
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			errcode = gcpErrorToHTTP(err)
			errstr = fmt.Sprintf("gcp: Failed to list objects of bucket %s, err: %v", bucket, err)
		}
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
		// TODO: other GetMsg props TBD

		reslist.Entries = append(reslist.Entries, entry)
	}
	if glog.V(3) {
		glog.Infof("listbucket count %d", len(reslist.Entries))
	}
	jsbytes, err := json.Marshal(reslist)
	assert(err == nil, err)
	return
}

func (gcpimpl *gcpimpl) getobj(fqn string, bucket string, objname string) (nhobj cksumvalue, size int64, errstr string, errcode int) {
	var v cksumvalue
	client, gctx, errstr := createclient()
	if errstr != "" {
		return
	}
	o := client.Bucket(bucket).Object(objname)
	attrs, err := o.Attrs(gctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("gcp: Failed to get attributes (object %s, bucket %s), err: %v", objname, bucket, err)
		return
	}
	v = newcksumvalue(attrs.Metadata[gcpDfcHashType], attrs.Metadata[gcpDfcHashVal])
	md5 := hex.EncodeToString(attrs.MD5)
	rc, err := o.NewReader(gctx)
	if err != nil {
		errstr = fmt.Sprintf("gcp: Failed to create rc (object %s, bucket %s), err: %v", objname, bucket, err)
		return
	}
	defer rc.Close()
	// hashtype and hash could be empty for legacy objects.
	if nhobj, size, errstr = gcpimpl.t.receiveFileAndFinalize(fqn, objname, md5, v, rc); errstr != "" {
		return
	}
	if glog.V(3) {
		glog.Infof("gcp: GET %s (bucket %s)", objname, bucket)
	}
	return
}

func (gcpimpl *gcpimpl) putobj(file *os.File, bucket, objname string, ohash cksumvalue) (errstr string, errcode int) {
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
	wc := client.Bucket(bucket).Object(objname).NewWriter(gctx)
	wc.Metadata = md

	buf := gcpimpl.t.buffers.alloc()
	defer gcpimpl.t.buffers.free(buf)
	written, err := io.CopyBuffer(wc, file, buf)
	if err != nil {
		errstr = fmt.Sprintf("gcp: Failed to copy-buffer (object %s, bucket %s), err: %v", objname, bucket, err)
		return
	}
	if err := wc.Close(); err != nil {
		errstr = fmt.Sprintf("gcp: Unexpected failure to close wc upon uploading %s (bucket %s), err: %v",
			objname, bucket, err)
		return
	}
	if glog.V(3) {
		glog.Infof("gcp: PUT %s (bucket %s, size %d) ", objname, bucket, written)
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
		errstr = fmt.Sprintf("gcp: Failed to delete %s (bucket %s), err: %v", objname, bucket, err)
		return
	}
	if glog.V(3) {
		glog.Infof("gcp: deleted %s (bucket %s)", objname, bucket)
	}
	return
}
