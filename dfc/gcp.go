// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.  *
 */
package dfc

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

func getProjID() (string, string) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		return "", "Failed to get ProjectID from GCP"
	}
	return projectID, ""
}

func (cobj *gcpif) listbucket(w http.ResponseWriter, bucket string, msg *GetMsg) error {
	glog.Infof("listbucket %s", bucket)
	projid, errstr := getProjID()
	if projid == "" {
		return webinterror(w, errstr)
	}
	gctx := context.Background()
	client, err := storage.NewClient(gctx)
	if err != nil {
		glog.Fatal(err)
	}
	it := client.Bucket(bucket).Objects(gctx, nil)

	// var msg GetMsg
	var reslist = BucketList{Entries: make([]*BucketEntry, 0, 1000)}
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
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
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsbytes)
	return nil
}

// FIXME: revisit error processing
func (cobj *gcpif) getobj(w http.ResponseWriter, fqn string, bucket string, objname string) (file *os.File, err error) {
	projid, errstr := getProjID()
	if projid == "" {
		return nil, webinterror(w, errstr)
	}
	gctx := context.Background()
	client, err := storage.NewClient(gctx)
	if err != nil {
		errstr := fmt.Sprintf("Failed to initialize client, err: %v", err)
		return nil, webinterror(w, errstr)
	}
	o := client.Bucket(bucket).Object(objname)
	attrs, err := o.Attrs(gctx)
	if err != nil {
		errstr := fmt.Sprintf("Failed to get attributes for object %s from bucket %s, err: %v", objname, bucket, err)
		return nil, webinterror(w, errstr)
	}
	omd5 := hex.EncodeToString(attrs.MD5)
	rc, err := o.NewReader(gctx)
	if err != nil {
		errstr := fmt.Sprintf("Failed to create rc for object %s to file %q, err: %v", objname, fqn, err)
		return nil, webinterror(w, errstr)
	}
	defer rc.Close()
	if file, errstr = initobj(fqn); errstr != "" {
		return nil, webinterror(w, errstr)
	}
	size, errstr := getobjto_Md5(file, fqn, objname, omd5, rc)
	if errstr != "" {
		return nil, webinterror(w, errstr)
	}
	stats := getstorstats()
	stats.add("bytesloaded", size)
	return file, nil
}

func (cobj *gcpif) putobj(r *http.Request, w http.ResponseWriter, fqn, bucket, objname, md5sum string) error {
	size := r.ContentLength
	teebuf, b := maketeerw(r)
	projid, errstr := getProjID()
	if projid == "" {
		return webinterror(w, errstr)
	}
	gctx := context.Background()
	client, err := storage.NewClient(gctx)
	if err != nil {
		errstr := fmt.Sprintf("Failed to create client for bucket %s object %s , err: %v",
			bucket, objname, err)
		return webinterror(w, errstr)
	}

	wc := client.Bucket(bucket).Object(objname).NewWriter(gctx)
	defer wc.Close()
	_, err = copyBuffer(wc, teebuf)
	if err != nil {
		errstr := fmt.Sprintf("Failed to upload object %s into bucket %s , err: %v", objname, bucket, err)
		return webinterror(w, errstr)
	}
	r.Body = ioutil.NopCloser(b)
	written, err := ReceiveFile(fqn, r.Body, md5sum)
	if err != nil {
		glog.Errorf("Failed to write to file %s bytes written %v, err : %v", fqn, written, err)
		return err
	}
	if size > 0 {
		errstr := truncatefile(fqn, size)
		if errstr != "" {
			glog.Errorf(errstr)
			return errors.New(errstr)
		}
	}
	//TODO stats
	return nil
}
