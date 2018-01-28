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
	client, gctx, err := createclient()
	if err != nil {
		return err
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

// Initialize and create storage client
func createclient() (*storage.Client, context.Context, error) {
	projid, errstr := getProjID()
	if projid == "" {
		return nil, nil, errors.New(errstr)
	}
	gctx := context.Background()
	client, err := storage.NewClient(gctx)
	if err != nil {
		errstr := fmt.Sprintf("Failed to create client, err: %v", err)
		return nil, nil, errors.New(errstr)
	}
	return client, gctx, nil
}

// FIXME: revisit error processing
func (cobj *gcpif) getobj(fqn string, bucket string, objname string) (file *os.File, err error) {
	var errstr string
	client, gctx, err := createclient()
	if err != nil {
		return nil, err
	}
	o := client.Bucket(bucket).Object(objname)
	attrs, err := o.Attrs(gctx)
	if err != nil {
		errstr = fmt.Sprintf(
			"Failed to get attributes for object %s from bucket %s, err: %v",
			objname, bucket, err)
		return nil, errors.New(errstr)
	}
	omd5 := hex.EncodeToString(attrs.MD5)
	rc, err := o.NewReader(gctx)
	if err != nil {
		errstr = fmt.Sprintf(
			"Failed to create rc for object %s to file %s, err: %v",
			objname, fqn, err)
		return nil, errors.New(errstr)
	}
	defer rc.Close()
	if file, errstr = initobj(fqn); errstr != "" {
		return nil, errors.New(errstr)
	}
	size, errstr := getobjto_Md5(file, fqn, objname, omd5, rc)
	if errstr != "" {
		file.Close()
		return nil, errors.New(errstr)
	}
	stats := getstorstats()
	stats.add("bytesloaded", size)
	return file, nil
}

func (cobj *gcpif) putobj(r *http.Request, fqn, bucket, objname, md5sum string) error {
	if glog.V(3) {
		glog.Infof("Put bucket %s object %s", bucket, objname)
	}
	size := r.ContentLength
	teebuf, b := maketeerw(r)
	client, gctx, err := createclient()
	if err != nil {
		return err
	}
	wc := client.Bucket(bucket).Object(objname).NewWriter(gctx)
	defer wc.Close()
	_, err = copyBuffer(wc, teebuf)
	if err != nil {
		errstr := fmt.Sprintf(
			"Failed to upload object %s into bucket %s , err: %v",
			objname, bucket, err)
		return errors.New(errstr)
	}
	r.Body = ioutil.NopCloser(b)
	written, err := ReceiveFile(fqn, r.Body, md5sum)
	if err != nil {
		errstr := fmt.Sprintf("Failed to write to file %s bytes written %v, err : %v",
			fqn, written, err)
		return errors.New(errstr)
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

func (cobj *gcpif) deleteobj(bucket, objname string) error {
	if glog.V(3) {
		glog.Infof("Delete bucket %s object %s", bucket, objname)
	}
	client, gctx, err := createclient()
	if err != nil {
		return err
	}
	o := client.Bucket(bucket).Object(objname)
	err = o.Delete(gctx)
	if err != nil {
		errstr := fmt.Sprintf("Failed to delete object %s from bucket %s, err: %v",
			objname, bucket, err)
		return errors.New(errstr)
	}
	//TODO stats
	return nil
}
