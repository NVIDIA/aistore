/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.  *
 */
package dfc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

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

func (cobj *gcpif) listbucket(w http.ResponseWriter, bucket string) error {
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

func (obj *gcpif) putobj(r *http.Request, w http.ResponseWriter,
	bucket string, kname string) error {

	projid, errstr := getProjID()
	if projid == "" {
		return webinterror(w, errstr)
	}
	gctx := context.Background()
	client, err := storage.NewClient(gctx)
	if err != nil {
		errstr := fmt.Sprintf("Failed to create client for bucket %s object %s , err: %v",
			bucket, kname, err)
		return webinterror(w, errstr)
	}

	wc := client.Bucket(bucket).Object(kname).NewWriter(gctx)
	defer wc.Close()
	//_, err = io.Copy(wc, r.Body)
	_, err = copyBuffer(wc, r.Body)
	if err != nil {
		errstr := fmt.Sprintf("Failed to upload object %s into bucket %s , err: %v",
			kname, bucket, err)
		return webinterror(w, errstr)
	}
	//TODO stats
	return nil
}
