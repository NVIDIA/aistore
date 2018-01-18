/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.  *
 */
package dfc

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
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
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			errstr := fmt.Sprintf("Failed to get bucket objects, err: %v", err)
			return webinterror(w, errstr)
		}
		fmt.Fprintln(w, attrs.Name)
	}
	return nil
}

// FIXME: revisit error processing
func (cobj *gcpif) getobj(w http.ResponseWriter, fqn string, bucket string,
	objname string) (file *os.File, err error) {

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
	file, err = createfile(fqn)
	if err != nil {
		errstr := fmt.Sprintf("Failed to create file %q, err: %v", fqn, err)
		return nil, webinterror(w, errstr)
	} else {
		glog.Infof("Created file %q", fqn)
	}
	// Calculate MD5 Hash
	hash := md5.New()
	writer := io.MultiWriter(file, hash)
	bytes, err := io.Copy(writer, rc)
	//bytes, err := copyBuffer(writer, rc)
	if err != nil {
		file.Close()
		errstr := fmt.Sprintf("Failed to download object %s to file %q, err: %v", objname, fqn, err)
		return nil, webinterror(w, errstr)
		// FIXME: checksetmounterror() - see aws.go
	}
	hashInBytes := hash.Sum(nil)[:16]
	fmd5 := hex.EncodeToString(hashInBytes)
	if omd5 != fmd5 {
		errstr := fmt.Sprintf("Object's %s MD5sum %v does not match with file(%s)'s MD5sum %v",
			objname, omd5, fqn, fmd5)
		// Remove downloaded file.
		err := os.Remove(fqn)
		if err != nil {
			glog.Errorf("Failed to delete file %s, err: %v", fqn, err)
		}
		return nil, webinterror(w, errstr)
	} else {
		glog.Infof("Object's %s MD5sum %v does MATCH with file(%s)'s MD5sum %v",
			objname, omd5, fqn, fmd5)
	}

	stats := getstorstats()
	stats.add("bytesloaded", bytes)
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
