/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

func getProjID() string {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		glog.Errorf("Failed to get ProjectID from GCP")
	}
	return projectID
}

func (obj *gcpif) listbucket(w http.ResponseWriter, bucket string) {
	glog.Infof(" listbucket : bucket = %s ", bucket)
	projid := getProjID()
	if projid == "" {
		webinterror(w, "Failed to get Project ID from GCP ")
		return
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		glog.Fatal(err)
	}
	it := client.Bucket(bucket).Objects(ctx, nil)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			errstr := fmt.Sprintf("Failed to get bucket objects, err : %s", err)
			webinterror(w, errstr)
			return
		}
		fmt.Fprintln(w, attrs.Name)
	}
	return
}

func (obj *gcpif) getobj(w http.ResponseWriter, mpath string, bktname string, objname string) {
	fname := mpath + fslash + bktname + fslash + objname

	projid := getProjID()
	if projid == "" {
		glog.Errorf("Failed to get Project ID from GCP ")
		return
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		glog.Fatal(err)
	}
	rc, err := client.Bucket(bktname).Object(objname).NewReader(ctx)
	if err != nil {
		errstr := fmt.Sprintf("Failed to create rc for object %s to filename %s , err : %s", objname, fname, err)
		webinterror(w, errstr)
		return
	}
	defer rc.Close()
	// strips the last part from filepath
	dirname := filepath.Dir(fname)
	_, err = os.Stat(dirname)
	if err != nil {
		// Create bucket-path directory for non existent paths.
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirname, 0755)
			if err != nil {
				errstr := fmt.Sprintf("Failed to create bucket dir %s, err: %s", dirname, err)
				webinterror(w, errstr)
				return
			}
		} else {
			errstr := fmt.Sprintf("Failed to fstat dir %q, err: %s", dirname, err)
			webinterror(w, errstr)
			return
		}
	}
	file, err := os.Create(fname)
	if err != nil {
		errstr := fmt.Sprintf("Failed to create file %s, err:%s", fname, err)
		webinterror(w, errstr)
		return
	} else {
		glog.Infof("Created file %s succesfully ", fname)
	}
	_, err = io.Copy(file, rc)
	if err != nil {
		errstr := fmt.Sprintf("Failed to download object %s to filename %s , err : %s", objname, fname, err)
		webinterror(w, errstr)
		return
	}
	return
}

func webinterror(w http.ResponseWriter, errstr string) {
	glog.Error(errstr)
	err := errors.New(errstr)
	http.Error(w, err.Error(), http.StatusInternalServerError)
	stats := getstorstats()
	atomic.AddInt64(&stats.numerr, 1)
}
