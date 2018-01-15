/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

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

func (obj *gcpif) listbucket(w http.ResponseWriter, bucket string) error {
	glog.Infof("listbucket %s", bucket)
	projid, errstr := getProjID()
	if projid == "" {
		return webinterror(w, errstr)
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
			errstr := fmt.Sprintf("Failed to get bucket objects, err: %v", err)
			return webinterror(w, errstr)
		}
		fmt.Fprintln(w, attrs.Name)
	}
	return nil
}

// FIXME: revisit error processing
func (obj *gcpif) getobj(w http.ResponseWriter, mpath string, bktname string, objname string) error {
	fname := mpath + "/" + bktname + "/" + objname

	projid, errstr := getProjID()
	if projid == "" {
		return webinterror(w, errstr)
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		glog.Fatal(err)
	}
	rc, err := client.Bucket(bktname).Object(objname).NewReader(ctx)
	if err != nil {
		errstr := fmt.Sprintf("Failed to create rc for object %s to file %q, err: %v", objname, fname, err)
		return webinterror(w, errstr)
	}
	defer rc.Close()
	// strips the last part from filepath
	dirname := filepath.Dir(fname)
	if err = CreateDir(dirname); err != nil {
		glog.Errorf("Failed to create local dir %q, err: %s", dirname, err)
		return webinterror(w, errstr)
	}
	file, err := os.Create(fname)
	if err != nil {
		errstr := fmt.Sprintf("Failed to create file %q, err: %v", fname, err)
		return webinterror(w, errstr)
	} else {
		glog.Infof("Created file %q", fname)
	}
	bytes, err := io.Copy(file, rc)
	if err != nil {
		errstr := fmt.Sprintf("Failed to download object %s to file %q, err: %v", objname, fname, err)
		return webinterror(w, errstr)
		// FIXME: checksetmounterror() - see aws.go
	}

	stats := getstorstats()
	stats.add("bytesloaded", bytes)
	return nil
}
