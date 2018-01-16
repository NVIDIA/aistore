/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
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

// TODO: jsonify
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
			errstr := fmt.Sprintf("Failed to list bucket, err: %v", err)
			return webinterror(w, errstr)
		}
		fmt.Fprintln(w, attrs.Name)
	}
	return nil
}

// FIXME: revisit error processing
func (obj *gcpif) getobj(w http.ResponseWriter, fqn, bucket, objname string) (file *os.File, err error) {
	projid, errstr := getProjID()
	if projid == "" {
		return nil, webinterror(w, errstr)
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		glog.Fatal(err)
	}
	rc, err := client.Bucket(bucket).Object(objname).NewReader(ctx)
	if err != nil {
		errstr = fmt.Sprintf("Failed to create rc for object %s to file %q, err: %v", objname, fqn, err)
		return nil, webinterror(w, errstr)
	}
	defer rc.Close()

	dirname := filepath.Dir(fqn)
	if err = CreateDir(dirname); err != nil {
		errstr = fmt.Sprintf("Failed to create local dir %q, err: %s", dirname, err)
		return nil, webinterror(w, errstr)
	}
	file, err = os.Create(fqn)
	if err != nil {
		errstr = fmt.Sprintf("Failed to create local file %q, err: %v", fqn, err)
		return nil, webinterror(w, errstr)
	}
	// bytes, err := io.Copy(file, rc)
	bytes, err := copyBuffer(file, rc)
	if err != nil {
		errstr = fmt.Sprintf("Failed to download object %s to file %q, err: %v", objname, fqn, err)
		return nil, webinterror(w, errstr)
		// FIXME: checksetmounterror() - see aws.go
	}

	stats := getstorstats()
	stats.add("bytesloaded", bytes)
	return file, nil
}
