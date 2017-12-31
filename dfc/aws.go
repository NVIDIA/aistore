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
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/glog"
)

func createsession() *session.Session {
	// TODO: avoid creating sessions for each request
	return session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable}))

}
func (obj *awsif) listbucket(w http.ResponseWriter, bucket string) {
	glog.Infof(" listbucket : bucket = %s ", bucket)
	sess := createsession()
	svc := s3.New(sess)
	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	resp, _ := svc.ListObjects(params)
	for _, key := range resp.Contents {
		glog.Infof(" bucket = %s key = %s", bucket, *key.Key)
		// TODO modifiy response format to JSON or as per application needs.
		// It will print keys but not metadata of keys.
		// Getting metadata of indiviual key is separate operation/call and
		// need to be done on per object basis.
		keystr := fmt.Sprintf("%s", *key.Key)
		fmt.Fprintln(w, keystr)
	}
}

func (obj *awsif) getobj(w http.ResponseWriter, mpath string, bktname string, keyname string) {
	fname := mpath + "/" + bktname + "/" + keyname
	sess := createsession()
	// Create S3 Downloader
	// TODO: Optimize downloader options
	// (currently: 5MB chunks and 5 concurrent downloads)
	downloader := s3manager.NewDownloader(sess)

	err := downloadobject(w, downloader, mpath, bktname, keyname)
	if err != nil {
		stats := getstorstats()
		atomic.AddInt64(&stats.numerr, 1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		glog.Infof("Downloaded bucket %s key %s fqn %q", bktname, keyname, fname)
	}
	return
}

// This function download S3 object into local file.
func downloadobject(w http.ResponseWriter, downloader *s3manager.Downloader,
	mpath string, bucket string, kname string) error {

	var file *os.File
	var err error
	var bytes int64

	fname := mpath + "/" + bucket + "/" + kname
	// strips the last part from filepath
	dirname := filepath.Dir(fname)
	_, err = os.Stat(dirname)
	if err != nil {
		// Create bucket-path directory for non existent paths.
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirname, 0755)
			if err != nil {
				glog.Errorf("Failed to create local dir %s, err: %s", dirname, err)
				return err
			}
		} else {
			glog.Errorf("Failed to fstat dir %q, err: %v", dirname, err)
			return err
		}
	}

	file, err = os.Create(fname)
	if err != nil {
		glog.Errorf("Unable to create file %q, err: %v", fname, err)
		checksetmounterror(fname)
		return err
	}
	bytes, err = downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(kname),
	})
	if err != nil {
		glog.Errorf("Failed to download key %s from bucket %s, err: %v", kname, bucket, err)
		checksetmounterror(fname)
	} else {
		stats := getstorstats()
		atomic.AddInt64(&stats.bytesloaded, bytes)
	}
	return err
}
