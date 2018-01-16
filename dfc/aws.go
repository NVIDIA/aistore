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
func (obj *awsif) listbucket(w http.ResponseWriter, bucket string) error {
	glog.Infof(" listbucket : bucket = %s ", bucket)
	sess := createsession()
	svc := s3.New(sess)
	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	resp, err := svc.ListObjects(params)
	if err != nil {
		return webinterror(w, err.Error())
	}
	// TODO: reimplement in JSON
	for _, key := range resp.Contents {
		glog.Infof("bucket = %s key = %s", bucket, *key.Key)
		keystr := fmt.Sprintf("%s", *key.Key)
		fmt.Fprintln(w, keystr)
	}
	return nil
}

func (obj *awsif) getobj(w http.ResponseWriter, fqn, bucket, objname string) (file *os.File, err error) {
	sess := createsession()
	//
	// TODO: Optimize downloader options (currently 5MB chunks and 5 concurrent downloads)
	//
	downloader := s3manager.NewDownloader(sess)

	file, err = downloadobject(w, downloader, fqn, bucket, objname)
	if err != nil {
		return nil, webinterror(w, err.Error())
	}
	glog.Infof("Downloaded bucket %s key %s fqn %q", bucket, objname, fqn)
	return file, nil
}

// This function download S3 object into local file.
func downloadobject(w http.ResponseWriter, downloader *s3manager.Downloader,
	fqn, bucket, objname string) (file *os.File, err error) {
	var bytes int64

	// strips the last part from filepath
	dirname := filepath.Dir(fqn)
	if err = CreateDir(dirname); err != nil {
		glog.Errorf("Failed to create local dir %q, err: %s", dirname, err)
		return nil, err
	}
	file, err = os.Create(fqn)
	if err != nil {
		glog.Errorf("Unable to create file %q, err: %v", fqn, err)
		checksetmounterror(fqn)
		return nil, err
	}
	bytes, err = downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objname),
	})
	if err != nil {
		glog.Errorf("Failed to download key %s from bucket %s, err: %v", objname, bucket, err)
		checksetmounterror(fqn)
		return nil, err
	}
	stats := getstorstats()
	stats.add("bytesloaded", bytes)
	return file, nil
}
