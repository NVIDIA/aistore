// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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

func (cobj *awsif) listbucket(w http.ResponseWriter, bucket string, msg *GetMsg) error {
	glog.Infof("listbucket %s", bucket)
	sess := createsession()
	svc := s3.New(sess)
	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	resp, err := svc.ListObjects(params)
	if err != nil {
		return webinterror(w, err.Error())
	}
	// var msg GetMsg
	var reslist = BucketList{Entries: make([]*BucketEntry, 0, 1000)}
	for _, key := range resp.Contents {
		entry := &BucketEntry{}
		entry.Name = *(key.Key)
		if strings.Contains(msg.GetProps, GetPropsSize) {
			entry.Size = *(key.Size)
		}
		if strings.Contains(msg.GetProps, GetPropsCtime) {
			t := *(key.LastModified)
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
			omd5, _ := strconv.Unquote(*key.ETag)
			entry.Checksum = omd5
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

// This function download S3 object into local file.
func (cobj *awsif) getobj(w http.ResponseWriter, fqn, bucket, objname string) (file *os.File, err error) {
	var errstr string
	if file, errstr = initobj(fqn); errstr != "" {
		return nil, webinterror(w, errstr)
	}
	sess := createsession()
	s3Svc := s3.New(sess)

	obj, err := s3Svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objname),
	})
	if err != nil {
		errstr := fmt.Sprintf("Failed to download object %s from bucket %s, err: %v", objname, bucket, err)
		file.Close()
		return nil, webinterror(w, errstr)
	}
	defer obj.Body.Close()
	// Get ETag from object header
	omd5, _ := strconv.Unquote(*obj.ETag)

	size, errstr := getobjto_Md5(file, fqn, objname, omd5, obj.Body)
	if errstr != "" {
		return nil, webinterror(w, errstr)
	}
	stats := getstorstats()
	stats.add("bytesloaded", size)
	return file, nil
}

func (cobj *awsif) putobj(r *http.Request, w http.ResponseWriter, fqn, bucket, objname, md5sum string) error {
	// create local copy
	var err error
	size := r.ContentLength
	teebuf, b := maketeerw(r)

	sess := createsession()
	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)
	// Upload the file to S3.
	_, err = uploader.Upload(&s3manager.UploadInput{

		Bucket: aws.String(bucket),
		Key:    aws.String(objname),
		Body:   teebuf,
	})
	if err != nil {
		glog.Errorf("Failed to put key %s into bucket %s, err: %v", objname, bucket, err)
		return webinterror(w, err.Error())
	}
	glog.Infof("Uploaded object %s into bucket %s", objname, bucket)

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
