/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"

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
	glog.Infof("listbucket %s ", bucket)
	sess := createsession()
	svc := s3.New(sess)
	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	resp, err := svc.ListObjects(params)
	if err != nil {
		return webinterror(w, err.Error())
	}
	// var msg GetMsg
	var reslist = GetMetaResList{ResList: make([]*GetMetaRes, 0, 1000)}
	for _, key := range resp.Contents {
		entry := &GetMetaRes{}
		entry.MetaName = *key.Key
		reslist.ResList = append(reslist.ResList, entry)
	}
	if glog.V(3) {
		glog.Infof("listbucket count %d", len(reslist.ResList))
	}
	jsbytes, err := json.Marshal(reslist)
	assert(err == nil, err)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsbytes)
	return nil
}

// This function download S3 object into local file.
func (cobj *awsif) getobj(w http.ResponseWriter, fqn string, bucket string, objname string) (file *os.File, err error) {
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
	defer obj.Body.Close()
	if err != nil {
		errstr := fmt.Sprintf("Failed to download object %s from bucket %s, err: %v", objname, bucket, err)
		file.Close()
		return nil, webinterror(w, errstr)
	}
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

func (cobj *awsif) putobj(r *http.Request, w http.ResponseWriter,
	bucket string, objname string) error {
	sess := createsession()
	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)
	// Upload the file to S3.
	_, err := uploader.Upload(&s3manager.UploadInput{

		Bucket: aws.String(bucket),
		Key:    aws.String(objname),
		Body:   r.Body,
	})
	if err != nil {
		glog.Errorf("Failed to put key %s into bucket %s, err: %v", objname, bucket, err)
		return webinterror(w, err.Error())
	} else {
		glog.Infof("Uploaded key %s into bucket %s", objname, bucket)
	}
	//TODO stats
	return nil
}
