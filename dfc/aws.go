// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/glog"
)

//======
//
// types
//
//======
type awsif struct {
}

//======
//
// session FIXME: optimize
//
//======
func createsession() *session.Session {
	// TODO: avoid creating sessions for each request
	return session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable}))

}

//======
//
// methods
//
//======
func (cloudif *awsif) listbucket(w http.ResponseWriter, bucket string, msg *GetMsg) (errstr string) {
	glog.Infof("aws listbucket %s", bucket)
	sess := createsession()
	svc := s3.New(sess)
	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	resp, err := svc.ListObjects(params)
	if err != nil {
		errstr = err.Error()
		return
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
	return
}

func (cloudif *awsif) getobj(fqn, bucket, objname string) (md5hash string, errstr string) {
	var (
		size int64
		omd5 string
	)
	sess := createsession()
	s3Svc := s3.New(sess)
	obj, err := s3Svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objname),
	})
	if err != nil {
		return "", fmt.Sprintf("aws: Failed to get %s from bucket %s, err: %v", objname, bucket, err)
	}
	defer obj.Body.Close()

	// the object is multipart?
	omd5, _ = strconv.Unquote(*obj.ETag)
	if size, md5hash, errstr = ReceiveFileAndFinalize(fqn, objname, omd5, obj.Body); errstr != "" {
		return "", errstr
	}
	stats := getstorstats()
	stats.add("bytesloaded", size)
	if glog.V(3) {
		glog.Infof("aws: GET %s (bucket %s)", objname, bucket)
	}
	return

}

func (cloudif *awsif) putobj(file *os.File, bucket, objname string) (errstr string) {
	sess := createsession()
	uploader := s3manager.NewUploader(sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objname),
		Body:   file,
	})
	if err != nil {
		errstr = fmt.Sprintf("aws: Failed to put %s (bucket %s), err: %v", objname, bucket, err)
		return
	}
	if glog.V(3) {
		glog.Infof("aws: PUT %s (bucket %s)", objname, bucket)
	}
	return
}
func (cloudif *awsif) deleteobj(bucket, objname string) (errstr string) {
	sess := createsession()
	svc := s3.New(sess)
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(objname)})
	if err != nil {
		errstr = fmt.Sprintf("aws: Failed to delete %s (bucket %s), err: %v", objname, bucket, err)
		return
	}
	if glog.V(3) {
		glog.Infof("aws: deleted %s (bucket %s)", objname, bucket)
	}
	return
}
