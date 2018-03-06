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
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/glog"
)

const (
	awsPutDfcHashType = "x-amz-meta-dfc-hash-type"
	awsPutDfcHashVal  = "x-amz-meta-dfc-hash-val"
	awsGetDfcHashType = "X-Amz-Meta-Dfc-Hash-Type"
	awsGetDfcHashVal  = "X-Amz-Meta-Dfc-Hash-Val"
	awsMultipartDelim = "-"
)

//======
//
// implements cloudif
//
//======
type awsimpl struct {
	t *targetrunner
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

func awsErrorToHTTP(awsError error) int {
	if reqErr, ok := awsError.(awserr.RequestFailure); ok {
		return reqErr.StatusCode()
	}

	return http.StatusInternalServerError
}

//======
//
// methods
//
//======
func (awsimpl *awsimpl) listbucket(bucket string, msg *GetMsg) (jsbytes []byte, errstr string, errcode int) {
	glog.Infof("aws: listbucket %s", bucket)
	sess := createsession()
	svc := s3.New(sess)

	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	if msg.GetPrefix != "" {
		params.Prefix = aws.String(msg.GetPrefix)
	}
	resp, err := svc.ListObjects(params)
	if err != nil {
		errstr = err.Error()
		errcode = awsErrorToHTTP(err)
		return
	}

	verParams := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)}
	if msg.GetPrefix != "" {
		verParams.Prefix = aws.String(msg.GetPrefix)
	}

	var versions map[string]*string
	if strings.Contains(msg.GetProps, GetPropsVersion) {
		verResp, err := svc.ListObjectVersions(verParams)
		if err != nil {
			errstr = err.Error()
			errcode = awsErrorToHTTP(err)
			return
		}

		versions := make(map[string]*string, initialBucketListSize)
		for _, vers := range verResp.Versions {
			if *(vers.IsLatest) && vers.VersionId != nil {
				versions[*(vers.Key)] = vers.VersionId
			}
		}
	}

	// var msg GetMsg
	var reslist = BucketList{Entries: make([]*BucketEntry, 0, initialBucketListSize)}
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
		if strings.Contains(msg.GetProps, GetPropsVersion) {
			if val, ok := versions[*(key.Key)]; ok {
				entry.Version = *val
			}
		}
		// TODO: other GetMsg props TBD
		reslist.Entries = append(reslist.Entries, entry)
	}
	if glog.V(3) {
		glog.Infof("listbucket count %d", len(reslist.Entries))
	}
	jsbytes, err = json.Marshal(reslist)
	assert(err == nil, err)
	return
}

func (awsimpl *awsimpl) getobj(fqn, bucket, objname string) (nhobj cksumvalue, size int64, errstr string, errcode int) {
	var v cksumvalue
	sess := createsession()
	svc := s3.New(sess)
	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objname),
	})
	if err != nil {
		errcode = awsErrorToHTTP(err)
		errstr = fmt.Sprintf("aws: Failed to get %s from bucket %s, err: %v", objname, bucket, err)
		return
	}
	defer obj.Body.Close()
	// object may not have dfc metadata
	if htype, ok := obj.Metadata[awsGetDfcHashType]; ok {
		if hval, ok := obj.Metadata[awsGetDfcHashVal]; ok {
			v = newcksumvalue(*htype, *hval)
		}
	}
	md5, _ := strconv.Unquote(*obj.ETag)
	// FIXME: multipart
	if strings.Contains(md5, awsMultipartDelim) {
		if glog.V(3) {
			glog.Infof("Multipart object %s (bucket %s) - not validating checksum", objname, bucket)
		}
		md5 = ""
	}
	if nhobj, size, errstr = awsimpl.t.receiveFileAndFinalize(fqn, objname, md5, v, obj.Body); errstr != "" {
		return
	}
	if glog.V(3) {
		glog.Infof("aws: GET %s (bucket %s)", objname, bucket)
	}
	return
}

func (awsimpl *awsimpl) putobj(file *os.File, bucket, objname string, ohash cksumvalue) (errstr string, errcode int) {
	var (
		err         error
		htype, hval string
		md          map[string]*string
	)
	if ohash != nil {
		htype, hval = ohash.get()
		md = make(map[string]*string)
		md[awsPutDfcHashType] = aws.String(htype)
		md[awsPutDfcHashVal] = aws.String(hval)
	}
	sess := createsession()
	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(objname),
		Body:     file,
		Metadata: md,
	})
	if err != nil {
		errcode = awsErrorToHTTP(err)
		errstr = fmt.Sprintf("aws: Failed to put %s (bucket %s), err: %v", objname, bucket, err)
		return
	}
	if glog.V(3) {
		glog.Infof("aws: PUT %s (bucket %s)", objname, bucket)
	}
	return
}

func (awsimpl *awsimpl) deleteobj(bucket, objname string) (errstr string, errcode int) {
	sess := createsession()
	svc := s3.New(sess)
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(objname)})
	if err != nil {
		errcode = awsErrorToHTTP(err)
		errstr = fmt.Sprintf("aws: Failed to delete %s (bucket %s), err: %v", objname, bucket, err)
		return
	}
	if glog.V(3) {
		glog.Infof("aws: deleted %s (bucket %s)", objname, bucket)
	}
	return
}
