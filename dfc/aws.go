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
	AWS_META_DATA_PUT_DFC_HASH_TYPE = "x-amz-meta-dfc-hash-type"
	AWS_META_DATA_PUT_DFC_HASH      = "x-amz-meta-dfc-hash"
	AWS_META_DATA_GET_DFC_HASH_TYPE = "X-Amz-Meta-Dfc-Hash-Type"
	AWS_META_DATA_GET_DFC_HASH      = "X-Amz-Meta-Dfc-Hash"
	AWS_MULTI_PART_DELIMITER        = "-"
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
	glog.Infof("aws listbucket %s", bucket)
	sess := createsession()
	svc := s3.New(sess)

	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	if msg.GetPrefix != "" {
		params.Prefix = &msg.GetPrefix
	}
	resp, err := svc.ListObjects(params)
	if err != nil {
		errstr = err.Error()
		errcode = awsErrorToHTTP(err)
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
	// object may not have dfc metadata.
	if val, ok := obj.Metadata[AWS_META_DATA_GET_DFC_HASH_TYPE]; ok {
		v = newcksumvalue(*val, *obj.Metadata[AWS_META_DATA_GET_DFC_HASH])
	}
	md5, _ := strconv.Unquote(*obj.ETag)
	// Check for MultiPart
	if strings.Contains(md5, AWS_MULTI_PART_DELIMITER) {
		if glog.V(3) {
			glog.Infof("MultiPart object (bucket %s key %s) download and validation not supported",
				bucket, objname)
		}
		// Ignore ETag
		md5 = ""
	}
	// cloudhobj may be nil for legacy objects.
	// md5 will be empty for Multipart objects.
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
		md[AWS_META_DATA_PUT_DFC_HASH_TYPE] = aws.String(htype)
		md[AWS_META_DATA_PUT_DFC_HASH] = aws.String(hval)
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
