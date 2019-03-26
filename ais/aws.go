// +build aws

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	jsoniter "github.com/json-iterator/go"
)

const (
	awsChecksumType   = "x-amz-meta-ais-cksum-type"
	awsChecksumVal    = "x-amz-meta-ais-cksum-val"
	awsMultipartDelim = "-"
	awsMaxPageSize    = 1000
)

//======
//
// implements cloudif
//
//======
type (
	awsCreds struct {
		region string
		key    string
		secret string
	}

	awsimpl struct {
		t *targetrunner
	}
)

var (
	_ cloudif = &awsimpl{}
)

func newAWSProvider(t *targetrunner) *awsimpl { return &awsimpl{t} }

// If extractAWSCreds returns no error and awsCreds is nil then the default
//   AWS client is used (that loads credentials from ~/.aws/credentials)
func extractAWSCreds(credsList map[string]string) *awsCreds {
	if len(credsList) == 0 {
		return nil
	}
	raw, ok := credsList[cmn.ProviderAmazon]
	if raw == "" || !ok {
		return nil
	}

	// TODO: maybe authn should make JSON from INI before creating a token
	// Now just parse AWS file in original INI-format
	parts := strings.Split(raw, "\n")
	creds := &awsCreds{}

	for _, s := range parts {
		if strings.HasPrefix(s, "region") {
			values := strings.SplitN(s, "=", 2)
			if len(values) == 2 {
				creds.region = strings.TrimSpace(values[1])
			}
		} else if strings.HasPrefix(s, "aws_access_key_id") {
			values := strings.SplitN(s, "=", 2)
			if len(values) == 2 {
				creds.key = strings.TrimSpace(values[1])
			}
		} else if strings.HasPrefix(s, "aws_secret_access_key") {
			values := strings.SplitN(s, "=", 2)
			if len(values) == 2 {
				creds.secret = strings.TrimSpace(values[1])
			}
		}
		if creds.region != "" && creds.key != "" && creds.secret != "" {
			return creds
		}
	}

	glog.Errorf("Invalid credentials: %#v\nUsing default AWS session", creds)
	return nil
}

//======
//
// session FIXME: optimize
//
//======
// A new session is created in two ways:
// 1. Authn is disabled or directory with credentials is not defined
//    In this case a session is created using default credentials from
//    configuration file in ~/.aws/credentials and environment variables
// 2. Authn is enabled and directory with credential files is set
//    The function looks for 'credentials' file in the directory.
//    A userID is retrieved from token. The userID section must exist
//    in credential file located in the given directory.
//    A userID section should look like this:
//    [userID]
//    region = us-east-1
//    aws_access_key_id = USERKEY
//    aws_secret_access_key = USERSECRET
// If creation of a session with provided directory and userID fails, it
// tries to create a session with default parameters
func createSession(ct context.Context) *session.Session {
	// TODO: avoid creating sessions for each request
	userID := getStringFromContext(ct, ctxUserID)
	userCreds := userCredsFromContext(ct)
	if userID == "" || userCreds == nil {
		if glog.V(5) {
			glog.Info("No user ID or empty credentials: opening default session")
		}
		// default session
		return session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable}))
	}

	creds := extractAWSCreds(userCreds)
	if creds == nil {
		glog.Errorf("Failed to retrieve %s credentials %s", cmn.ProviderAmazon, userID)
		return session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable}))
	}

	awsCreds := credentials.NewStaticCredentials(creds.key, creds.secret, "")
	conf := aws.Config{
		Region:      aws.String(creds.region),
		Credentials: awsCreds,
	}
	return session.Must(session.NewSessionWithOptions(session.Options{Config: conf}))
}

func awsErrorToHTTP(awsError error) int {
	if reqErr, ok := awsError.(awserr.RequestFailure); ok {
		return reqErr.StatusCode()
	}

	return http.StatusInternalServerError
}

func awsIsVersionSet(version *string) bool {
	return version != nil && *version != "null" && *version != ""
}

//==================
//
// bucket operations
//
//==================
func (awsimpl *awsimpl) listbucket(ct context.Context, bucket string, msg *cmn.GetMsg) (jsbytes []byte, errstr string, errcode int) {
	if glog.V(4) {
		glog.Infof("listbucket %s", bucket)
	}
	sess := createSession(ct)
	svc := s3.New(sess)

	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	if msg.GetPrefix != "" {
		params.Prefix = aws.String(msg.GetPrefix)
	}
	if msg.GetPageMarker != "" {
		params.Marker = aws.String(msg.GetPageMarker)
	}
	if msg.GetPageSize != 0 {
		if msg.GetPageSize > awsMaxPageSize {
			glog.Warningf("AWS maximum page size is %d (%d requested). Returning the first %d keys",
				awsMaxPageSize, msg.GetPageSize, awsMaxPageSize)
			msg.GetPageSize = awsMaxPageSize
		}
		params.MaxKeys = aws.Int64(int64(msg.GetPageSize))
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
	if strings.Contains(msg.GetProps, cmn.GetPropsVersion) {
		verResp, err := svc.ListObjectVersions(verParams)
		if err != nil {
			errstr = err.Error()
			errcode = awsErrorToHTTP(err)
			return
		}

		versions = make(map[string]*string, initialBucketListSize)
		for _, vers := range verResp.Versions {
			if *(vers.IsLatest) && awsIsVersionSet(vers.VersionId) {
				versions[*(vers.Key)] = vers.VersionId
			}
		}
	}

	// var msg cmn.GetMsg
	var reslist = cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, initialBucketListSize)}
	for _, key := range resp.Contents {
		entry := &cmn.BucketEntry{}
		entry.Name = *(key.Key)
		if strings.Contains(msg.GetProps, cmn.GetPropsSize) {
			entry.Size = *(key.Size)
		}
		if strings.Contains(msg.GetProps, cmn.GetPropsCtime) {
			t := *(key.LastModified)
			entry.Ctime = cmn.FormatTime(t, msg.GetTimeFormat)
		}
		if strings.Contains(msg.GetProps, cmn.GetPropsChecksum) {
			omd5, _ := strconv.Unquote(*key.ETag)
			entry.Checksum = omd5
		}
		if strings.Contains(msg.GetProps, cmn.GetPropsVersion) {
			if val, ok := versions[*(key.Key)]; ok && awsIsVersionSet(val) {
				entry.Version = *val
			}
		}
		// TODO: other cmn.GetMsg props TBD
		reslist.Entries = append(reslist.Entries, entry)
	}
	if glog.V(4) {
		glog.Infof("listbucket count %d", len(reslist.Entries))
	}

	if *resp.IsTruncated {
		// For AWS, resp.NextMarker is only set when a query has a delimiter.
		// Without a delimiter, NextMarker should be the last returned key.
		reslist.PageMarker = reslist.Entries[len(reslist.Entries)-1].Name
	}

	jsbytes, err = jsoniter.Marshal(reslist)
	cmn.AssertNoErr(err)
	return
}

func (awsimpl *awsimpl) headbucket(ct context.Context, bucket string) (bucketprops cmn.SimpleKVs, errstr string, errcode int) {
	if glog.V(4) {
		glog.Infof("headbucket %s", bucket)
	}
	bucketprops = make(cmn.SimpleKVs)

	sess := createSession(ct)
	svc := s3.New(sess)
	input := &s3.HeadBucketInput{Bucket: aws.String(bucket)}

	_, err := svc.HeadBucket(input)
	if err != nil {
		errcode = awsErrorToHTTP(err)
		errstr = fmt.Sprintf("The bucket %s either %s or is not accessible, err: %v", bucket, cmn.DoesNotExist, err)
		return
	}
	bucketprops[cmn.HeaderCloudProvider] = cmn.ProviderAmazon

	inputVers := &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)}
	result, err := svc.GetBucketVersioning(inputVers)
	if err != nil {
		errcode = awsErrorToHTTP(err)
		errstr = fmt.Sprintf("The bucket %s either %s or is not accessible, err: %v", bucket, cmn.DoesNotExist, err)
	} else {
		if result.Status != nil && *result.Status == s3.BucketVersioningStatusEnabled {
			bucketprops[cmn.HeaderVersioning] = cmn.VersionCloud
		} else {
			bucketprops[cmn.HeaderVersioning] = cmn.VersionNone
		}
	}
	return
}

func (awsimpl *awsimpl) getbucketnames(ct context.Context) (buckets []string, errstr string, errcode int) {
	sess := createSession(ct)
	svc := s3.New(sess)
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		errcode = awsErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to list all buckets, err: %v", err)
		return
	}
	buckets = make([]string, len(result.Buckets))
	for idx, bkt := range result.Buckets {
		if glog.V(4) {
			glog.Infof("%s: created %v", aws.StringValue(bkt.Name), *bkt.CreationDate)
		}
		buckets[idx] = aws.StringValue(bkt.Name)
	}
	return
}

//============
//
// object meta
//
//============
func (awsimpl *awsimpl) headobject(ct context.Context, bucket string, objname string) (objmeta cmn.SimpleKVs, errstr string, errcode int) {
	if glog.V(4) {
		glog.Infof("headobject %s/%s", bucket, objname)
	}
	objmeta = make(cmn.SimpleKVs)

	sess := createSession(ct)
	svc := s3.New(sess)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(objname)}

	headOutput, err := svc.HeadObject(input)
	if err != nil {
		errcode = awsErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to retrieve %s/%s metadata, err: %v", bucket, objname, err)
		return
	}
	objmeta[cmn.HeaderCloudProvider] = cmn.ProviderAmazon
	if awsIsVersionSet(headOutput.VersionId) {
		objmeta[cmn.HeaderObjVersion] = *headOutput.VersionId
	}
	size := strconv.FormatInt(*headOutput.ContentLength, 10)
	objmeta[cmn.HeaderObjSize] = size
	return
}

//=======================
//
// object data operations
//
//=======================
func (awsimpl *awsimpl) getobj(ctx context.Context, workFQN, bucket, objname string) (lom *cluster.LOM, errstr string, errcode int) {
	var (
		cksum        cmn.CksumProvider
		cksumToCheck cmn.CksumProvider
	)

	sess := createSession(ctx)
	svc := s3.New(sess)
	obj, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objname),
	})
	if err != nil {
		errcode = awsErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to GET %s/%s, err: %v", bucket, objname, err)
		return
	}
	// may not have ais metadata
	if htype, ok := obj.Metadata[awsChecksumType]; ok {
		if hval, ok := obj.Metadata[awsChecksumVal]; ok {
			cksum = cmn.NewCksum(*htype, *hval)
		}
	}

	md5, _ := strconv.Unquote(*obj.ETag)
	// FIXME: multipart
	if !strings.Contains(md5, awsMultipartDelim) {
		cksumToCheck = cmn.NewCksum(cmn.ChecksumMD5, md5)
	}

	lom = &cluster.LOM{T: awsimpl.t, Bucket: bucket, Objname: objname, Cksum: cksum}
	if obj.VersionId != nil {
		lom.Version = *obj.VersionId
	}
	if errstr = lom.Fill(cmn.CloudBs, 0); errstr != "" {
		return
	}
	roi := &recvObjInfo{
		t:            awsimpl.t,
		cold:         true,
		r:            obj.Body,
		cksumToCheck: cksumToCheck,
		lom:          lom,
		workFQN:      workFQN,
	}
	if err := roi.writeToFile(); err != nil {
		errstr = err.Error()
		return
	}
	if glog.V(4) {
		glog.Infof("GET %s/%s", bucket, objname)
	}
	return
}

func (awsimpl *awsimpl) putobj(ct context.Context, file *os.File, bucket, objname string, cksum cmn.CksumProvider) (version string, errstr string, errcode int) {
	var (
		err          error
		uploadoutput *s3manager.UploadOutput
	)

	cksumType, cksumValue := cksum.Get()
	md := make(map[string]*string)
	md[awsChecksumType] = aws.String(cksumType)
	md[awsChecksumVal] = aws.String(cksumValue)

	sess := createSession(ct)
	uploader := s3manager.NewUploader(sess)
	uploadoutput, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(objname),
		Body:     file,
		Metadata: md,
	})
	if err != nil {
		errcode = awsErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to PUT %s/%s, err: %v", bucket, objname, err)
		return
	}
	if glog.V(4) {
		if uploadoutput.VersionID != nil {
			version = *uploadoutput.VersionID
			glog.Infof("PUT %s/%s, version %s", bucket, objname, version)
		} else {
			glog.Infof("PUT %s/%s", bucket, objname)
		}
	}
	return
}

func (awsimpl *awsimpl) deleteobj(ct context.Context, bucket, objname string) (errstr string, errcode int) {
	sess := createSession(ct)
	svc := s3.New(sess)
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(objname)})
	if err != nil {
		errcode = awsErrorToHTTP(err)
		errstr = fmt.Sprintf("Failed to DELETE %s/%s, err: %v", bucket, objname, err)
		return
	}
	if glog.V(4) {
		glog.Infof("DELETE %s/%s", bucket, objname)
	}
	return
}
