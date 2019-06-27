// +build aws

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
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

func awsErrorToAISError(bucket string, awsError error) (error, int) {
	if reqErr, ok := awsError.(awserr.RequestFailure); ok {
		if reqErr.Code() == s3.ErrCodeNoSuchBucket {
			return cmn.NewErrorCloudBucketDoesNotExist(bucket), reqErr.StatusCode()
		}
		return awsError, reqErr.StatusCode()
	}

	return awsError, http.StatusInternalServerError
}

func awsIsVersionSet(version *string) bool {
	return version != nil && *version != "null" && *version != ""
}

//==================
//
// bucket operations
//
//==================
func (awsimpl *awsimpl) ListBucket(ct context.Context, bucket string, msg *cmn.SelectMsg) (reslist *cmn.BucketList, err error, errcode int) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("listbucket %s", bucket)
	}
	sess := createSession(ct)
	svc := s3.New(sess)

	params := &s3.ListObjectsInput{Bucket: aws.String(bucket)}
	if msg.Prefix != "" {
		params.Prefix = aws.String(msg.Prefix)
	}
	if msg.PageMarker != "" {
		params.Marker = aws.String(msg.PageMarker)
	}
	if msg.PageSize != 0 {
		if msg.PageSize > awsMaxPageSize {
			glog.Warningf("AWS maximum page size is %d (%d requested). Returning the first %d keys",
				awsMaxPageSize, msg.PageSize, awsMaxPageSize)
			msg.PageSize = awsMaxPageSize
		}
		params.MaxKeys = aws.Int64(int64(msg.PageSize))
	}

	resp, err := svc.ListObjects(params)
	if err != nil {
		err, errcode = awsErrorToAISError(bucket, err)
		return
	}

	verParams := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)}
	if msg.Prefix != "" {
		verParams.Prefix = aws.String(msg.Prefix)
	}

	var versions map[string]*string
	if strings.Contains(msg.Props, cmn.GetPropsVersion) {
		var verResp *s3.ListObjectVersionsOutput

		verResp, err = svc.ListObjectVersions(verParams)
		if err != nil {
			err, errcode = awsErrorToAISError(bucket, err)
			return
		}

		versions = make(map[string]*string, InitialBucketListSize)
		for _, vers := range verResp.Versions {
			if *(vers.IsLatest) && awsIsVersionSet(vers.VersionId) {
				versions[*(vers.Key)] = vers.VersionId
			}
		}
	}

	// var msg cmn.SelectMsg
	reslist = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, InitialBucketListSize)}
	for _, key := range resp.Contents {
		entry := &cmn.BucketEntry{}
		entry.Name = *(key.Key)
		if strings.Contains(msg.Props, cmn.GetPropsSize) {
			entry.Size = *(key.Size)
		}
		if strings.Contains(msg.Props, cmn.GetPropsChecksum) {
			omd5, _ := strconv.Unquote(*key.ETag)
			entry.Checksum = omd5
		}
		if strings.Contains(msg.Props, cmn.GetPropsVersion) {
			if val, ok := versions[*(key.Key)]; ok && awsIsVersionSet(val) {
				entry.Version = *val
			}
		}
		// TODO: other cmn.SelectMsg props TBD
		reslist.Entries = append(reslist.Entries, entry)
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("listbucket count %d", len(reslist.Entries))
	}

	if *resp.IsTruncated {
		// For AWS, resp.NextMarker is only set when a query has a delimiter.
		// Without a delimiter, NextMarker should be the last returned key.
		reslist.PageMarker = reslist.Entries[len(reslist.Entries)-1].Name
	}

	return
}

func (awsimpl *awsimpl) headbucket(ct context.Context, bucket string) (bucketprops cmn.SimpleKVs, err error, errcode int) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("headbucket %s", bucket)
	}
	bucketprops = make(cmn.SimpleKVs)

	sess := createSession(ct)
	svc := s3.New(sess)
	input := &s3.HeadBucketInput{Bucket: aws.String(bucket)}

	_, err = svc.HeadBucket(input)
	if err != nil {
		err, errcode = awsErrorToAISError(bucket, err)
		return
	}
	bucketprops[cmn.HeaderCloudProvider] = cmn.ProviderAmazon

	inputVers := &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)}
	result, err := svc.GetBucketVersioning(inputVers)
	if err != nil {
		err, errcode = awsErrorToAISError(bucket, err)
		return
	}

	bucketprops[cmn.HeaderBucketVerEnabled] =
		strconv.FormatBool(result.Status != nil && *result.Status == s3.BucketVersioningStatusEnabled)
	return
}

func (awsimpl *awsimpl) getbucketnames(ctx context.Context) (buckets []string, err error, errcode int) {
	sess := createSession(ctx)
	svc := s3.New(sess)
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		err, errcode = awsErrorToAISError("", err)
		return
	}
	buckets = make([]string, len(result.Buckets))
	for idx, bkt := range result.Buckets {
		if glog.FastV(4, glog.SmoduleAIS) {
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
func (awsimpl *awsimpl) headobject(ctx context.Context, lom *cluster.LOM) (objmeta cmn.SimpleKVs, err error, errcode int) {
	objmeta = make(cmn.SimpleKVs)

	sess := createSession(ctx)
	svc := s3.New(sess)
	input := &s3.HeadObjectInput{Bucket: aws.String(lom.Bucket), Key: aws.String(lom.Objname)}

	headOutput, err := svc.HeadObject(input)
	if err != nil {
		err, errcode = awsErrorToAISError(lom.Bucket, err)
		return
	}
	objmeta[cmn.HeaderCloudProvider] = cmn.ProviderAmazon
	if awsIsVersionSet(headOutput.VersionId) {
		objmeta[cmn.HeaderObjVersion] = *headOutput.VersionId
	}
	size := strconv.FormatInt(*headOutput.ContentLength, 10)
	objmeta[cmn.HeaderObjSize] = size
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("HEAD %s", lom)
	}
	return
}

//=======================
//
// object data operations
//
//=======================
func (awsimpl *awsimpl) getobj(ctx context.Context, workFQN string, lom *cluster.LOM) (err error, errcode int) {
	var (
		cksum        cmn.Cksummer
		cksumToCheck cmn.Cksummer
	)
	sess := createSession(ctx)
	svc := s3.New(sess)
	obj, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(lom.Bucket),
		Key:    aws.String(lom.Objname),
	})
	if err != nil {
		err, errcode = awsErrorToAISError(lom.Bucket, err)
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
	lom.SetCksum(cksum)
	if obj.VersionId != nil {
		lom.SetVersion(*obj.VersionId)
	}
	poi := &putObjInfo{
		t:            awsimpl.t,
		lom:          lom,
		r:            obj.Body,
		cksumToCheck: cksumToCheck,
		workFQN:      workFQN,
		cold:         true,
	}
	if err = poi.writeToFile(); err != nil {
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("GET %s", lom)
	}
	return
}

func (awsimpl *awsimpl) putobj(ctx context.Context, file *os.File, lom *cluster.LOM) (version string, err error, errcode int) {
	var (
		uploadoutput *s3manager.UploadOutput
	)
	cksumType, cksumValue := lom.Cksum().Get()
	md := make(map[string]*string)
	md[awsChecksumType] = aws.String(cksumType)
	md[awsChecksumVal] = aws.String(cksumValue)

	sess := createSession(ctx)
	uploader := s3manager.NewUploader(sess)
	uploadoutput, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:   aws.String(lom.Bucket),
		Key:      aws.String(lom.Objname),
		Body:     file,
		Metadata: md,
	})
	if err != nil {
		err, errcode = awsErrorToAISError(lom.Bucket, err)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		if uploadoutput.VersionID != nil {
			version = *uploadoutput.VersionID
			glog.Infof("PUT %s, version %s", lom, version)
		} else {
			glog.Infof("PUT %s", lom)
		}
	}
	return
}

func (awsimpl *awsimpl) deleteobj(ctx context.Context, lom *cluster.LOM) (err error, errcode int) {
	sess := createSession(ctx)
	svc := s3.New(sess)
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(lom.Bucket), Key: aws.String(lom.Objname)})
	if err != nil {
		err, errcode = awsErrorToAISError(lom.Bucket, err)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("DELETE %s", lom)
	}
	return
}
