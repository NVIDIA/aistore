// +build aws

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"io"
	"net/http"
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
	awsMaxPageSize    = 1000 // AWS limitation, see also cmn.DefaultListPageSize
)

type (
	awsCreds struct {
		region string
		key    string
		secret string
	}

	awsProvider struct {
		t *targetrunner
	}
)

var (
	_ cloudProvider = &awsProvider{}
)

func newAWSProvider(t *targetrunner) (cloudProvider, error) { return &awsProvider{t}, nil }

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

func awsErrorToAISError(awsError error, bck *cluster.Bck, node string) (error, int) {
	if reqErr, ok := awsError.(awserr.RequestFailure); ok {
		if reqErr.Code() == s3.ErrCodeNoSuchBucket {
			return cmn.NewErrorCloudBucketDoesNotExist(bck.Bck, node), reqErr.StatusCode()
		}
		return awsError, reqErr.StatusCode()
	}

	return awsError, http.StatusInternalServerError
}

func awsIsVersionSet(version *string) bool {
	return version != nil && *version != "null" && *version != ""
}

/////////////////
// LIST BUCKET //
/////////////////

func (awsp *awsProvider) ListBucket(ct context.Context, bucket string, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
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
		err, errCode = awsErrorToAISError(err, cluster.NewBck(bucket, cmn.ProviderAmazon, cmn.NsGlobal), "")
		return
	}

	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, initialBucketListSize)}
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

		bckList.Entries = append(bckList.Entries, entry)
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[list_bucket] count %d", len(bckList.Entries))
	}

	if *resp.IsTruncated {
		// For AWS, resp.NextMarker is only set when a query has a delimiter.
		// Without a delimiter, NextMarker should be the last returned key.
		bckList.PageMarker = bckList.Entries[len(bckList.Entries)-1].Name
	}

	if len(bckList.Entries) == 0 {
		return
	}

	// if version is requested, read versions page by page and stop
	// when there is nothing to read or the version page marker is
	// greater than object page marker
	// Page is limited with 500+ items, so reading them is slow
	if strings.Contains(msg.Props, cmn.GetPropsVersion) {
		versions := make(map[string]*string, initialBucketListSize)
		keyMarker := msg.PageMarker

		verParams := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)}
		if msg.Prefix != "" {
			verParams.Prefix = aws.String(msg.Prefix)
		}

		for {
			if keyMarker != "" {
				verParams.KeyMarker = aws.String(keyMarker)
			}

			verResp, err := svc.ListObjectVersions(verParams)
			if err != nil {
				err, errCode := awsErrorToAISError(err, cluster.NewBck(bucket, cmn.ProviderAmazon, cmn.NsGlobal), "")
				return nil, err, errCode
			}

			for _, vers := range verResp.Versions {
				if *(vers.IsLatest) && awsIsVersionSet(vers.VersionId) {
					versions[*(vers.Key)] = vers.VersionId
				}
			}

			if !(*verResp.IsTruncated) {
				break
			}

			keyMarker = *verResp.NextKeyMarker
			if bckList.PageMarker != "" && keyMarker > bckList.PageMarker {
				break
			}
		}

		for _, entry := range bckList.Entries {
			if version, ok := versions[entry.Name]; ok {
				entry.Version = *version
			}
		}
	}

	return
}

/////////////////
// HEAD BUCKET //
/////////////////

func (awsp *awsProvider) headBucket(ctx context.Context, bucket string) (bckProps cmn.SimpleKVs, err error, errCode int) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_bucket] %s", bucket)
	}
	bckProps = make(cmn.SimpleKVs)

	svc := s3.New(createSession(ctx))
	_, err = svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		err, errCode = awsErrorToAISError(err, cluster.NewBck(bucket, cmn.ProviderAmazon, cmn.NsGlobal), "")
		return
	}
	bckProps[cmn.HeaderCloudProvider] = cmn.ProviderAmazon

	inputVers := &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)}
	result, err := svc.GetBucketVersioning(inputVers)
	if err != nil {
		err, errCode = awsErrorToAISError(err, cluster.NewBck(bucket, cmn.ProviderAmazon, cmn.NsGlobal), "")
		return
	}

	bckProps[cmn.HeaderBucketVerEnabled] = strconv.FormatBool(
		result.Status != nil && *result.Status == s3.BucketVersioningStatusEnabled,
	)
	return
}

//////////////////
// BUCKET NAMES //
//////////////////

func (awsp *awsProvider) getBucketNames(ctx context.Context) (buckets []string, err error, errCode int) {
	svc := s3.New(createSession(ctx))
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		err, errCode = awsErrorToAISError(err, cluster.NewBck("", cmn.ProviderAmazon, cmn.NsGlobal), "")
		return
	}

	buckets = make([]string, len(result.Buckets))
	for idx, bck := range result.Buckets {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("[bucket_names] %s: created %v", aws.StringValue(bck.Name), *bck.CreationDate)
		}
		buckets[idx] = aws.StringValue(bck.Name)
	}
	return
}

////////////////
// HEAD OBJECT //
////////////////

func (awsp *awsProvider) headObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	objMeta = make(cmn.SimpleKVs)

	sess := createSession(ctx)
	svc := s3.New(sess)
	input := &s3.HeadObjectInput{Bucket: aws.String(lom.BckName()), Key: aws.String(lom.Objname)}

	headOutput, err := svc.HeadObject(input)
	if err != nil {
		err, errCode = awsErrorToAISError(err, lom.Bck(), lom.T.Snode().Name())
		return
	}
	objMeta[cmn.HeaderCloudProvider] = cmn.ProviderAmazon
	if awsIsVersionSet(headOutput.VersionId) {
		objMeta[cmn.HeaderObjVersion] = *headOutput.VersionId
	}
	size := strconv.FormatInt(*headOutput.ContentLength, 10)
	objMeta[cmn.HeaderObjSize] = size
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_object] %s", lom)
	}
	return
}

////////////////
// GET OBJECT //
////////////////

func (awsp *awsProvider) getObj(ctx context.Context, workFQN string, lom *cluster.LOM) (err error, errCode int) {
	var (
		cksum        *cmn.Cksum
		cksumToCheck *cmn.Cksum
	)
	sess := createSession(ctx)
	svc := s3.New(sess)
	obj, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(lom.BckName()),
		Key:    aws.String(lom.Objname),
	})
	if err != nil {
		err, errCode = awsErrorToAISError(err, lom.Bck(), lom.T.Snode().Name())
		return
	}
	// may not have ais metadata
	if cksumType, ok := obj.Metadata[awsChecksumType]; ok {
		if cksumValue, ok := obj.Metadata[awsChecksumVal]; ok {
			cksum = cmn.NewCksum(*cksumType, *cksumValue)
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
		t:            awsp.t,
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
		glog.Infof("[get_object] %s", lom)
	}
	return
}

////////////////
// PUT OBJECT //
////////////////

func (awsp *awsProvider) putObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	var (
		uploadOutput          *s3manager.UploadOutput
		cksumType, cksumValue = lom.Cksum().Get()
		md                    = make(map[string]*string)
	)
	md[awsChecksumType] = aws.String(cksumType)
	md[awsChecksumVal] = aws.String(cksumValue)

	uploader := s3manager.NewUploader(createSession(ctx))
	uploadOutput, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:   aws.String(lom.BckName()),
		Key:      aws.String(lom.Objname),
		Body:     r,
		Metadata: md,
	})
	if err != nil {
		err, errCode = awsErrorToAISError(err, lom.Bck(), lom.T.Snode().Name())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		if uploadOutput.VersionID != nil {
			version = *uploadOutput.VersionID
			glog.Infof("[put_object] %s, version %s", lom, version)
		} else {
			glog.Infof("[put_object] %s", lom)
		}
	}
	return
}

///////////////////
// DELETE OBJECT //
///////////////////

func (awsp *awsProvider) deleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int) {
	svc := s3.New(createSession(ctx))
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(lom.BckName()), Key: aws.String(lom.Objname)})
	if err != nil {
		err, errCode = awsErrorToAISError(err, lom.Bck(), lom.T.Snode().Name())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[delete_object] %s", lom)
	}
	return
}
