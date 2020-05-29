// +build aws

// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

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
	awsProvider struct {
		t cluster.Target
	}
)

var (
	_ cluster.CloudProvider = &awsProvider{}
)

func NewAWS(t cluster.Target) (cluster.CloudProvider, error) { return &awsProvider{t: t}, nil }

//======
//
// session FIXME: optimize
//
//======

// A session is created using default credentials from
// configuration file in ~/.aws/credentials and environment variables
func createSession() *session.Session {
	// TODO: avoid creating sessions for each request
	return session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
}

func awsErrorToAISError(awsError error, bck cmn.Bck, node string) (error, int) {
	if reqErr, ok := awsError.(awserr.RequestFailure); ok {
		if reqErr.Code() == s3.ErrCodeNoSuchBucket {
			return cmn.NewErrorRemoteBucketDoesNotExist(bck, node), reqErr.StatusCode()
		}
		// AWS returns confusing error when a bucket does not exist in the region, ideally we should never rely on error message
		if reqErr.StatusCode() == http.StatusMovedPermanently && strings.Contains(reqErr.Error(), "BucketRegionError") {
			return cmn.NewErrorRemoteBucketDoesNotExist(bck, node), http.StatusNotFound
		}
		return awsError, reqErr.StatusCode()
	}

	return awsError, http.StatusInternalServerError
}

func awsIsVersionSet(version *string) bool {
	return version != nil && *version != "null" && *version != ""
}

func (awsp *awsProvider) Provider() string {
	return cmn.ProviderAmazon
}

//////////////////
// LIST OBJECTS //
//////////////////

func (awsp *awsProvider) ListObjects(ctx context.Context, bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	var (
		cloudBck = bck.CloudBck()
		svc      = s3.New(createSession())
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("list_objects %s", cloudBck.Name)
	}

	params := &s3.ListObjectsInput{Bucket: aws.String(cloudBck.Name)}
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
		err, errCode = awsErrorToAISError(err, cloudBck, "")
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

		verParams := &s3.ListObjectVersionsInput{Bucket: aws.String(bck.Name)}
		if msg.Prefix != "" {
			verParams.Prefix = aws.String(msg.Prefix)
		}

		for {
			if keyMarker != "" {
				verParams.KeyMarker = aws.String(keyMarker)
			}

			verResp, err := svc.ListObjectVersions(verParams)
			if err != nil {
				err, errCode := awsErrorToAISError(err, cloudBck, "")
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

func (awsp *awsProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
	var (
		cloudBck = bck.CloudBck()
		svc      = s3.New(createSession())
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_bucket] %s", cloudBck.Name)
	}
	_, err = svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(cloudBck.Name),
	})
	if err != nil {
		err, errCode = awsErrorToAISError(err, cloudBck, "")
		return
	}

	inputVersion := &s3.GetBucketVersioningInput{Bucket: aws.String(cloudBck.Name)}
	result, err := svc.GetBucketVersioning(inputVersion)
	if err != nil {
		err, errCode = awsErrorToAISError(err, cloudBck, "")
		return
	}

	bckProps = make(cmn.SimpleKVs, 2)
	bckProps[cmn.HeaderCloudProvider] = cmn.ProviderAmazon
	bckProps[cmn.HeaderBucketVerEnabled] = strconv.FormatBool(
		result.Status != nil && *result.Status == s3.BucketVersioningStatusEnabled,
	)
	return
}

//////////////////
// BUCKET NAMES //
//////////////////

func (awsp *awsProvider) ListBuckets(ctx context.Context, _ cmn.QueryBcks) (buckets cmn.BucketNames, err error, errCode int) {
	var (
		svc = s3.New(createSession())
	)
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		err, errCode = awsErrorToAISError(err, cmn.Bck{Provider: cmn.ProviderAmazon}, "")
		return
	}

	buckets = make(cmn.BucketNames, len(result.Buckets))
	for idx, bck := range result.Buckets {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("[bucket_names] %s: created %v", aws.StringValue(bck.Name), *bck.CreationDate)
		}
		buckets[idx] = cmn.Bck{
			Name:     aws.StringValue(bck.Name),
			Provider: cmn.ProviderAmazon,
		}
	}
	return
}

////////////////
// HEAD OBJECT //
////////////////

func (awsp *awsProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	var (
		cloudBck = lom.Bck().CloudBck()
		svc      = s3.New(createSession())
		input    = &s3.HeadObjectInput{Bucket: aws.String(cloudBck.Name), Key: aws.String(lom.ObjName)}
	)

	headOutput, err := svc.HeadObject(input)
	if err != nil {
		err, errCode = awsErrorToAISError(err, cloudBck, lom.T.Snode().Name())
		return
	}
	objMeta = make(cmn.SimpleKVs, 3)
	objMeta[cmn.HeaderCloudProvider] = cmn.ProviderAmazon
	objMeta[cmn.HeaderObjSize] = strconv.FormatInt(*headOutput.ContentLength, 10)
	if awsIsVersionSet(headOutput.VersionId) {
		objMeta[cmn.HeaderObjVersion] = *headOutput.VersionId
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_object] %s", lom)
	}
	return
}

////////////////
// GET OBJECT //
////////////////

func (awsp *awsProvider) GetObj(ctx context.Context, workFQN string, lom *cluster.LOM) (err error, errCode int) {
	var (
		cksum        *cmn.Cksum
		cksumToCheck *cmn.Cksum
		bck          = lom.Bck().CloudBck()
	)
	sess := createSession()
	svc := s3.New(sess)
	obj, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		err, errCode = awsErrorToAISError(err, bck, lom.T.Snode().Name())
		return
	}
	// may not have ais metadata
	if cksumType, ok := obj.Metadata[awsChecksumType]; ok {
		if cksumValue, ok := obj.Metadata[awsChecksumVal]; ok {
			cksum = cmn.NewCksum(*cksumType, *cksumValue)
		}
	}

	customMD := cmn.SimpleKVs{
		cluster.SourceObjMD: cluster.SourceAmazonObjMD,
	}

	md5, _ := strconv.Unquote(*obj.ETag)
	// FIXME: multipart
	if !strings.Contains(md5, awsMultipartDelim) {
		cksumToCheck = cmn.NewCksum(cmn.ChecksumMD5, md5)
		customMD[cluster.AmazonMD5ObjMD] = md5
	}
	lom.SetCksum(cksum)
	if obj.VersionId != nil {
		lom.SetVersion(*obj.VersionId)
		customMD[cluster.AmazonVersionObjMD] = *obj.VersionId
	}
	lom.SetCustomMD(customMD)
	if obj.ContentLength != nil {
		setSize(ctx, *obj.ContentLength)
	}
	err = awsp.t.PutObject(cluster.PutObjectParams{
		LOM:          lom,
		Reader:       wrapReader(ctx, obj.Body),
		WorkFQN:      workFQN,
		RecvType:     cluster.ColdGet,
		Cksum:        cksumToCheck,
		WithFinalize: false,
	})
	if err != nil {
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

func (awsp *awsProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	var (
		uploadOutput          *s3manager.UploadOutput
		cksumType, cksumValue = lom.Cksum().Get()
		cloudBck              = lom.Bck().CloudBck()
		md                    = make(map[string]*string, 2)
	)
	md[awsChecksumType] = aws.String(cksumType)
	md[awsChecksumVal] = aws.String(cksumValue)

	uploader := s3manager.NewUploader(createSession())
	uploadOutput, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:   aws.String(cloudBck.Name),
		Key:      aws.String(lom.ObjName),
		Body:     r,
		Metadata: md,
	})
	if err != nil {
		err, errCode = awsErrorToAISError(err, cloudBck, lom.T.Snode().Name())
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

func (awsp *awsProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int) {
	var (
		cloudBck = lom.Bck().CloudBck()
		svc      = s3.New(createSession())
	)
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(cloudBck.Name), Key: aws.String(lom.ObjName)})
	if err != nil {
		err, errCode = awsErrorToAISError(err, cloudBck, lom.T.Snode().Name())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[delete_object] %s", lom)
	}
	return
}
