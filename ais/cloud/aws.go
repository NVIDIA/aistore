// +build aws

// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	awsChecksumType = "x-amz-meta-ais-cksum-type"
	awsChecksumVal  = "x-amz-meta-ais-cksum-val"
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

func (awsp *awsProvider) awsErrorToAISError(awsError error, bck cmn.Bck) (error, int) {
	if reqErr, ok := awsError.(awserr.RequestFailure); ok {
		node := awsp.t.Snode().Name()
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

func (awsp *awsProvider) Provider() string { return cmn.ProviderAmazon }

// https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-pagination.html#cli-usage-pagination-serverside
func (awsp *awsProvider) MaxPageSize() uint { return 1000 }

//////////////////
// LIST OBJECTS //
//////////////////

func (awsp *awsProvider) ListObjects(_ context.Context, bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	msg.PageSize = calcPageSize(msg.PageSize, awsp.MaxPageSize())

	var (
		h        = cmn.CloudHelpers.Amazon
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
	if msg.ContinuationToken != "" {
		params.Marker = aws.String(msg.ContinuationToken)
	}
	params.MaxKeys = aws.Int64(int64(msg.PageSize))

	resp, err := svc.ListObjects(params)
	if err != nil {
		err, errCode = awsp.awsErrorToAISError(err, cloudBck)
		return
	}

	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, len(resp.Contents))}
	for _, key := range resp.Contents {
		entry := &cmn.BucketEntry{}
		entry.Name = *(key.Key)
		if msg.WantProp(cmn.GetPropsSize) {
			entry.Size = *(key.Size)
		}
		if msg.WantProp(cmn.GetPropsChecksum) {
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
		bckList.ContinuationToken = bckList.Entries[len(bckList.Entries)-1].Name
	}

	if len(bckList.Entries) == 0 {
		return
	}

	// if version is requested, read versions page by page and stop
	// when there is nothing to read or the version page marker is
	// greater than object page marker
	// Page is limited with 500+ items, so reading them is slow
	if msg.WantProp(cmn.GetPropsVersion) {
		versions := make(map[string]string, len(bckList.Entries))
		keyMarker := msg.ContinuationToken

		verParams := &s3.ListObjectVersionsInput{Bucket: aws.String(cloudBck.Name)}
		if msg.Prefix != "" {
			verParams.Prefix = aws.String(msg.Prefix)
		}

		for {
			if keyMarker != "" {
				verParams.KeyMarker = aws.String(keyMarker)
			}

			verResp, err := svc.ListObjectVersions(verParams)
			if err != nil {
				err, errCode := awsp.awsErrorToAISError(err, cloudBck)
				return nil, err, errCode
			}

			for _, vers := range verResp.Versions {
				if *(vers.IsLatest) {
					if v, ok := h.EncodeVersion(vers.VersionId); ok {
						versions[*(vers.Key)] = v
					}
				}
			}

			if !(*verResp.IsTruncated) {
				break
			}

			keyMarker = *verResp.NextKeyMarker
			if bckList.ContinuationToken != "" && keyMarker > bckList.ContinuationToken {
				break
			}
		}

		for _, entry := range bckList.Entries {
			if version, ok := versions[entry.Name]; ok {
				entry.Version = version
			}
		}
	}

	return
}

/////////////////
// HEAD BUCKET //
/////////////////

func (awsp *awsProvider) HeadBucket(_ context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
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
		err, errCode = awsp.awsErrorToAISError(err, cloudBck)
		return
	}

	inputVersion := &s3.GetBucketVersioningInput{Bucket: aws.String(cloudBck.Name)}
	result, err := svc.GetBucketVersioning(inputVersion)
	if err != nil {
		err, errCode = awsp.awsErrorToAISError(err, cloudBck)
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

func (awsp *awsProvider) ListBuckets(_ context.Context, _ cmn.QueryBcks) (buckets cmn.BucketNames, err error, errCode int) {
	var (
		svc = s3.New(createSession())
	)
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		err, errCode = awsp.awsErrorToAISError(err, cmn.Bck{Provider: cmn.ProviderAmazon})
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

func (awsp *awsProvider) HeadObj(_ context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	var (
		h        = cmn.CloudHelpers.Amazon
		cloudBck = lom.Bck().CloudBck()
		svc      = s3.New(createSession())
		input    = &s3.HeadObjectInput{Bucket: aws.String(cloudBck.Name), Key: aws.String(lom.ObjName)}
	)

	headOutput, err := svc.HeadObject(input)
	if err != nil {
		err, errCode = awsp.awsErrorToAISError(err, cloudBck)
		return
	}
	objMeta = make(cmn.SimpleKVs, 3)
	objMeta[cmn.HeaderCloudProvider] = cmn.ProviderAmazon
	objMeta[cmn.HeaderObjSize] = strconv.FormatInt(*headOutput.ContentLength, 10)
	if v, ok := h.EncodeVersion(headOutput.VersionId); ok {
		objMeta[cmn.HeaderObjVersion] = v
	}
	if v, ok := h.EncodeCksum(headOutput.ETag); ok {
		objMeta[cluster.MD5ObjMD] = v
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_object] %s/%s", cloudBck, lom.ObjName)
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
		h            = cmn.CloudHelpers.Amazon
		bck          = lom.Bck().CloudBck()
	)
	sess := createSession()
	svc := s3.New(sess)
	obj, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		err, errCode = awsp.awsErrorToAISError(err, bck)
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

	if v, ok := h.EncodeVersion(obj.VersionId); ok {
		lom.SetVersion(v)
		customMD[cluster.VersionObjMD] = v
	}
	if v, ok := h.EncodeCksum(obj.ETag); ok {
		cksumToCheck = cmn.NewCksum(cmn.ChecksumMD5, v)
		customMD[cluster.MD5ObjMD] = v
	}
	lom.SetCksum(cksum)
	lom.SetCustomMD(customMD)
	setSize(ctx, *obj.ContentLength)
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

func (awsp *awsProvider) PutObj(_ context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	var (
		uploadOutput          *s3manager.UploadOutput
		h                     = cmn.CloudHelpers.Amazon
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
		err, errCode = awsp.awsErrorToAISError(err, cloudBck)
		return
	}
	if v, ok := h.EncodeVersion(uploadOutput.VersionID); ok {
		version = v
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[put_object] %s, version %s", lom, version)
	}
	return
}

///////////////////
// DELETE OBJECT //
///////////////////

func (awsp *awsProvider) DeleteObj(_ context.Context, lom *cluster.LOM) (err error, errCode int) {
	var (
		cloudBck = lom.Bck().CloudBck()
		svc      = s3.New(createSession())
	)
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(cloudBck.Name), Key: aws.String(lom.ObjName)})
	if err != nil {
		err, errCode = awsp.awsErrorToAISError(err, cloudBck)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[delete_object] %s", lom)
	}
	return
}
