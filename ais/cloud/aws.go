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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
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

	sessConf struct {
		bck    *cluster.Bck
		region string
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
		Config:            aws.Config{HTTPClient: cmn.NewClient(cmn.TransportArgs{})},
	}))
}

// newS3Client creates new S3 client that can be used to make requests. It is
// guaranteed that the client is initialized even in case of errors.
func (awsp *awsProvider) newS3Client(conf sessConf) (svc *s3.S3, regIsSet bool) {
	var (
		sess    = createSession()
		awsConf = &aws.Config{}
	)

	if conf.region != "" {
		awsConf.Region = aws.String(conf.region)
		regIsSet = true
	} else if conf.bck != nil {
		if conf.bck.Props == nil || conf.bck.Props.Extra.CloudRegion == "" {
			return s3.New(sess), regIsSet
		}
		debug.Assert(conf.bck.Props.Extra.CloudRegion != "")
		regIsSet = true
		awsConf.Region = aws.String(conf.bck.Props.Extra.CloudRegion)
	}
	svc = s3.New(sess, awsConf)
	return
}

func (awsp *awsProvider) awsErrorToAISError(awsError error, bck *cluster.Bck) (error, int) {
	if reqErr, ok := awsError.(awserr.RequestFailure); ok {
		node := awsp.t.Snode().Name()
		if reqErr.Code() == s3.ErrCodeNoSuchBucket {
			return cmn.NewErrorRemoteBucketDoesNotExist(bck.Bck, node), reqErr.StatusCode()
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
		hasRegion bool
		svc       *s3.S3
		h         = cmn.CloudHelpers.Amazon
		cloudBck  = bck.BackendBck()
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("list_objects %s", cloudBck.Name)
	}

	svc, hasRegion = awsp.newS3Client(sessConf{bck: cloudBck})
	if !hasRegion {
		glog.Warningf("[list_objects]: missing cloud region props for bucket %v -- proceeding with default", cloudBck.Bck)
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

func (awsp *awsProvider) getBucketLocation(svc *s3.S3, bckName string) (region string, err error) {
	resp, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(bckName),
	})
	if err != nil {
		return
	}
	region = aws.StringValue(resp.LocationConstraint)

	// NOTE: AWS API returns empty region "only" for 'us-east-1`
	if region == "" {
		region = endpoints.UsEast1RegionID
	}
	return
}

func (awsp *awsProvider) HeadBucket(_ context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
	var (
		svc       *s3.S3
		region    string
		hasRegion bool
		cloudBck  = bck.BackendBck()
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_bucket] %s", cloudBck.Name)
	}

	// Since it's possible that the cloud bucket may not yet exist in the BMD,
	// we must get the region manually and recreate S3 client.
	svc, hasRegion = awsp.newS3Client(sessConf{bck: cloudBck})
	if !hasRegion {
		if region, err = awsp.getBucketLocation(svc, cloudBck.Name); err != nil {
			err, errCode = awsp.awsErrorToAISError(err, cloudBck)
			return
		}

		// Create new svc with the region details.
		svc, _ = awsp.newS3Client(sessConf{region: region})
	}

	region = *svc.Config.Region
	debug.Assert(region != "")

	inputVersion := &s3.GetBucketVersioningInput{Bucket: aws.String(cloudBck.Name)}
	result, err := svc.GetBucketVersioning(inputVersion)
	if err != nil {
		err, errCode = awsp.awsErrorToAISError(err, cloudBck)
		return
	}

	bckProps = make(cmn.SimpleKVs, 3)
	bckProps[cmn.HeaderCloudProvider] = cmn.ProviderAmazon
	bckProps[cmn.HeaderCloudRegion] = region
	bckProps[cmn.HeaderBucketVerEnabled] = strconv.FormatBool(
		result.Status != nil && *result.Status == s3.BucketVersioningStatusEnabled,
	)
	return
}

//////////////////
// BUCKET NAMES //
//////////////////

func (awsp *awsProvider) ListBuckets(_ context.Context, _ cmn.QueryBcks) (buckets cmn.BucketNames, err error, errCode int) {
	svc, _ := awsp.newS3Client(sessConf{})
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		err, errCode = awsp.awsErrorToAISError(err, cluster.NewBck("", cmn.ProviderAmazon, cmn.NsGlobal))
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
		svc       *s3.S3
		hasRegion bool
		h         = cmn.CloudHelpers.Amazon
		cloudBck  = lom.Bck().BackendBck()
	)
	svc, hasRegion = awsp.newS3Client(sessConf{bck: cloudBck})
	if !hasRegion {
		glog.Warningf("[head_object]: missing cloud region props for bucket %v -- proceeding with default", cloudBck.Bck)
	}

	headOutput, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
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
		svc          *s3.S3
		cksum        *cmn.Cksum
		cksumToCheck *cmn.Cksum
		hasRegion    bool
		h            = cmn.CloudHelpers.Amazon
		cloudBck     = lom.Bck().BackendBck()
	)

	svc, hasRegion = awsp.newS3Client(sessConf{bck: cloudBck})
	if !hasRegion {
		glog.Warningf("[get_object]: missing cloud region props for bucket %v -- proceeding with default", cloudBck.Bck)
	}

	obj, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		err, errCode = awsp.awsErrorToAISError(err, cloudBck)
		return
	}

	// Check if have custom metadata.
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
		svc                   *s3.S3
		uploadOutput          *s3manager.UploadOutput
		hasRegion             bool
		h                     = cmn.CloudHelpers.Amazon
		cksumType, cksumValue = lom.Cksum().Get()
		cloudBck              = lom.Bck().BackendBck()
		md                    = make(map[string]*string, 2)
	)

	svc, hasRegion = awsp.newS3Client(sessConf{bck: cloudBck})
	if !hasRegion {
		glog.Warningf("[put_object]: missing cloud region props for bucket %v -- proceeding with default", cloudBck.Bck)
	}

	md[awsChecksumType] = aws.String(cksumType)
	md[awsChecksumVal] = aws.String(cksumValue)

	uploader := s3manager.NewUploaderWithClient(svc)
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
		svc       *s3.S3
		hasRegion bool
		cloudBck  = lom.Bck().BackendBck()
	)
	svc, hasRegion = awsp.newS3Client(sessConf{bck: cloudBck})
	if !hasRegion {
		glog.Warningf("[delete_object]: missing cloud region props for bucket %v -- proceeding with default", cloudBck.Bck)
	}

	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		err, errCode = awsp.awsErrorToAISError(err, cloudBck)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[delete_object] %s", lom)
	}
	return
}
