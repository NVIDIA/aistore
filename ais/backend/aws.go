// +build aws

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
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
		bck    *cmn.Bck
		region string
	}
)

// interface guard
var _ cluster.BackendProvider = (*awsProvider)(nil)

func NewAWS(t cluster.Target) (cluster.BackendProvider, error) { return &awsProvider{t: t}, nil }

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
func newS3Client(conf sessConf, tag string) (svc *s3.S3, regIsSet bool, err error) {
	var (
		sess    = createSession()
		awsConf = &aws.Config{}
	)

	if conf.region != "" {
		regIsSet = true
		awsConf.Region = aws.String(conf.region)
	} else if conf.bck != nil {
		region := ""
		if conf.bck.Props != nil {
			region = conf.bck.Props.Extra.AWS.CloudRegion
		}
		if region == "" {
			if tag != "" {
				err = fmt.Errorf("%s: unknown region for bucket %s -- proceeding with default", tag, conf.bck)
			}
			svc = s3.New(sess)
			return
		}
		regIsSet = true
		awsConf.Region = aws.String(region)
	}
	svc = s3.New(sess, awsConf)
	return
}

// Original AWS error contains extra information that a caller does not need:
//     status code: 400, request id: D918CB, host id: RJtDP0q8
// The extra information starts from the new line (`\n`) and tab (`\t`) of the message.
// At the same time we want to preserve original error which starts with `\ncaused by:`.
// See more `aws-sdk-go/aws/awserr/types.go:12` (`SprintError`).
func cleanError(awsError error) error {
	var (
		msg    string
		errMsg = awsError.Error()
	)
	// Strip extra information...
	if idx := strings.Index(errMsg, "\n\t"); idx > 0 {
		msg = errMsg[:idx]
	}
	// ...but preserve original error information.
	if idx := strings.Index(errMsg, "\ncaused"); idx > 0 {
		// `idx+1` because we want to remove `\n`.
		msg += " (" + errMsg[idx+1:] + ")"
	}
	return errors.New(msg)
}

func awsErrorToAISError(awsError error, bck *cmn.Bck) (int, error) {
	if reqErr, ok := awsError.(awserr.RequestFailure); ok {
		if reqErr.Code() == s3.ErrCodeNoSuchBucket {
			return reqErr.StatusCode(), cmn.NewErrRemoteBckNotFound(*bck)
		}
		return reqErr.StatusCode(), cleanError(awsError)
	}

	return http.StatusInternalServerError, cleanError(awsError)
}

func (*awsProvider) Provider() string { return cmn.ProviderAmazon }

// https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-pagination.html#cli-usage-pagination-serverside
func (*awsProvider) MaxPageSize() uint { return 1000 }

///////////////////
// CREATE BUCKET //
///////////////////

func (awsp *awsProvider) CreateBucket(_ context.Context, _ *cluster.Bck) (errCode int, err error) {
	return creatingBucketNotSupportedErr(awsp.Provider())
}

/////////////////
// HEAD BUCKET //
/////////////////

func (*awsProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cos.SimpleKVs, errCode int, err error) {
	var (
		svc       *s3.S3
		region    string
		cloudBck  = bck.RemoteBck()
		hasRegion bool
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_bucket] %s", cloudBck.Name)
	}

	// Since AWS bucket may not yet exist in the BMD,
	// we must get the region manually and recreate S3 client.
	svc, hasRegion, _ = newS3Client(sessConf{bck: cloudBck}, "") // nolint:errcheck // on purpose
	if !hasRegion {
		if region, err = getBucketLocation(svc, cloudBck.Name); err != nil {
			errCode, err = awsErrorToAISError(err, cloudBck)
			return
		}

		// Create new svc with the region details.
		if svc, _, err = newS3Client(sessConf{region: region}, ""); err != nil {
			errCode, err = awsErrorToAISError(err, cloudBck)
			return
		}
	}

	region = *svc.Config.Region
	debug.Assert(region != "")

	inputVersion := &s3.GetBucketVersioningInput{Bucket: aws.String(cloudBck.Name)}
	result, err := svc.GetBucketVersioning(inputVersion)
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck)
		return
	}

	bckProps = make(cos.SimpleKVs, 3)
	bckProps[cmn.HdrBackendProvider] = cmn.ProviderAmazon
	bckProps[cmn.HdrCloudRegion] = region
	bckProps[cmn.HdrBucketVerEnabled] = strconv.FormatBool(
		result.Status != nil && *result.Status == s3.BucketVersioningStatusEnabled,
	)
	return
}

func getBucketLocation(svc *s3.S3, bckName string) (region string, err error) {
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

//////////////////
// LIST OBJECTS //
//////////////////

func (awsp *awsProvider) ListObjects(_ context.Context, bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList,
	errCode int, err error) {
	msg.PageSize = calcPageSize(msg.PageSize, awsp.MaxPageSize())

	var (
		svc      *s3.S3
		h        = cmn.BackendHelpers.Amazon
		cloudBck = bck.RemoteBck()
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("list_objects %s", cloudBck.Name)
	}

	svc, _, err = newS3Client(sessConf{bck: cloudBck}, "[list_objects]")
	if err != nil {
		glog.Warning(err)
	}

	params := &s3.ListObjectsV2Input{Bucket: aws.String(cloudBck.Name)}
	if msg.Prefix != "" {
		params.Prefix = aws.String(msg.Prefix)
	}
	if msg.ContinuationToken != "" {
		params.ContinuationToken = aws.String(msg.ContinuationToken)
	}
	params.MaxKeys = aws.Int64(int64(msg.PageSize))

	resp, err := svc.ListObjectsV2(params)
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck)
		return
	}

	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, len(resp.Contents))}
	for _, key := range resp.Contents {
		entry := &cmn.BucketEntry{Name: *key.Key}
		if msg.WantProp(cmn.GetPropsSize) {
			entry.Size = *key.Size
		}
		if msg.WantProp(cmn.GetPropsChecksum) {
			if v, ok := h.EncodeCksum(key.ETag); ok {
				entry.Checksum = v
			}
		}

		bckList.Entries = append(bckList.Entries, entry)
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[list_bucket] count %d", len(bckList.Entries))
	}

	if *resp.IsTruncated {
		bckList.ContinuationToken = *resp.NextContinuationToken
	}

	if len(bckList.Entries) == 0 {
		return
	}

	// If version is requested, read versions page by page and stop when there
	// is nothing to read or the version page marker is greater than object page marker.
	// Page is limited with 500+ items, so reading them is slow.
	if msg.WantProp(cmn.GetPropsVersion) {
		var (
			versions   = make(map[string]string, len(bckList.Entries))
			keyMarker  = ""
			lastMarker = bckList.Entries[len(bckList.Entries)-1].Name
		)

		verParams := &s3.ListObjectVersionsInput{Bucket: aws.String(cloudBck.Name)}
		if msg.Prefix != "" {
			verParams.Prefix = aws.String(msg.Prefix)
		}

		for keyMarker <= lastMarker {
			if keyMarker != "" {
				verParams.KeyMarker = aws.String(keyMarker)
			}

			verResp, err := svc.ListObjectVersions(verParams)
			if err != nil {
				errCode, err := awsErrorToAISError(err, cloudBck)
				return nil, errCode, err
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
		}

		for _, entry := range bckList.Entries {
			if version, ok := versions[entry.Name]; ok {
				entry.Version = version
			}
		}
	}

	return
}

//////////////////
// LIST BUCKETS //
//////////////////

func (*awsProvider) ListBuckets(_ context.Context, query cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	svc, _, err := newS3Client(sessConf{}, "")
	if err != nil {
		errCode, err = awsErrorToAISError(err, &cmn.Bck{Provider: cmn.ProviderAmazon})
		return
	}
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		errCode, err = awsErrorToAISError(err, &cmn.Bck{Provider: cmn.ProviderAmazon})
		return
	}

	bcks = make(cmn.Bcks, len(result.Buckets))
	for idx, bck := range result.Buckets {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("[bucket_names] %s: created %v", aws.StringValue(bck.Name), *bck.CreationDate)
		}
		bcks[idx] = cmn.Bck{
			Name:     aws.StringValue(bck.Name),
			Provider: cmn.ProviderAmazon,
		}
	}
	return
}

/////////////////
// HEAD OBJECT //
/////////////////

func (*awsProvider) HeadObj(_ context.Context, lom *cluster.LOM) (objMeta cos.SimpleKVs, errCode int, err error) {
	var (
		svc      *s3.S3
		h        = cmn.BackendHelpers.Amazon
		cloudBck = lom.Bck().RemoteBck()
	)
	svc, _, err = newS3Client(sessConf{bck: cloudBck}, "[head_object]")
	if err != nil {
		glog.Warning(err)
	}

	headOutput, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck)
		return
	}
	objMeta = make(cos.SimpleKVs, 3)
	objMeta[cmn.HdrBackendProvider] = cmn.ProviderAmazon
	objMeta[cmn.HdrObjSize] = strconv.FormatInt(*headOutput.ContentLength, 10)
	if v, ok := h.EncodeVersion(headOutput.VersionId); ok {
		objMeta[cmn.HdrObjVersion] = v
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

func (awsp *awsProvider) GetObj(ctx context.Context, lom *cluster.LOM) (errCode int, err error) {
	r, cksumToUse, errCode, err := awsp.GetObjReader(ctx, lom)
	if err != nil {
		return errCode, err
	}
	params := cluster.PutObjectParams{
		Tag:      fs.WorkfileColdget,
		Reader:   r,
		RecvType: cluster.ColdGet,
		Cksum:    cksumToUse,
	}
	err = awsp.t.PutObject(lom, params)
	if err != nil {
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[get_object] %s", lom)
	}
	return
}

////////////////////
// GET OBJ READER //
////////////////////

func (*awsProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (r io.ReadCloser, expectedCksm *cos.Cksum,
	errCode int, err error) {
	var (
		svc      *s3.S3
		cksum    *cos.Cksum
		h        = cmn.BackendHelpers.Amazon
		cloudBck = lom.Bck().RemoteBck()
	)
	svc, _, err = newS3Client(sessConf{bck: cloudBck}, "[get_object]")
	if err != nil {
		glog.Warning(err)
	}
	obj, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck)
		return
	}

	// Check if have custom metadata.
	if cksumType, ok := obj.Metadata[awsChecksumType]; ok {
		if cksumValue, ok := obj.Metadata[awsChecksumVal]; ok {
			cksum = cos.NewCksum(*cksumType, *cksumValue)
		}
	}

	customMD := cos.SimpleKVs{
		cluster.SourceObjMD: cluster.SourceAmazonObjMD,
	}
	if v, ok := h.EncodeVersion(obj.VersionId); ok {
		lom.SetVersion(v)
		customMD[cluster.VersionObjMD] = v
	}
	if v, ok := h.EncodeCksum(obj.ETag); ok {
		expectedCksm = cos.NewCksum(cos.ChecksumMD5, v)
		customMD[cluster.MD5ObjMD] = v
	}
	lom.SetCksum(cksum)
	lom.SetCustom(customMD)
	setSize(ctx, *obj.ContentLength)
	return wrapReader(ctx, obj.Body), expectedCksm, 0, nil
}

////////////////
// PUT OBJECT //
////////////////

func (*awsProvider) PutObj(ctx context.Context, r io.ReadCloser, lom *cluster.LOM) (version string, errCode int, err error) {
	var (
		svc                   *s3.S3
		uploadOutput          *s3manager.UploadOutput
		h                     = cmn.BackendHelpers.Amazon
		cksumType, cksumValue = lom.Checksum().Get()
		cloudBck              = lom.Bck().RemoteBck()
		md                    = make(map[string]*string, 2)
	)
	defer cos.Close(r)

	svc, _, err = newS3Client(sessConf{bck: cloudBck}, "[put_object]")
	if err != nil {
		glog.Warning(err)
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
		errCode, err = awsErrorToAISError(err, cloudBck)
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

func (*awsProvider) DeleteObj(_ context.Context, lom *cluster.LOM) (errCode int, err error) {
	var (
		svc      *s3.S3
		cloudBck = lom.Bck().RemoteBck()
	)
	svc, _, err = newS3Client(sessConf{bck: cloudBck}, "[delete_object]")
	if err != nil {
		glog.Warning(err)
	}

	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[delete_object] %s", lom)
	}
	return
}
