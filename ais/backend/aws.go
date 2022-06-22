//go:build aws

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
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

	// environment variable to globally override the default 'https://s3.amazonaws.com' endpoint
	// NOTE: the same can be done on a per-bucket basis, via bucket prop `Extra.AWS.Endpoint`
	//       (and of course, bucket override will take precedence)
	awsEnvS3Endpoint = "S3_ENDPOINT"
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

var (
	clients    map[string]map[string]*s3.S3 // one client per (region, endpoint)
	cmu        sync.RWMutex
	s3Endpoint string
)

// interface guard
var _ cluster.BackendProvider = (*awsProvider)(nil)

func NewAWS(t cluster.Target) (cluster.BackendProvider, error) {
	clients = make(map[string]map[string]*s3.S3, 2)
	s3Endpoint = os.Getenv(awsEnvS3Endpoint)
	return &awsProvider{t: t}, nil
}

func (*awsProvider) Provider() string { return apc.ProviderAmazon }

// https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-pagination.html#cli-usage-pagination-serverside
func (*awsProvider) MaxPageSize() uint { return 1000 }

///////////////////
// CREATE BUCKET //
///////////////////

func (awsp *awsProvider) CreateBucket(_ *cluster.Bck) (errCode int, err error) {
	return creatingBucketNotSupportedErr(awsp.Provider())
}

/////////////////
// HEAD BUCKET //
/////////////////

func (*awsProvider) HeadBucket(_ ctx, bck *cluster.Bck) (bckProps cos.SimpleKVs, errCode int, err error) {
	var (
		svc      *s3.S3
		region   string
		errC     error
		cloudBck = bck.RemoteBck()
	)
	svc, region, errC = newClient(sessConf{bck: cloudBck}, "")
	if verbose {
		glog.Infof("[head_bucket] %s (%v)", cloudBck.Name, errC)
	}
	if region == "" {
		// AWS bucket may not yet exist in the BMD -
		// get the region manually and recreate S3 client.
		if region, err = getBucketLocation(svc, cloudBck.Name); err != nil {
			errCode, err = awsErrorToAISError(err, cloudBck)
			return
		}
		// Create new svc with the region details.
		if svc, _, err = newClient(sessConf{region: region}, ""); err != nil {
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
	bckProps = make(cos.SimpleKVs, 4)
	bckProps[apc.HdrBackendProvider] = apc.ProviderAmazon
	bckProps[apc.HdrS3Region] = region
	bckProps[apc.HdrS3Endpoint] = ""
	if bck.Props != nil {
		bckProps[apc.HdrS3Endpoint] = bck.Props.Extra.AWS.Endpoint
	}
	bckProps[apc.HdrBucketVerEnabled] = strconv.FormatBool(
		result.Status != nil && *result.Status == s3.BucketVersioningStatusEnabled,
	)
	return
}

//////////////////
// LIST OBJECTS //
//////////////////

func (awsp *awsProvider) ListObjects(bck *cluster.Bck, msg *apc.ListObjsMsg) (bckList *cmn.BucketList, errCode int, err error) {
	var (
		svc      *s3.S3
		h        = cmn.BackendHelpers.Amazon
		cloudBck = bck.RemoteBck()
	)
	if verbose {
		glog.Infof("list_objects %s", cloudBck.Name)
	}
	svc, _, err = newClient(sessConf{bck: cloudBck}, "[list_objects]")
	if err != nil && verbose {
		glog.Warning(err)
	}

	params := &s3.ListObjectsV2Input{Bucket: aws.String(cloudBck.Name)}
	if msg.Prefix != "" {
		params.Prefix = aws.String(msg.Prefix)
	}
	if msg.ContinuationToken != "" {
		params.ContinuationToken = aws.String(msg.ContinuationToken)
	}
	msg.PageSize = calcPageSize(msg.PageSize, awsp.MaxPageSize())
	params.MaxKeys = aws.Int64(int64(msg.PageSize))

	resp, err := svc.ListObjectsV2(params)
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck)
		return
	}

	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, len(resp.Contents))}
	for _, key := range resp.Contents {
		entry := &cmn.BucketEntry{Name: *key.Key}
		if msg.WantProp(apc.GetPropsSize) {
			entry.Size = *key.Size
		}
		if msg.WantProp(apc.GetPropsChecksum) {
			if v, ok := h.EncodeCksum(key.ETag); ok {
				entry.Checksum = v
			}
		}

		bckList.Entries = append(bckList.Entries, entry)
	}
	if verbose {
		glog.Infof("[list_objects] count %d", len(bckList.Entries))
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
	if msg.WantProp(apc.GetPropsVersion) {
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

func (*awsProvider) ListBuckets(cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	svc, _, err := newClient(sessConf{}, "")
	if err != nil {
		errCode, err = awsErrorToAISError(err, &cmn.Bck{Provider: apc.ProviderAmazon})
		return
	}
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		errCode, err = awsErrorToAISError(err, &cmn.Bck{Provider: apc.ProviderAmazon})
		return
	}

	bcks = make(cmn.Bcks, len(result.Buckets))
	for idx, bck := range result.Buckets {
		if verbose {
			glog.Infof("[bucket_names] %s: created %v", aws.StringValue(bck.Name), *bck.CreationDate)
		}
		bcks[idx] = cmn.Bck{
			Name:     aws.StringValue(bck.Name),
			Provider: apc.ProviderAmazon,
		}
	}
	return
}

/////////////////
// HEAD OBJECT //
/////////////////

func (*awsProvider) HeadObj(_ ctx, lom *cluster.LOM) (oa *cmn.ObjAttrs, errCode int, err error) {
	var (
		headOutput *s3.HeadObjectOutput
		svc        *s3.S3
		h          = cmn.BackendHelpers.Amazon
		cloudBck   = lom.Bck().RemoteBck()
	)
	svc, _, err = newClient(sessConf{bck: cloudBck}, "[head_object]")
	if err != nil && verbose {
		glog.Warning(err)
	}
	headOutput, err = svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck)
		return
	}
	oa = &cmn.ObjAttrs{}
	oa.SetCustomKey(cmn.SourceObjMD, apc.ProviderAmazon)
	oa.Size = *headOutput.ContentLength
	if v, ok := h.EncodeVersion(headOutput.VersionId); ok {
		lom.SetCustomKey(cmn.VersionObjMD, v)
		oa.Ver = v
	}
	if v, ok := h.EncodeCksum(headOutput.ETag); ok {
		oa.SetCustomKey(cmn.ETag, v)
		// NOTE: the checksum is _not_ multipart (see `h.EncodeCksum`) but still,
		//       assuming SSE-S3 or plaintext encryption - see
		//       https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
		oa.SetCustomKey(cmn.MD5ObjMD, v)
	}
	if verbose {
		glog.Infof("[head_object] %s/%s", cloudBck, lom.ObjName)
	}
	return
}

////////////////
// GET OBJECT //
////////////////

func (awsp *awsProvider) GetObj(ctx context.Context, lom *cluster.LOM, owt cmn.OWT) (errCode int, err error) {
	var (
		r        io.ReadCloser
		expCksum *cos.Cksum
	)
	r, expCksum, errCode, err = awsp.GetObjReader(ctx, lom)
	if err != nil {
		return
	}
	params := cluster.AllocPutObjParams()
	{
		params.WorkTag = fs.WorkfileColdget
		params.Reader = r
		params.OWT = owt
		params.Cksum = expCksum
		params.Atime = time.Now()
	}
	err = awsp.t.PutObject(lom, params)
	if verbose {
		glog.Infof("[get_object] %s: %v", lom, err)
	}
	return
}

////////////////////
// GET OBJ READER //
////////////////////

func (*awsProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (r io.ReadCloser, expCksum *cos.Cksum,
	errCode int, err error) {
	var (
		obj      *s3.GetObjectOutput
		svc      *s3.S3
		cloudBck = lom.Bck().RemoteBck()
	)
	svc, _, err = newClient(sessConf{bck: cloudBck}, "[get_object]")
	if err != nil && verbose {
		glog.Warning(err)
	}
	obj, err = svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck)
		return
	}

	// custom metadata
	lom.SetCustomKey(cmn.SourceObjMD, apc.ProviderAmazon)
	if cksumType, ok := obj.Metadata[awsChecksumType]; ok {
		if cksumValue, ok := obj.Metadata[awsChecksumVal]; ok {
			lom.SetCksum(cos.NewCksum(*cksumType, *cksumValue))
		}
	}
	expCksum = setCustomS3(lom, obj)

	setSize(ctx, *obj.ContentLength)
	return wrapReader(ctx, obj.Body), expCksum, 0, nil
}

func setCustomS3(lom *cluster.LOM, obj *s3.GetObjectOutput) (expCksum *cos.Cksum) {
	h := cmn.BackendHelpers.Amazon
	if v, ok := h.EncodeVersion(obj.VersionId); ok {
		lom.SetVersion(v)
		lom.SetCustomKey(cmn.VersionObjMD, v)
	}
	// see ETag/MD5 NOTE above
	if v, ok := h.EncodeCksum(obj.ETag); ok {
		expCksum = cos.NewCksum(cos.ChecksumMD5, v)
		lom.SetCustomKey(cmn.MD5ObjMD, v)
	}
	return
}

////////////////
// PUT OBJECT //
////////////////

func (*awsProvider) PutObj(r io.ReadCloser, lom *cluster.LOM) (errCode int, err error) {
	var (
		svc                   *s3.S3
		uploadOutput          *s3manager.UploadOutput
		h                     = cmn.BackendHelpers.Amazon
		cksumType, cksumValue = lom.Checksum().Get()
		cloudBck              = lom.Bck().RemoteBck()
		md                    = make(map[string]*string, 2)
	)
	defer cos.Close(r)

	svc, _, err = newClient(sessConf{bck: cloudBck}, "[put_object]")
	if err != nil && verbose {
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
	// compare with setCustomS3() above
	if v, ok := h.EncodeVersion(uploadOutput.VersionID); ok {
		lom.SetCustomKey(cmn.VersionObjMD, v)
		lom.SetVersion(v)
	}
	if v, ok := h.EncodeCksum(uploadOutput.ETag); ok {
		lom.SetCustomKey(cmn.ETag, v)
		// see ETag/MD5 NOTE above
		lom.SetCustomKey(cmn.MD5ObjMD, v)
	}
	if verbose {
		glog.Infof("[put_object] %s", lom)
	}
	return
}

///////////////////
// DELETE OBJECT //
///////////////////

func (*awsProvider) DeleteObj(lom *cluster.LOM) (errCode int, err error) {
	var (
		svc      *s3.S3
		cloudBck = lom.Bck().RemoteBck()
	)
	svc, _, err = newClient(sessConf{bck: cloudBck}, "[delete_object]")
	if err != nil && verbose {
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
	if verbose {
		glog.Infof("[delete_object] %s", lom)
	}
	return
}

//
// static helpers
//

// newClient creates new S3 client on a per-region basis or, more precisely,
// per (region, endpoint) pair - and note that s3 endpoint is per-bucket configurable.
// If the client already exists newClient simply returns it.
//
// From S3 SDK:
//     "S3 methods are safe to use concurrently. It is not safe to
//      modify mutate any of the struct's properties though."
func newClient(conf sessConf, tag string) (svc *s3.S3, region string, err error) {
	endpoint := s3Endpoint
	region = conf.region
	if conf.bck != nil && conf.bck.Props != nil {
		if region == "" {
			region = conf.bck.Props.Extra.AWS.CloudRegion
		}
		if conf.bck.Props.Extra.AWS.Endpoint != "" {
			endpoint = conf.bck.Props.Extra.AWS.Endpoint
		}
	}

	// reuse
	if region != "" {
		cmu.RLock()
		svc = clients[region][endpoint]
		cmu.RUnlock()
		if svc != nil {
			return
		}
	}
	// create
	var (
		sess    = _session(endpoint)
		awsConf = &aws.Config{}
	)
	if region == "" {
		if tag != "" {
			err = fmt.Errorf("%s: unknown region for bucket %s -- proceeding with default", tag, conf.bck)
		}
		svc = s3.New(sess)
		return
	}
	// ok
	awsConf.Region = aws.String(region)
	svc = s3.New(sess, awsConf)
	debug.Assertf(region == *svc.Config.Region, "%s != %s", region, *svc.Config.Region)

	cmu.Lock()
	eps := clients[region]
	if eps == nil {
		eps = make(map[string]*s3.S3, 1)
		clients[region] = eps
	}
	eps[endpoint] = svc
	cmu.Unlock()
	return
}

// Create session using default creds from ~/.aws/credentials and environment variables.
func _session(endpoint string) *session.Session {
	config := aws.Config{HTTPClient: cmn.NewClient(cmn.TransportArgs{})}
	config.WithEndpoint(endpoint) // normally empty but could also be `Props.Extra.AWS.Endpoint` or `os.Getenv(awsEnvS3Endpoint)`
	return session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            config,
	}))
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

func awsErrorToAISError(awsError error, bck *cmn.Bck) (int, error) {
	if reqErr, ok := awsError.(awserr.RequestFailure); ok {
		if reqErr.Code() == s3.ErrCodeNoSuchBucket {
			return reqErr.StatusCode(), cmn.NewErrRemoteBckNotFound(bck)
		}
		return reqErr.StatusCode(), cleanError(awsError)
	}

	return http.StatusInternalServerError, cleanError(awsError)
}

// Original AWS error contains extra information that a caller does not need:
//     status code: 400, request id: D918CB, host id: RJtDP0q8
// The extra information starts from the new line (`\n`) and tab (`\t`) of the message.
// At the same time we want to preserve original error which starts with `\ncaused by:`.
// See more `aws-sdk-go/aws/awserr/types.go:12` (`SprintError`).
func cleanError(awsError error) error {
	var (
		msg        = awsError.Error()
		origErrMsg = awsError.Error()
	)
	// Strip extra information...
	if idx := strings.Index(msg, "\n\t"); idx > 0 {
		msg = msg[:idx]
	}
	// ...but preserve original error information.
	if idx := strings.Index(origErrMsg, "\ncaused"); idx > 0 {
		// `idx+1` because we want to remove `\n`.
		msg += " (" + origErrMsg[idx+1:] + ")"
	}
	return errors.New(msg)
}
