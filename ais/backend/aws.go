//go:build aws

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	// environment variable to globally override the default 'https://s3.amazonaws.com' endpoint
	// NOTE: the same can be done on a per-bucket basis, via bucket prop `Extra.AWS.Endpoint`
	// (bucket override will always take precedence)
	awsEnvS3Endpoint = "S3_ENDPOINT"

	// ditto non-default profile (the default is [default] in ~/.aws/credentials)
	// same NOTE in re precedence
	awsEnvConfigProfile = "AWS_PROFILE"
)

type (
	awsProvider struct {
		t core.TargetPut
	}
	sessConf struct {
		bck    *cmn.Bck
		region string
	}
)

var (
	clients    map[string]*s3.S3 // one s3.Client aka "svc" per (profile, region, endpoint) triplet
	cmu        sync.Mutex
	s3Endpoint string
	awsProfile string
)

// interface guard
var _ core.BackendProvider = (*awsProvider)(nil)

func NewAWS(t core.TargetPut) (core.BackendProvider, error) {
	clients = make(map[string]*s3.S3, 2)
	s3Endpoint = os.Getenv(awsEnvS3Endpoint)
	awsProfile = os.Getenv(awsEnvConfigProfile)
	return &awsProvider{t: t}, nil
}

// as core.BackendProvider --------------------------------------------------------------

func (*awsProvider) Provider() string { return apc.AWS }

// https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-pagination.html#cli-usage-pagination-serverside
func (*awsProvider) MaxPageSize() uint { return apc.DefaultPageSizeCloud }

//
// CREATE BUCKET
//

func (*awsProvider) CreateBucket(_ *meta.Bck) (int, error) {
	return http.StatusNotImplemented, cmn.NewErrNotImpl("create", "s3:// bucket")
}

//
// HEAD BUCKET
//

func (*awsProvider) HeadBucket(_ ctx, bck *meta.Bck) (bckProps cos.StrKVs, errCode int, err error) {
	var (
		svc      *s3.S3
		region   string
		errC     error
		cloudBck = bck.RemoteBck()
	)
	svc, region, errC = newClient(sessConf{bck: cloudBck}, "")
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("[head_bucket]", cloudBck.Name, errC)
	}
	if region == "" {
		// AWS bucket may not yet exist in the BMD -
		// get the region manually and recreate S3 client.
		if region, err = getBucketLocation(svc, cloudBck.Name); err != nil {
			errCode, err = awsErrorToAISError(err, cloudBck, "")
			return
		}
		// Create new svc with the region details.
		if svc, _, err = newClient(sessConf{region: region}, ""); err != nil {
			errCode, err = awsErrorToAISError(err, cloudBck, "")
			return
		}
	}
	region = *svc.Config.Region
	debug.Assert(region != "")

	// NOTE: return a few assorted fields, specifically to fill-in vendor-specific `cmn.ExtraProps`
	bckProps = make(cos.StrKVs, 4)
	bckProps[apc.HdrBackendProvider] = apc.AWS
	bckProps[apc.HdrS3Region] = region
	bckProps[apc.HdrS3Endpoint] = ""
	if bck.Props != nil {
		bckProps[apc.HdrS3Endpoint] = bck.Props.Extra.AWS.Endpoint
	}
	versioned, errV := getBucketVersioning(svc, cloudBck)
	if errV != nil {
		errCode, err = awsErrorToAISError(errV, cloudBck, "")
		return
	}
	bckProps[apc.HdrBucketVerEnabled] = strconv.FormatBool(versioned)
	return
}

//
// LIST OBJECTS
//

// NOTE: obtaining versioning info is extremely slow - to avoid timeouts, imposing a hard limit on the page size
const versionedPageSize = 20

func (awsp *awsProvider) ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoResult) (errCode int, err error) {
	var (
		svc        *s3.S3
		h          = cmn.BackendHelpers.Amazon
		cloudBck   = bck.RemoteBck()
		versioning bool
	)
	svc, _, err = newClient(sessConf{bck: cloudBck}, "[list_objects]")
	if err != nil && cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Warningln(err)
	}

	params := &s3.ListObjectsV2Input{Bucket: aws.String(cloudBck.Name)}
	if msg.Prefix != "" {
		params.Prefix = aws.String(msg.Prefix)
	}
	if msg.ContinuationToken != "" {
		params.ContinuationToken = aws.String(msg.ContinuationToken)
	}

	versioning = bck.Props != nil && bck.Props.Versioning.Enabled && msg.WantProp(apc.GetPropsVersion)
	msg.PageSize = calcPageSize(msg.PageSize, awsp.MaxPageSize())
	if versioning {
		msg.PageSize = min(versionedPageSize, msg.PageSize)
	}
	params.MaxKeys = aws.Int64(int64(msg.PageSize))

	resp, err := svc.ListObjectsV2(params)
	if err != nil {
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln("list_objects", cloudBck.Name, err)
		}
		errCode, err = awsErrorToAISError(err, cloudBck, "")
		return
	}

	var (
		custom = cos.StrKVs{}
		l      = len(resp.Contents)
	)
	for i := len(lst.Entries); i < l; i++ {
		lst.Entries = append(lst.Entries, &cmn.LsoEntry{}) // add missing empty
	}
	for i, key := range resp.Contents {
		entry := lst.Entries[i]
		entry.Name = *key.Key
		entry.Size = *key.Size
		if msg.IsFlagSet(apc.LsNameOnly) || msg.IsFlagSet(apc.LsNameSize) {
			continue
		}
		if v, ok := h.EncodeCksum(key.ETag); ok {
			entry.Checksum = v
		}
		if msg.WantProp(apc.GetPropsCustom) {
			custom[cmn.ETag] = entry.Checksum
			mtime := *(key.LastModified)
			custom[cmn.LastModified] = fmtTime(mtime)
			entry.Custom = cmn.CustomMD2S(custom)
		}
	}
	lst.Entries = lst.Entries[:l]

	if *resp.IsTruncated {
		lst.ContinuationToken = *resp.NextContinuationToken
	}

	if len(lst.Entries) == 0 || !versioning {
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln("[list_objects]", cloudBck.Name, len(lst.Entries))
		}
		return
	}

	// [slow path] for each already listed object:
	// - set the `ListObjectVersionsInput.Prefix` to the object's full name
	// - get the versions and lookup the latest one
	var (
		verParams = &s3.ListObjectVersionsInput{Bucket: aws.String(cloudBck.Name)}
		num       int
	)
	for _, entry := range lst.Entries {
		verParams.Prefix = aws.String(entry.Name)
		verResp, err := svc.ListObjectVersions(verParams)
		if err != nil {
			return awsErrorToAISError(err, cloudBck, "")
		}
		for _, vers := range verResp.Versions {
			if latest := *(vers.IsLatest); !latest {
				continue
			}
			if key := *(vers.Key); key == entry.Name {
				v, ok := h.EncodeVersion(vers.VersionId)
				debug.Assert(ok, entry.Name+": "+*(vers.VersionId))
				entry.Version = v
				num++
			}
		}
	}
	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infoln("[list_objects]", cloudBck.Name, len(lst.Entries), num)
	}
	return
}

//
// LIST BUCKETS
//

func (*awsProvider) ListBuckets(cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	svc, _, err := newClient(sessConf{}, "")
	if err != nil {
		errCode, err = awsErrorToAISError(err, &cmn.Bck{Provider: apc.AWS}, "")
		return
	}
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		errCode, err = awsErrorToAISError(err, &cmn.Bck{Provider: apc.AWS}, "")
		return
	}

	bcks = make(cmn.Bcks, len(result.Buckets))
	for idx, bck := range result.Buckets {
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln("[bucket_names]", aws.StringValue(bck.Name), "created", *bck.CreationDate)
		}
		bcks[idx] = cmn.Bck{
			Name:     aws.StringValue(bck.Name),
			Provider: apc.AWS,
		}
	}
	return
}

//
// HEAD OBJECT
//

func (*awsProvider) HeadObj(_ ctx, lom *core.LOM) (oa *cmn.ObjAttrs, errCode int, err error) {
	var (
		headOutput *s3.HeadObjectOutput
		svc        *s3.S3
		h          = cmn.BackendHelpers.Amazon
		cloudBck   = lom.Bck().RemoteBck()
	)
	svc, _, err = newClient(sessConf{bck: cloudBck}, "[head_object]")
	if err != nil && cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Warningln(err)
	}
	headOutput, err = svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
		return
	}
	oa = &cmn.ObjAttrs{}
	oa.CustomMD = make(cos.StrKVs, 6)
	oa.SetCustomKey(cmn.SourceObjMD, apc.AWS)
	oa.Size = *headOutput.ContentLength
	if v, ok := h.EncodeVersion(headOutput.VersionId); ok {
		lom.SetCustomKey(cmn.VersionObjMD, v)
		oa.Ver = v
	}
	if v, ok := h.EncodeCksum(headOutput.ETag); ok {
		oa.SetCustomKey(cmn.ETag, v)
		// assuming SSE-S3 or plaintext encryption
		// from https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html:
		// - "The entity tag is a hash of the object. The ETag reflects changes only
		//    to the contents of an object, not its metadata."
		// - "The ETag may or may not be an MD5 digest of the object data. Whether or
		//    not it is depends on how the object was created and how it is encrypted..."
		if !strings.Contains(v, cmn.AwsMultipartDelim) {
			oa.SetCustomKey(cmn.MD5ObjMD, v)
		}
	}

	// AIS custom (see also: PutObject, GetObjReader)
	md := headOutput.Metadata
	if cksumType, ok := md[cos.S3MetadataChecksumType]; ok {
		if cksumValue, ok := md[cos.S3MetadataChecksumVal]; ok {
			oa.SetCksum(*cksumType, *cksumValue)
		}
	}

	// unlike other custom attrs, "Content-Type" is not getting stored w/ LOM
	// - only shown via list-objects and HEAD when not present
	if v := headOutput.ContentType; v != nil {
		oa.SetCustomKey(cos.HdrContentType, *v)
	}
	if v := headOutput.LastModified; v != nil {
		mtime := *(headOutput.LastModified)
		if oa.Atime == 0 {
			oa.Atime = mtime.UnixNano()
		}
		oa.SetCustomKey(cmn.LastModified, fmtTime(mtime))
	}
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("[head_object]", cloudBck.Cname(lom.ObjName))
	}
	return
}

//
// GET OBJECT
//

func (awsp *awsProvider) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT) (int, error) {
	res := awsp.GetObjReader(ctx, lom)
	if res.Err != nil {
		return res.ErrCode, res.Err
	}
	params := allocPutParams(res, owt)
	err := awsp.t.PutObject(lom, params)
	core.FreePutParams(params)
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("[get_object]", lom.String(), err)
	}
	return 0, err
}

func (*awsProvider) GetObjReader(ctx context.Context, lom *core.LOM) (res core.GetReaderResult) {
	var (
		obj      *s3.GetObjectOutput
		cloudBck = lom.Bck().RemoteBck()
	)
	svc, _, err := newClient(sessConf{bck: cloudBck}, "[get_object]")
	if err != nil && cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Warningln(err)
	}
	obj, err = svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		res.ErrCode, res.Err = awsErrorToAISError(err, cloudBck, lom.ObjName)
		return
	}

	// custom metadata
	lom.SetCustomKey(cmn.SourceObjMD, apc.AWS)

	res.ExpCksum = _getCustom(lom, obj)

	md := obj.Metadata
	if cksumType, ok := md[cos.S3MetadataChecksumType]; ok {
		if cksumValue, ok := md[cos.S3MetadataChecksumVal]; ok {
			cksum := cos.NewCksum(*cksumType, *cksumValue)
			lom.SetCksum(cksum)
			res.ExpCksum = cksum // precedence over md5 (<= ETag)
		}
	}

	res.R = obj.Body
	res.Size = *obj.ContentLength
	return
}

func _getCustom(lom *core.LOM, obj *s3.GetObjectOutput) (md5 *cos.Cksum) {
	h := cmn.BackendHelpers.Amazon
	if v, ok := h.EncodeVersion(obj.VersionId); ok {
		lom.SetVersion(v)
		lom.SetCustomKey(cmn.VersionObjMD, v)
	}
	// see ETag/MD5 NOTE above
	if v, ok := h.EncodeCksum(obj.ETag); ok {
		lom.SetCustomKey(cmn.ETag, v)
		if !strings.Contains(v, cmn.AwsMultipartDelim) {
			md5 = cos.NewCksum(cos.ChecksumMD5, v)
			lom.SetCustomKey(cmn.MD5ObjMD, v)
		}
	}
	mtime := *(obj.LastModified)
	lom.SetCustomKey(cmn.LastModified, fmtTime(mtime))
	return
}

//
// PUT OBJECT
//

func (*awsProvider) PutObj(r io.ReadCloser, lom *core.LOM) (errCode int, err error) {
	var (
		svc                   *s3.S3
		uploadOutput          *s3manager.UploadOutput
		h                     = cmn.BackendHelpers.Amazon
		cksumType, cksumValue = lom.Checksum().Get()
		cloudBck              = lom.Bck().RemoteBck()
		md                    = make(map[string]*string, 2)
	)

	svc, _, err = newClient(sessConf{bck: cloudBck}, "[put_object]")
	if err != nil && cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Warningln(err)
	}

	md[cos.S3MetadataChecksumType] = aws.String(cksumType)
	md[cos.S3MetadataChecksumVal] = aws.String(cksumValue)

	uploader := s3manager.NewUploaderWithClient(svc)
	uploadOutput, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:   aws.String(cloudBck.Name),
		Key:      aws.String(lom.ObjName),
		Body:     r,
		Metadata: md,
	})
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
		cos.Close(r)
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
		if !strings.Contains(v, cmn.AwsMultipartDelim) {
			lom.SetCustomKey(cmn.MD5ObjMD, v)
		}
	}
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("[put_object]", lom.String())
	}
	cos.Close(r)
	return
}

//
// DELETE OBJECT
//

func (*awsProvider) DeleteObj(lom *core.LOM) (errCode int, err error) {
	var (
		svc      *s3.S3
		cloudBck = lom.Bck().RemoteBck()
	)
	svc, _, err = newClient(sessConf{bck: cloudBck}, "[delete_object]")
	if err != nil && cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Warningln(err)
	}
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("[delete_object]", lom.String())
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
// "S3 methods are safe to use concurrently. It is not safe to modify mutate
// any of the struct's properties though."
func newClient(conf sessConf, tag string) (svc *s3.S3, region string, err error) {
	var (
		endpoint = s3Endpoint
		profile  = awsProfile
	)
	region = conf.region

	if conf.bck != nil && conf.bck.Props != nil {
		if region == "" {
			region = conf.bck.Props.Extra.AWS.CloudRegion
		}
		if conf.bck.Props.Extra.AWS.Endpoint != "" {
			endpoint = conf.bck.Props.Extra.AWS.Endpoint
		}
		if conf.bck.Props.Extra.AWS.Profile != "" {
			profile = conf.bck.Props.Extra.AWS.Profile
		}
	}
	cid := _cid(profile, region, endpoint)

	// reuse
	cmu.Lock()
	svc = clients[cid]
	cmu.Unlock()
	if svc != nil {
		return
	}

	// slow path
	sess, config := _session(endpoint, profile)
	if region == "" {
		if tag != "" {
			err = fmt.Errorf("%s: unknown region for bucket %s -- proceeding with default", tag, conf.bck)
		}
		svc = s3.New(sess)
		return
	}
	// have region
	config.Region = aws.String(region)
	svc = s3.New(sess, config)
	debug.Assertf(region == *svc.Config.Region, "%s != %s", region, *svc.Config.Region)

	cmu.Lock()
	clients[cid] = svc
	cmu.Unlock()
	return
}

func _cid(profile, region, endpoint string) string {
	sb := &strings.Builder{}
	if profile != "" {
		sb.WriteString(profile)
	}
	sb.WriteByte('#')
	if region != "" {
		sb.WriteString(region)
	}
	sb.WriteByte('#')
	if endpoint != "" {
		sb.WriteString(endpoint)
	}
	return sb.String()
}

// Create session using default creds from ~/.aws/credentials and environment variables.
func _session(endpoint, profile string) (*session.Session, *aws.Config) {
	config := aws.Config{
		HTTPClient:          cmn.NewClient(cmn.TransportArgs{}),
		LowerCaseHeaderMaps: apc.Bool(true),
	}
	// `endpoint` is normally empty but could also be `Props.Extra.AWS.Endpoint` or `os.Getenv(awsEnvS3Endpoint)`
	// (with bucket-specific `Props` taking precedence)
	config.WithEndpoint(endpoint)

	opts := session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            config,
		Profile:           profile,
	}
	return session.Must(session.NewSessionWithOptions(opts)), &config
}

func getBucketVersioning(svc *s3.S3, bck *cmn.Bck) (enabled bool, errV error) {
	input := &s3.GetBucketVersioningInput{Bucket: aws.String(bck.Name)}
	result, err := svc.GetBucketVersioning(input)
	if err != nil {
		return false, err
	}
	enabled = result.Status != nil && *result.Status == s3.BucketVersioningStatusEnabled
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

func awsErrorToAISError(awsError error, bck *cmn.Bck, objName string) (int, error) {
	reqErr, ok := awsError.(awserr.RequestFailure)
	if !ok {
		return http.StatusInternalServerError, _awsErr(awsError)
	}
	awsCode, status := reqErr.Code(), reqErr.StatusCode()
	switch {
	case awsCode == s3.ErrCodeNoSuchBucket:
		return status, cmn.NewErrRemoteBckNotFound(bck)
	case awsCode == s3.ErrCodeNoSuchKey || (status == http.StatusNotFound && objName != ""):
		debug.Assert(status == http.StatusNotFound, status) // expected
		return status, errors.New("aws-error[NotFound: " + bck.Cname(objName) + "]")
	case status == http.StatusForbidden && strings.Contains(awsCode, "AllAccessDisabled"):
		// HACK: "not found or misspelled" vs  "service not paid for" (the latter less likely)
		err := _awsErr(awsError)
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln(err)
		}
		return http.StatusNotFound, err
	default:
		return status, _awsErr(awsError)
	}
}

// Original AWS error contains extra information that a caller does not need:
// status code: 400, request id: D918CB, host id: RJtDP0q8
// The extra information starts from the new line (`\n`) and tab (`\t`) of the message.
// At the same time we want to preserve original error which starts with `\ncaused by:`.
// See more `aws-sdk-go/aws/awserr/types.go:12` (`SprintError`).
func _awsErr(awsError error) error {
	var (
		msg        = awsError.Error()
		origErrMsg = awsError.Error()
	)
	// Strip extra information
	if idx := strings.Index(msg, "\n\t"); idx > 0 {
		msg = msg[:idx]
	}
	// ...but preserve original error information.
	if idx := strings.Index(origErrMsg, "\ncaused"); idx > 0 {
		// `idx+1` because we want to remove `\n`.
		msg += " (" + origErrMsg[idx+1:] + ")"
	}
	return errors.New("aws-error[" + strings.TrimSuffix(msg, ".") + "]")
}
