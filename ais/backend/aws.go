//go:build aws

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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

	aiss3 "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tracing"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type (
	s3bp struct {
		t  core.TargetPut
		mm *memsys.MMSA
		base
	}
	sessConf struct {
		bck    *cmn.Bck
		region string
	}
)

var (
	// map[string]*s3.Client, with one s3.Client a.k.a. "svc"
	// per (profile, region, endpoint) triplet
	clients sync.Map

	s3Endpoint string
	awsProfile string
)

// interface guard
var _ core.Backend = (*s3bp)(nil)

// environment variables => static defaults that can still be overridden via bck.Props.Extra.AWS
// in addition to these two (below), default bucket region = env.AwsDefaultRegion()
func NewAWS(t core.TargetPut, tstats stats.Tracker, startingUp bool) (core.Backend, error) {
	s3Endpoint = os.Getenv(env.AWSEndpoint)
	awsProfile = os.Getenv(env.AWSProfile)
	bp := &s3bp{
		t:    t,
		mm:   t.PageMM(),
		base: base{provider: apc.AWS},
	}
	// register metrics
	bp.base.init(t.Snode(), tstats, startingUp)
	// reset clients map
	clients.Clear()
	return bp, nil
}

// as core.Backend --------------------------------------------------------------

//
// HEAD BUCKET
//

const gotBucketLocation = "got_bucket_location"

func (*s3bp) HeadBucket(_ context.Context, bck *meta.Bck) (bckProps cos.StrKVs, ecode int, _ error) {
	var (
		cloudBck = bck.RemoteBck()
		sessConf = sessConf{bck: cloudBck}
	)
	svc, err := sessConf.s3client("")
	if err != nil {
		return nil, 0, err
	}
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("[head_bucket]", cloudBck.Name)
	}
	if sessConf.region == "" {
		var region string
		if region, err = getBucketLocation(svc, cloudBck.Name); err != nil {
			ecode, err = awsErrorToAISError(err, cloudBck, "")
			return nil, ecode, err
		}
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln("get-bucket-location", cloudBck.Name, "region", region)
		}
		svc, err = sessConf.s3client(gotBucketLocation)
		debug.AssertNoErr(err)
	}

	// NOTE: return a few assorted fields, specifically to fill-in vendor-specific `cmn.ExtraProps`
	bckProps = make(cos.StrKVs, 4)
	bckProps[apc.HdrBackendProvider] = apc.AWS
	bckProps[apc.HdrS3Region] = sessConf.region
	bckProps[apc.HdrS3Endpoint] = ""
	if bck.Props != nil {
		bckProps[apc.HdrS3Endpoint] = bck.Props.Extra.AWS.Endpoint
	}
	versioned, errV := getBucketVersioning(svc, cloudBck)
	if errV != nil {
		ecode, err = awsErrorToAISError(errV, cloudBck, "")
		return nil, ecode, err
	}
	bckProps[apc.HdrBucketVerEnabled] = strconv.FormatBool(versioned)
	return bckProps, 0, nil
}

//
// LIST OBJECTS via INVENTORY
//

// when successful, returns w/ rlock held and inventory's (lom, lmfh) in the context;
// otherwise, always unlocks and frees
func (s3bp *s3bp) GetBucketInv(bck *meta.Bck, ctx *core.LsoInvCtx) (int, error) {
	debug.Assert(ctx != nil && ctx.Lom == nil)
	var (
		cloudBck = bck.RemoteBck()
		sessConf = sessConf{bck: cloudBck}
	)
	svc, err := sessConf.s3client("[get_bucket_inv]")
	if err != nil {
		return 0, err
	}

	// one bucket, one inventory, one statically defined name
	prefix, objName := aiss3.InvPrefObjname(bck.Bucket(), ctx.Name, ctx.ID)
	lom := core.AllocLOM(objName)
	if err = lom.InitBck(bck.Bucket()); err != nil {
		core.FreeLOM(lom)
		return 0, err
	}
	if !lom.TryLock(false) {
		err = cmn.NewErrBusy(invTag, lom.Cname(), "likely getting updated")
		core.FreeLOM(lom)
		return 0, err
	}

	lsV2resp, csv, manifest, ecode, err := s3bp.initInventory(cloudBck, svc, ctx, prefix)
	if err != nil {
		lom.Unlock(false)
		core.FreeLOM(lom)
		return ecode, err
	}
	ctx.Lom = lom
	mtime, usable := checkInvLom(csv.mtime, ctx)
	if usable {
		if ctx.Lmfh, err = ctx.Lom.Open(); err != nil {
			lom.Unlock(false)
			core.FreeLOM(lom)
			ctx.Lom = nil
			return 0, _errInv("usable-inv-open", err)
		}

		return 0, nil // w/ rlock
	}

	// rlock -> wlock

	lom.Unlock(false)
	err = cmn.NewErrBusy(invTag, lom.Cname(), "timed out waiting to acquire write access") // prelim
	sleep, total := time.Second, invBusyTimeout
	for total >= 0 {
		if lom.TryLock(true) {
			err = nil
			break
		}
		time.Sleep(sleep)
		total -= sleep
	}
	if err != nil {
		core.FreeLOM(lom)
		ctx.Lom = nil
		return 0, err // busy
	}

	// acquired wlock: check for write/write race

	_, _, newMtime, err := ctx.Lom.Fstat(false /*get-atime*/)
	if err == nil && newMtime.Sub(mtime) > time.Hour {
		// updated by smbd else
		// reload the lom and return
		ctx.Lom.Uncache()
		_, usable = checkInvLom(newMtime, ctx)
		debug.Assert(usable)

		// wlock --> rlock must succeed
		lom.Unlock(true)
		lom.Lock(false)

		if ctx.Lmfh, err = ctx.Lom.Open(); err != nil {
			lom.Unlock(false)
			core.FreeLOM(lom)
			ctx.Lom = nil
			return 0, _errInv("reload-inv-open", err)
		}
		return 0, nil // ok
	}

	// still under wlock: cleanup old, read and write as ctx.Lom

	cleanupOldInventory(cloudBck, svc, lsV2resp, csv, manifest)

	err = s3bp.getInventory(cloudBck, ctx, csv)

	// wlock --> rlock

	lom.Unlock(true)

	if err != nil {
		core.FreeLOM(lom)
		ctx.Lom = nil
		return 0, err
	}

	lom.Lock(false) // must succeed
	if ctx.Lmfh, err = ctx.Lom.Open(); err != nil {
		lom.Unlock(false)
		core.FreeLOM(lom)
		ctx.Lom = nil
		return 0, _errInv("get-inv-open", err)
	}

	return 0, nil // ok
}

// using local(ized) .csv
func (s3bp *s3bp) ListObjectsInv(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes, ctx *core.LsoInvCtx) (err error) {
	debug.Assert(ctx.Lom != nil && ctx.Lmfh != nil, ctx.Lom, " ", ctx.Lmfh)

	cloudBck := bck.RemoteBck()

	if ctx.SGL == nil {
		if ctx.EOF {
			debug.Assert(false) // (unlikely)
			goto none
		}
		ctx.SGL = s3bp.mm.NewSGL(invPageSGL, memsys.DefaultBuf2Size)
	} else if l := ctx.SGL.Len(); l > 0 && l < invSwapSGL && !ctx.EOF {
		// swap SGLs
		sgl := s3bp.mm.NewSGL(invPageSGL, memsys.DefaultBuf2Size)
		written, err := io.Copy(sgl, ctx.SGL) // buffering not needed - gets executed via sgl WriteTo()
		debug.AssertNoErr(err)
		debug.Assert(written == l && sgl.Len() == l, written, " vs ", l, " vs ", sgl.Len())
		ctx.SGL.Free()
		ctx.SGL = sgl
	}
	err = s3bp.listInventory(cloudBck, ctx, msg, lst)

	if err == nil || err == io.EOF {
		return nil
	}
none:
	lst.Entries = lst.Entries[:0]
	return err
}

//
// LIST OBJECTS
//

// NOTE: obtaining versioning info is extremely slow - to avoid timeouts, imposing a hard limit on the page size
const versionedPageSize = 20

func (*s3bp) ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (ecode int, _ error) {
	const tag = "list_objects"
	var (
		h          = cmn.BackendHelpers.Amazon
		cloudBck   = bck.RemoteBck()
		sessConf   = sessConf{bck: cloudBck}
		versioning bool
	)
	svc, err := sessConf.s3client(tag)
	if err != nil {
		return 0, err
	}
	params := &s3.ListObjectsV2Input{Bucket: aws.String(cloudBck.Name)}
	if msg.IsFlagSet(apc.LsNoRecursion) {
		params.Delimiter = aws.String("/")
	}
	if prefix := msg.Prefix; prefix != "" {
		params.Prefix = aws.String(prefix)
	}
	if msg.ContinuationToken != "" {
		params.ContinuationToken = aws.String(msg.ContinuationToken)
	}

	if bck.Props != nil && bck.Props.Versioning.Enabled {
		versioning = msg.WantProp(apc.GetPropsVersion) && bck.Props.Features.IsSet(feat.S3ListObjectVersions)
	}
	msg.PageSize = calcPageSize(msg.PageSize, bck.MaxPageSize())
	if versioning {
		msg.PageSize = min(versionedPageSize, msg.PageSize)
	}
	params.MaxKeys = aws.Int32(int32(msg.PageSize))

	resp, err := svc.ListObjectsV2(context.Background(), params)
	if err != nil {
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln(tag, cloudBck.Name, err)
		}
		ecode, err = awsErrorToAISError(err, cloudBck, "")
		return ecode, err
	}

	var (
		wantCustom = msg.WantProp(apc.GetPropsCustom)
	)
	lst.Entries = lst.Entries[:0]
	for _, obj := range resp.Contents {
		en := cmn.LsoEnt{Name: *obj.Key, Size: *obj.Size}
		// rarely
		if en.Size == 0 && cos.IsLastB(en.Name, '/') {
			if msg.IsFlagSet(apc.LsNoDirs) { // do not return virtual subdirectories
				continue
			}
			en.Flags = apc.EntryIsDir
		} else if !msg.IsFlagSet(apc.LsNameOnly) && !msg.IsFlagSet(apc.LsNameSize) {
			if v, ok := h.EncodeCksum(obj.ETag); ok {
				en.Checksum = v
			}
			if wantCustom {
				mtime := *(obj.LastModified)
				en.Custom = cmn.CustomProps2S(cmn.ETag, en.Checksum, cmn.LsoLastModified, fmtLsoTime(mtime))
			}
		}
		lst.Entries = append(lst.Entries, &en)
	}

	// append virtual directories unless '--no-dirs'
	if !msg.IsFlagSet(apc.LsNoDirs) {
		for _, dir := range resp.CommonPrefixes {
			lst.Entries = append(lst.Entries, &cmn.LsoEnt{Name: *dir.Prefix, Flags: apc.EntryIsDir})
		}
	}

	if *resp.IsTruncated {
		lst.ContinuationToken = *resp.NextContinuationToken
	}

	if len(lst.Entries) == 0 || !versioning {
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln(tag, cloudBck.Name, len(lst.Entries))
		}
		return 0, nil
	}

	// [slow path] for each already listed object:
	// - set the `ListObjectVersionsInput.Prefix` to the object's full name
	// - get the versions and lookup the latest one
	var (
		verParams = &s3.ListObjectVersionsInput{Bucket: aws.String(cloudBck.Name)}
		num       int
	)
	for _, en := range lst.Entries {
		verParams.Prefix = aws.String(en.Name)
		verResp, err := svc.ListObjectVersions(context.Background(), verParams)
		if err != nil {
			return awsErrorToAISError(err, cloudBck, "")
		}
		for i := range verResp.Versions {
			vers := &verResp.Versions[i]
			if latest := *(vers.IsLatest); !latest {
				continue
			}
			if key := *(vers.Key); key == en.Name {
				v, ok := h.EncodeVersion(vers.VersionId)
				debug.Assert(ok, en.Name+": "+*(vers.VersionId))
				en.Version = v
				num++
			}
		}
	}
	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infoln(tag, cloudBck.Name, len(lst.Entries), num)
	}
	return 0, nil
}

//
// LIST BUCKETS
//

func (*s3bp) ListBuckets(cmn.QueryBcks) (bcks cmn.Bcks, ecode int, _ error) {
	var (
		sessConf sessConf
		result   *s3.ListBucketsOutput
	)
	svc, err := sessConf.s3client("")
	if err != nil {
		ecode, err = awsErrorToAISError(err, &cmn.Bck{Provider: apc.AWS}, "")
		return nil, ecode, err
	}
	result, err = svc.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		ecode, err = awsErrorToAISError(err, &cmn.Bck{Provider: apc.AWS}, "")
		return nil, ecode, err
	}

	bcks = make(cmn.Bcks, len(result.Buckets))
	for idx, bck := range result.Buckets {
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln("[bucket_names]", aws.ToString(bck.Name), "created", *bck.CreationDate)
		}
		bcks[idx] = cmn.Bck{
			Name:     aws.ToString(bck.Name),
			Provider: apc.AWS,
		}
	}
	return bcks, 0, nil
}

//
// HEAD OBJECT
//

func (*s3bp) HeadObj(_ context.Context, lom *core.LOM, oreq *http.Request) (oa *cmn.ObjAttrs, ecode int, err error) {
	const tag = "[head_object]"
	var (
		svc        *s3.Client
		headOutput *s3.HeadObjectOutput
		h          = cmn.BackendHelpers.Amazon
		cloudBck   = lom.Bck().RemoteBck()
		sessConf   = sessConf{bck: cloudBck}
	)

	if lom.IsFeatureSet(feat.S3PresignedRequest) && oreq != nil {
		q := oreq.URL.Query() // TODO: optimize-out
		pts := aiss3.NewPresignedReq(oreq, lom, nil, q)
		resp, err := pts.DoHead(core.T.DataClient())
		if err != nil {
			return nil, resp.StatusCode, err
		}
		if resp != nil {
			oa = resp.ObjAttrs()
			goto exit
		}
	}

	svc, err = sessConf.s3client(tag)
	if err != nil {
		return
	}
	headOutput, err = svc.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		ecode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
		return
	}
	oa = &cmn.ObjAttrs{}
	oa.CustomMD = make(cos.StrKVs, 6)
	oa.SetCustomKey(cmn.SourceObjMD, apc.AWS)
	oa.Size = *headOutput.ContentLength
	if v, ok := h.EncodeVersion(headOutput.VersionId); ok {
		lom.SetCustomKey(cmn.VersionObjMD, v)
		oa.SetVersion(v)
	}
	if v, ok := h.EncodeETag(headOutput.ETag); ok {
		oa.SetCustomKey(cmn.ETag, v)
	}
	if v, ok := h.EncodeCksum(headOutput.ETag); ok {
		oa.SetCustomKey(cmn.MD5ObjMD, v)
	}

	// AIS custom (see also: PutObject, GetObjReader)
	if cksumType, ok := headOutput.Metadata[cos.S3MetadataChecksumType]; ok {
		if cksumValue, ok := headOutput.Metadata[cos.S3MetadataChecksumVal]; ok {
			oa.SetCksum(cksumType, cksumValue)
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
		oa.SetCustomKey(cos.HdrLastModified, fmtHdrTime(mtime))
	}

exit:
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln(tag, cloudBck.Cname(lom.ObjName))
	}
	return
}

//
// GET OBJECT
//

func (s3bp *s3bp) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT, oreq *http.Request) (int, error) {
	var res core.GetReaderResult

	if lom.IsFeatureSet(feat.S3PresignedRequest) && oreq != nil {
		q := oreq.URL.Query() // TODO: optimize-out
		pts := aiss3.NewPresignedReq(oreq, lom, nil, q)
		resp, err := pts.DoReader(core.T.DataClient())
		if err != nil {
			res = core.GetReaderResult{Err: err, ErrCode: resp.StatusCode}
			goto finalize
		}
		if resp != nil {
			res = core.GetReaderResult{
				R:       resp.BodyR,
				Size:    resp.Size,
				ErrCode: resp.StatusCode,
			}
			goto finalize
		}
	}

	res = s3bp.GetObjReader(ctx, lom, 0, 0)

finalize:
	if res.Err != nil {
		return res.ErrCode, res.Err
	}
	params := allocPutParams(res, owt)
	err := s3bp.t.PutObject(lom, params)
	core.FreePutParams(params)
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln("[get_object]", lom.String(), err)
	}
	return 0, err
}

func (*s3bp) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
	var (
		obj      *s3.GetObjectOutput
		cloudBck = lom.Bck().RemoteBck()
		sessConf = sessConf{bck: cloudBck}
		input    = s3.GetObjectInput{
			Bucket: aws.String(cloudBck.Name),
			Key:    aws.String(lom.ObjName),
		}
	)
	svc, err := sessConf.s3client("[get_obj_reader]")
	if err != nil {
		res.Err = err
		return
	}
	if length > 0 {
		rng := cmn.MakeRangeHdr(offset, length)
		input.Range = aws.String(rng)
		obj, err = svc.GetObject(ctx, &input)
		if err != nil {
			res.ErrCode, res.Err = awsErrorToAISError(err, cloudBck, lom.ObjName)
			if res.ErrCode == http.StatusRequestedRangeNotSatisfiable {
				res.Err = cmn.NewErrRangeNotSatisfiable(res.Err, nil, 0)
			}
			return res
		}
	} else {
		obj, err = svc.GetObject(ctx, &input)
		if err != nil {
			res.ErrCode, res.Err = awsErrorToAISError(err, cloudBck, lom.ObjName)
			return res
		}
		// custom metadata
		lom.SetCustomKey(cmn.SourceObjMD, apc.AWS)

		res.ExpCksum = _getCustom(lom, obj)

		md := obj.Metadata
		if cksumType, ok := md[cos.S3MetadataChecksumType]; ok {
			if cksumValue, ok := md[cos.S3MetadataChecksumVal]; ok {
				cksum := cos.NewCksum(cksumType, cksumValue)
				lom.SetCksum(cksum)
				res.ExpCksum = cksum // precedence over md5 (<= ETag)
			}
		}
	}

	res.R = obj.Body
	res.Size = *obj.ContentLength
	return res
}

func _getCustom(lom *core.LOM, obj *s3.GetObjectOutput) (md5 *cos.Cksum) {
	h := cmn.BackendHelpers.Amazon
	if v, ok := h.EncodeVersion(obj.VersionId); ok {
		lom.SetVersion(v)
		lom.SetCustomKey(cmn.VersionObjMD, v)
	}
	if v, ok := h.EncodeETag(obj.ETag); ok {
		lom.SetCustomKey(cmn.ETag, v)
	}
	if v, ok := h.EncodeCksum(obj.ETag); ok {
		md5 = cos.NewCksum(cos.ChecksumMD5, v)
		lom.SetCustomKey(cmn.MD5ObjMD, v)
	}
	for k, v := range h.EncodeMetadata(obj.Metadata) {
		lom.SetCustomKey(k, v)
	}
	mtime := *(obj.LastModified)

	// double down
	lom.SetCustomKey(cmn.LsoLastModified, fmtLsoTime(mtime))
	lom.SetCustomKey(cos.HdrLastModified, fmtHdrTime(mtime))

	return md5
}

//
// PUT OBJECT
//

func (*s3bp) PutObj(ctx context.Context, r io.ReadCloser, lom *core.LOM, oreq *http.Request) (ecode int, err error) {
	const tag = "[put_object]"
	var (
		svc                   *s3.Client
		uploader              *s3manager.Uploader
		uploadOutput          *s3manager.UploadOutput
		h                     = cmn.BackendHelpers.Amazon
		cksumType, cksumValue = lom.Checksum().Get()
		cloudBck              = lom.Bck().RemoteBck()
		sessConf              = sessConf{bck: cloudBck}
		md                    = make(map[string]string, 2)
	)
	if lom.IsFeatureSet(feat.S3PresignedRequest) && oreq != nil {
		q := oreq.URL.Query() // TODO: optimize-out
		pts := aiss3.NewPresignedReq(oreq, lom, r, q)
		resp, err := pts.Do(core.T.DataClient())
		if err != nil {
			return resp.StatusCode, err
		}
		if resp != nil {
			uploadOutput = &s3manager.UploadOutput{
				VersionID: aws.String(resp.Header.Get(cos.S3VersionHeader)),
				ETag:      aws.String(resp.Header.Get(cos.HdrETag)),
			}
			goto exit
		}
	}

	svc, err = sessConf.s3client(tag)
	if err != nil {
		return
	}

	md[cos.S3MetadataChecksumType] = cksumType
	md[cos.S3MetadataChecksumVal] = cksumValue
	if oreq != nil {
		for k, v := range cmn.BackendHelpers.Amazon.DecodeMetadata(oreq.Header) {
			md[k] = v
		}
	}

	uploader = s3manager.NewUploader(svc)
	uploader.PartSize = cos.NonZero(int64(lom.Bprops().Extra.AWS.MultiPartSize), aiss3.DefaultPartSize)
	uploadOutput, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(cloudBck.Name),
		Key:      aws.String(lom.ObjName),
		Body:     r,
		Metadata: md,
	})
	if err != nil {
		ecode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
		cos.Close(r)
		return
	}

exit:
	// compare with _getCustom() above
	if v, ok := h.EncodeVersion(uploadOutput.VersionID); ok {
		lom.SetCustomKey(cmn.VersionObjMD, v)
		lom.SetVersion(v)
	}
	if v, ok := h.EncodeETag(uploadOutput.ETag); ok {
		lom.SetCustomKey(cmn.ETag, v)
	}
	if v, ok := h.EncodeCksum(uploadOutput.ETag); ok {
		lom.SetCustomKey(cmn.MD5ObjMD, v)
	}
	if oreq != nil {
		for header := range oreq.Header {
			if strings.HasPrefix(header, aiss3.HeaderMetaPrefix) {
				lom.SetCustomKey(header, oreq.Header.Get(header))
			}
		}
	}
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln(tag, lom.String())
	}
	cos.Close(r)
	return
}

//
// DELETE OBJECT
//

// [NOTE]
// - returns (0, nil) when the object does not exist
// - `s3.DeleteObjectOutput` does not help to differentiate
// - to fight it, specify some sort of matching criteria as per:
// - https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html#ExampleVersionObjectDelete
func (*s3bp) DeleteObj(ctx context.Context, lom *core.LOM) (ecode int, err error) {
	const tag = "[delete_object]"
	var (
		svc      *s3.Client
		cloudBck = lom.Bck().RemoteBck()
		sessConf = sessConf{bck: cloudBck}
	)
	svc, err = sessConf.s3client(tag)
	if err != nil {
		return
	}
	_, err = svc.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cloudBck.Name),
		Key:    aws.String(lom.ObjName),
	})
	if err != nil {
		ecode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln(tag, lom.String())
	}
	return
}

//
// static helpers
//

// s3client creates or loads an existing S3 client for each triplet of profile/region/endpoint.
// Note that each property is configurable per-bucket.
// From S3 SDK:
// "S3 methods are safe to use concurrently. It is not safe to modify mutate
// any of the struct's properties though."
// TODO: use config.Net.HTTP.IdleConnTimeout and friends (https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/custom-http)
func (sessConf *sessConf) s3client(tag string) (*s3.Client, error) {
	var (
		endpoint = s3Endpoint
		profile  = awsProfile
	)
	if sessConf.bck != nil && sessConf.bck.Props != nil {
		if sessConf.region == "" {
			sessConf.region = sessConf.bck.Props.Extra.AWS.CloudRegion
		}
		if sessConf.bck.Props.Extra.AWS.Endpoint != "" {
			endpoint = sessConf.bck.Props.Extra.AWS.Endpoint
		}
		if sessConf.bck.Props.Extra.AWS.Profile != "" {
			profile = sessConf.bck.Props.Extra.AWS.Profile
		}
	}

	cid := _cid(profile, sessConf.region, endpoint)
	asvc, loaded := clients.Load(cid)
	if loaded {
		svc, ok := asvc.(*s3.Client)
		debug.Assert(ok)
		return svc, nil
	}

	// slow path
	cfg, err := loadConfig(endpoint, profile)
	if err != nil {
		return nil, err
	}

	svc := s3.NewFromConfig(cfg, sessConf.options)

	// NOTE:
	// - gotBucketLocation special case
	// - otherwise, not caching s3 client for an unknown or missing region
	if sessConf.region == "" && tag != gotBucketLocation {
		if tag != "" && cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Warningln(tag, "no region for bucket", sessConf.bck.Cname(""))
		}
		return svc, nil
	}

	// cache (without recomputing _cid and possibly an empty region)
	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infoln("add s3client for tuple (profile, region, endpoint):", cid)
	}
	clients.Store(cid, svc) // race or no race, no particular reason to do LoadOrStore
	return svc, nil
}

func (sessConf *sessConf) options(options *s3.Options) {
	if sessConf.region != "" {
		options.Region = sessConf.region
	} else {
		sessConf.region = options.Region
	}
	if bck := sessConf.bck; bck != nil {
		if bck.Props != nil {
			options.UsePathStyle = bck.Props.Features.IsSet(feat.S3UsePathStyle)
		} else {
			options.UsePathStyle = cmn.Rom.Features().IsSet(feat.S3UsePathStyle)
		}
	}
}

func _cid(profile, region, endpoint string) string {
	var (
		sb strings.Builder
		l  = len(profile) + 1 + len(region) + 1 + len(endpoint)
	)
	sb.Grow(l)
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

// loadConfig create config using default creds from ~/.aws/credentials and environment variables.
func loadConfig(endpoint, profile string) (aws.Config, error) {
	// NOTE: The AWS SDK for Go v2, uses lower case header maps by default.
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithHTTPClient(tracing.NewTraceableClient(cmn.NewClient(cmn.TransportArgs{}))),
		config.WithSharedConfigProfile(profile),
	)
	if err != nil {
		return cfg, err
	}
	if endpoint != "" {
		cfg.BaseEndpoint = aws.String(endpoint)
	}
	return cfg, nil
}

func getBucketVersioning(svc *s3.Client, bck *cmn.Bck) (enabled bool, errV error) {
	input := &s3.GetBucketVersioningInput{Bucket: aws.String(bck.Name)}
	result, err := svc.GetBucketVersioning(context.Background(), input)
	if err != nil {
		return false, err
	}
	enabled = result.Status == types.BucketVersioningStatusEnabled
	return
}

func getBucketLocation(svc *s3.Client, bckName string) (region string, err error) {
	resp, err := svc.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{
		Bucket: aws.String(bckName),
	})
	if err != nil {
		return
	}
	region = string(resp.LocationConstraint)
	if region == "" {
		region = env.AwsDefaultRegion() // env "AWS_REGION" or "us-east-1" - in that order
	}
	return
}

// For reference see https://github.com/aws/aws-sdk-go-v2/issues/1110#issuecomment-1054643716.
func awsErrorToAISError(awsError error, bck *cmn.Bck, objName string) (int, error) {
	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.InfoDepth(1, "begin "+aiss3.ErrPrefix+" =========================")
		nlog.InfoDepth(1, awsError)
		nlog.InfoDepth(1, "end "+aiss3.ErrPrefix+" ===========================")
	}

	var reqErr smithy.APIError
	if !errors.As(awsError, &reqErr) {
		return http.StatusInternalServerError, _awsErr(awsError, "")
	}

	code := reqErr.ErrorCode()
	switch reqErr.(type) {
	case *types.NoSuchBucket:
		return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(bck)
	case *types.NoSuchKey:
		e := fmt.Errorf("%s[%s: %s]", aiss3.ErrPrefix, reqErr.ErrorCode(), bck.Cname(objName))
		return http.StatusNotFound, e
	default:
		var rspErr *awshttp.ResponseError
		if !errors.As(awsError, &rspErr) {
			return http.StatusBadRequest, _awsErr(awsError, code)
		}
		// handle assorted status codes
		switch status := rspErr.HTTPStatusCode(); status {
		case http.StatusMovedPermanently:
			// [BUG] when bucket does not exist or isn't accessible AWS may return
			// 301 ("MovedPermanently") with code == "PermanentRedirect" which is 308
			err := cmn.NewErrRemoteBckNotFound(bck)
			err.Set(" (PermanentRedirect)")
			return http.StatusNotFound, err
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			if code == "" {
				debug.Assert(false, "empty error code in ", awsError.Error()) // (unlikely)
				code = strconv.Itoa(status)
			}
			e := fmt.Errorf("%s[%s: %s]", aiss3.ErrPrefix, code, bck.Cname(objName))
			return status, cmn.NewErrTooManyRequests(e, status)
		default:
			return status, _awsErr(awsError, code)
		}
	}
}

// Strip original AWS error to its essentials: type code and error message
// See also:
// * ais/s3/err.go WriteErr() that (NOTE) relies on the formatting below
// * aws-sdk-go/aws/awserr/types.go
func _awsErr(awsError error, code string) error {
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
	if code != "" {
		if i := strings.Index(msg, code+": "); i > 0 {
			msg = msg[i:]
		}
	}
	return errors.New(aiss3.ErrPrefix + "[" + strings.TrimSuffix(msg, ".") + "]")
}
