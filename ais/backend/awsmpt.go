//go:build aws

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"bytes"
	"context"
	"io"
	"net/http"

	aiss3 "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (*s3bp) StartMpt(lom *core.LOM, oreq *http.Request) (id string, ecode int, err error) {
	if lom.IsFeatureSet(feat.S3PresignedRequest) && oreq != nil {
		pts := aiss3.NewPresignedReq(oreq, lom, nil, oreq.URL.Query())
		resp, err := pts.Do(core.T.DataClient())
		if err != nil {
			return "", resp.StatusCode, err
		}
		if resp != nil {
			result, err := aiss3.DecodeXML[aiss3.InitiateMptUploadResult](resp.Body)
			if err != nil {
				return "", http.StatusBadRequest, err
			}
			return result.UploadID, http.StatusOK, nil
		}
	}

	var metadata map[string]string
	if oreq != nil {
		metadata = cmn.BackendHelpers.Amazon.DecodeMetadata(oreq.Header)
	}

	var (
		cloudBck = lom.Bck().RemoteBck()
		sessConf = sessConf{bck: cloudBck}
		input    = s3.CreateMultipartUploadInput{
			Bucket:   aws.String(cloudBck.Name),
			Key:      aws.String(lom.ObjName),
			Metadata: metadata,
		}
	)
	svc, errN := sessConf.s3client("[start_mpt]")
	if errN != nil && cmn.Rom.V(5, cos.ModBackend) {
		nlog.Warningln(errN)
	}
	out, err := svc.CreateMultipartUpload(context.Background(), &input)
	if err == nil {
		id = *out.UploadId
	} else {
		ecode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
	}
	return id, ecode, err
}

func (*s3bp) PutMptPart(lom *core.LOM, r cos.ReadOpenCloser, oreq *http.Request, uploadID string, size int64, partNum int32) (string, int, error) {
	h := cmn.BackendHelpers.Amazon

	// presigned
	if lom.IsFeatureSet(feat.S3PresignedRequest) && oreq != nil {
		pts := aiss3.NewPresignedReq(oreq, lom, r, oreq.URL.Query())
		resp, err := pts.Do(core.T.DataClient())
		if err != nil {
			return "", _awsPresignedStatus(resp), err
		}
		if resp != nil {
			etag, _ := h.EncodeETag(resp.Header.Get(cos.HdrETag))
			return etag, resp.StatusCode, nil
		}
	}

	// regular
	var (
		cloudBck = lom.Bck().RemoteBck()
		sessConf = sessConf{bck: cloudBck}
		input    = s3.UploadPartInput{
			Bucket:        aws.String(cloudBck.Name),
			Key:           aws.String(lom.ObjName),
			Body:          r,
			UploadId:      aws.String(uploadID),
			PartNumber:    &partNum,
			ContentLength: &size,
		}
	)

	svc, errN := sessConf.s3client("[put_mpt_part]")
	if errN != nil && cmn.Rom.V(5, cos.ModBackend) {
		nlog.Warningln(errN)
	}

	// disable retries if the reader is not seekable (to avoid "failed to rewind transport stream for retry")
	var (
		_, seekable = r.(io.ReadSeeker)
		ctx         = context.Background()
		out         *s3.UploadPartOutput
		err         error
	)
	if seekable {
		out, err = svc.UploadPart(ctx, &input)
	} else {
		out, err = svc.UploadPart(ctx, &input, func(o *s3.Options) {
			o.Retryer = s3NoRetry
		})
	}

	if err != nil {
		ecode, errV := awsErrorToAISError(err, cloudBck, lom.ObjName)
		return "", ecode, errV
	}
	etag, _ := h.EncodeETag(out.ETag)
	return etag, 0, nil
}

func (*s3bp) CompleteMpt(lom *core.LOM, oreq *http.Request, uploadID string, obody []byte, parts apc.MptCompletedParts) (version, etag string, _ int, _ error) {
	h := cmn.BackendHelpers.Amazon

	// presigned
	if lom.IsFeatureSet(feat.S3PresignedRequest) && oreq != nil {
		pts := aiss3.NewPresignedReq(oreq, lom, io.NopCloser(bytes.NewReader(obody)), oreq.URL.Query())
		resp, err := pts.Do(core.T.DataClient())
		if err != nil {
			return "", "", _awsPresignedStatus(resp), err
		}
		if resp != nil {
			result, err := aiss3.DecodeXML[aiss3.CompleteMptUploadResult](resp.Body)
			if err != nil {
				return "", "", http.StatusBadRequest, err
			}
			version, _ = h.EncodeVersion(resp.Header.Get(cos.S3VersionHeader))
			etag, _ = h.EncodeETag(result.ETag)
			return version, etag, 0, nil
		}
	}

	// regular
	var (
		cloudBck = lom.Bck().RemoteBck()
		sessConf = sessConf{bck: cloudBck}
		s3parts  types.CompletedMultipartUpload
		input    = s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(cloudBck.Name),
			Key:      aws.String(lom.ObjName),
			UploadId: aws.String(uploadID),
		}
	)
	svc, errN := sessConf.s3client("[complete_mpt]")
	if errN != nil && cmn.Rom.V(5, cos.ModBackend) {
		nlog.Warningln(errN)
	}

	// Convert apc.MptCompletedParts to AWS types.CompletedPart
	s3parts.Parts = make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		pn := int32(part.PartNumber)
		s3parts.Parts[i] = types.CompletedPart{
			PartNumber: &pn,
			ETag:       &part.ETag,
		}
	}
	input.MultipartUpload = &s3parts

	out, err := svc.CompleteMultipartUpload(context.Background(), &input)
	if err != nil {
		ecode, errV := awsErrorToAISError(err, cloudBck, lom.ObjName)
		return "", "", ecode, errV
	}

	version, _ = h.EncodeVersion(out.VersionId)
	etag, _ = h.EncodeETag(out.ETag)
	return version, etag, 0, nil
}

func (*s3bp) AbortMpt(lom *core.LOM, oreq *http.Request, uploadID string) (ecode int, err error) {
	if lom.IsFeatureSet(feat.S3PresignedRequest) && oreq != nil {
		pts := aiss3.NewPresignedReq(oreq, lom, oreq.Body, oreq.URL.Query())
		resp, err := pts.Do(core.T.DataClient())
		if err != nil {
			return _awsPresignedStatus(resp), err
		}
		if resp != nil {
			return resp.StatusCode, nil
		}
	}

	var (
		cloudBck = lom.Bck().RemoteBck()
		sessConf = sessConf{bck: cloudBck}
		input    = s3.AbortMultipartUploadInput{
			Bucket:   aws.String(cloudBck.Name),
			Key:      aws.String(lom.ObjName),
			UploadId: aws.String(uploadID),
		}
	)
	svc, errN := sessConf.s3client("[abort_mpt]")
	if errN != nil && cmn.Rom.V(5, cos.ModBackend) {
		nlog.Warningln(errN)
	}
	if _, err = svc.AbortMultipartUpload(context.Background(), &input); err != nil {
		ecode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
	}
	return ecode, err
}
