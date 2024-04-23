//go:build aws

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"

	aiss3 "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func decodeXML[T any](body []byte) (result T, _ error) {
	if err := xml.Unmarshal(body, &result); err != nil {
		return result, err
	}
	return result, nil
}

func StartMpt(lom *core.LOM, oreq *http.Request, oq url.Values) (id string, ecode int, _ error) {
	if lom.IsFeatureSet(feat.PresignedS3Req) && oreq != nil {
		pts := aiss3.NewPresignedReq(oreq, lom, nil, oq)
		resp, err := pts.Do(core.T.DataClient())
		if err != nil {
			return "", resp.StatusCode, err
		}
		if resp != nil {
			result, err := decodeXML[aiss3.InitiateMptUploadResult](resp.Body)
			if err != nil {
				return "", http.StatusBadRequest, err
			}
			return result.UploadID, http.StatusOK, nil
		}
	}

	var (
		cloudBck = lom.Bck().RemoteBck()
		input    = s3.CreateMultipartUploadInput{
			Bucket: aws.String(cloudBck.Name),
			Key:    aws.String(lom.ObjName),
		}
	)
	svc, _, errN := newClient(sessConf{bck: cloudBck}, "[start_mpt]")
	if errN != nil && cmn.Rom.FastV(5, cos.SmoduleBackend) {
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

func PutMptPart(lom *core.LOM, r io.ReadCloser, oreq *http.Request, oq url.Values, uploadID string, size int64, partNum int32) (etag string,
	ecode int, _ error) {
	if lom.IsFeatureSet(feat.PresignedS3Req) && oreq != nil {
		pts := aiss3.NewPresignedReq(oreq, lom, r, oq)
		resp, err := pts.Do(core.T.DataClient())
		if err != nil {
			return "", resp.StatusCode, err
		}
		if resp != nil {
			ecode = resp.StatusCode
			etag = cmn.UnquoteCEV(resp.Header.Get(cos.HdrETag))
			return
		}
	}

	var (
		cloudBck = lom.Bck().RemoteBck()
		input    = s3.UploadPartInput{
			Bucket:        aws.String(cloudBck.Name),
			Key:           aws.String(lom.ObjName),
			Body:          r,
			UploadId:      aws.String(uploadID),
			PartNumber:    &partNum,
			ContentLength: &size,
		}
	)
	svc, _, errN := newClient(sessConf{bck: cloudBck}, "[put_mpt_part]")
	if errN != nil && cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Warningln(errN)
	}

	out, err := svc.UploadPart(context.Background(), &input)
	if err != nil {
		ecode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
	} else {
		etag = cmn.UnquoteCEV(*out.ETag)
	}

	return etag, ecode, err
}

func CompleteMpt(lom *core.LOM, oreq *http.Request, oq url.Values, uploadID string, parts *aiss3.CompleteMptUpload) (etag string,
	ecode int, _ error) {
	if lom.IsFeatureSet(feat.PresignedS3Req) && oreq != nil {
		body, err := xml.Marshal(parts)
		if err != nil {
			return "", http.StatusBadRequest, err
		}
		pts := aiss3.NewPresignedReq(oreq, lom, io.NopCloser(bytes.NewReader(body)), oq)
		resp, err := pts.Do(core.T.DataClient())
		if err != nil {
			return "", resp.StatusCode, err
		}
		if resp != nil {
			result, err := decodeXML[aiss3.CompleteMptUploadResult](resp.Body)
			if err != nil {
				return "", http.StatusBadRequest, err
			}
			etag = result.ETag
			return
		}
	}

	var (
		cloudBck = lom.Bck().RemoteBck()
		s3parts  types.CompletedMultipartUpload
		input    = s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(cloudBck.Name),
			Key:      aws.String(lom.ObjName),
			UploadId: aws.String(uploadID),
		}
	)
	svc, _, errN := newClient(sessConf{bck: cloudBck}, "[complete_mpt]")
	if errN != nil && cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Warningln(errN)
	}

	// TODO -- FIXME: reduce copying
	s3parts.Parts = make([]types.CompletedPart, 0, len(parts.Parts))
	for _, part := range parts.Parts {
		s3parts.Parts = append(s3parts.Parts, types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(part.PartNumber),
		})
	}
	input.MultipartUpload = &s3parts

	out, err := svc.CompleteMultipartUpload(context.Background(), &input)
	if err != nil {
		ecode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
	} else {
		etag = cmn.UnquoteCEV(*out.ETag)
	}

	return etag, ecode, err
}

func AbortMpt(lom *core.LOM, oreq *http.Request, oq url.Values, uploadID string) (ecode int, err error) {
	if lom.IsFeatureSet(feat.PresignedS3Req) && oreq != nil {
		pts := aiss3.NewPresignedReq(oreq, lom, oreq.Body, oq)
		resp, err := pts.Do(core.T.DataClient())
		if err != nil {
			return resp.StatusCode, err
		}
		if resp != nil {
			return resp.StatusCode, nil
		}
	}

	var (
		cloudBck = lom.Bck().RemoteBck()
		input    = s3.AbortMultipartUploadInput{
			Bucket:   aws.String(cloudBck.Name),
			Key:      aws.String(lom.ObjName),
			UploadId: aws.String(uploadID),
		}
	)
	svc, _, errN := newClient(sessConf{bck: cloudBck}, "[abort_mpt]")
	if errN != nil && cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Warningln(errN)
	}
	if _, err = svc.AbortMultipartUpload(context.Background(), &input); err != nil {
		ecode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
	}
	return ecode, err
}
