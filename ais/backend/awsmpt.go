//go:build aws

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"os"

	s3types "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func StartMpt(lom *core.LOM) (id string, ecode int, _ error) {
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

func PutMptPart(lom *core.LOM, fh *os.File, uploadID string, partNum int32, size int64) (etag string, ecode int, _ error) {
	var (
		cloudBck = lom.Bck().RemoteBck()
		input    = s3.UploadPartInput{
			Bucket:        aws.String(cloudBck.Name),
			Key:           aws.String(lom.ObjName),
			Body:          fh,
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

func CompleteMpt(lom *core.LOM, uploadID string, parts *s3types.CompleteMptUpload) (etag string, ecode int, _ error) {
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

func AbortMpt(lom *core.LOM, uploadID string) (ecode int, err error) {
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
