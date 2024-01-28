//go:build aws

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"os"
	"strings"

	s3types "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func StartMpt(lom *core.LOM) (id string, errCode int, _ error) {
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
	out, err := svc.CreateMultipartUpload(&input)
	if err == nil {
		id = *out.UploadId
	} else {
		errCode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
	}
	return id, errCode, err
}

func PutMptPart(lom *core.LOM, fh *os.File, uploadID string, partNum, size int64) (etag string, errCode int, _ error) {
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

	out, err := svc.UploadPart(&input)
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
	} else {
		etag = strings.Trim(*out.ETag, "\"")
	}

	return etag, errCode, err
}

func CompleteMpt(lom *core.LOM, uploadID string, parts *s3types.CompleteMptUpload) (etag string, errCode int, _ error) {
	var (
		cloudBck = lom.Bck().RemoteBck()
		s3parts  s3.CompletedMultipartUpload
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
	s3parts.Parts = make([]*s3.CompletedPart, 0, len(parts.Parts))
	for _, part := range parts.Parts {
		s3parts.Parts = append(s3parts.Parts, &s3.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int64(part.PartNumber),
		})
	}
	input.MultipartUpload = &s3parts

	out, err := svc.CompleteMultipartUpload(&input)
	if err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
	} else {
		etag = strings.Trim(*out.ETag, "\"")
	}

	return etag, errCode, err
}

func AbortMpt(lom *core.LOM, uploadID string) (errCode int, err error) {
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
	if _, err = svc.AbortMultipartUpload(&input); err != nil {
		errCode, err = awsErrorToAISError(err, cloudBck, lom.ObjName)
	}
	return errCode, err
}
