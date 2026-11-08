//go:build aws

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"errors"
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// forward write conditions to the SDK's default S3 endpoint
func (*s3bp) CondWrite(bck *meta.Bck) bool {
	if bck == nil {
		return false
	}
	svc, err := getS3Svc(bck.RemoteBck(), "")
	return err == nil && awsSupportsCondWrite(svc)
}

func awsSupportsCondWrite(svc *s3.Client) bool {
	return aws.ToString(svc.Options().BaseEndpoint) == ""
}

// return bucket-configured endpoint, if any, or the default (see env.AWSEndpoint)
func awsEndpoint(bck *cmn.Bck) string {
	if bck != nil && bck.Props != nil && bck.Props.Extra.AWS.Endpoint != "" {
		return bck.Props.Extra.AWS.Endpoint
	}
	return s3Endpoint
}

// translate write conditions (see cos.ParseCondWrite) to S3 request fields;
// S3 expects quoted If-Match ETag
func awsWriteConditions(svc *s3.Client, r *http.Request) (match, none *string, ecode int, err error) {
	if r == nil || !cos.IsCondWrite(r.Header) {
		return nil, nil, 0, nil
	}
	if !awsSupportsCondWrite(svc) {
		return nil, nil, http.StatusNotImplemented, cmn.NewErrUnsupp("conditionally write to", "this S3 endpoint")
	}
	cw, err := cos.ParseCondWrite(r.Header)
	if err != nil {
		return nil, nil, http.StatusBadRequest, err
	}
	if cw.IfMatch != "" {
		match = aws.String(cmn.QuoteETag(cw.IfMatch))
	}
	if cw.IfNoneMatch {
		none = aws.String("*")
	}
	return match, none, 0, nil
}

// identify rejected (412) and conflicting (409) conditional writes
func awsWriteError(err error, bck *cmn.Bck, objName string, cond bool, details ...string) (int, error) {
	ecode, errV := awsErrorToAISError(err, bck, objName, details...)
	if !cond {
		return ecode, errV
	}
	switch ecode {
	case http.StatusPreconditionFailed:
		return ecode, cos.NewErrPreconditionFailed(errV)
	case http.StatusConflict:
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == cmn.TcodeCondWriteConflict {
			herr := cmn.NewErrHTTP(nil, errV, ecode)
			herr.TypeCode = cmn.TcodeCondWriteConflict
			return ecode, herr
		}
	}
	return ecode, errV
}
