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
	"github.com/aws/smithy-go"
)

// forward write conditions to any S3 endpoint; the endpoint decides
func (*s3bp) CondWrite(*meta.Bck) bool { return true }

// quote a single If-Match ETag; forward other forms for the endpoint to evaluate
func awsWriteConditions(r *http.Request) (match, none *string, ecode int, err error) {
	if r == nil {
		return nil, nil, 0, nil
	}
	cw, err := cos.ParseCondWrite(r.Header)
	if err != nil {
		return nil, nil, http.StatusBadRequest, err
	}
	if cw.IfMatch != "" {
		if cw.MatchETag {
			cw.IfMatch = `"` + cw.IfMatch + `"`
		}
		match = aws.String(cw.IfMatch)
	}
	if cw.IfNoneMatch != "" {
		none = aws.String(cw.IfNoneMatch)
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
			return ecode, errCondConflict(errV, ecode)
		}
	}
	return ecode, errV
}
