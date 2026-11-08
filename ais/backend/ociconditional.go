//go:build oci

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

	ocicmn "github.com/oracle/oci-go-sdk/v65/common"
)

// forward write conditions to OCI for evaluation by the remote store.
func (*ocibp) CondWrite(*meta.Bck) bool { return true }

// translate write conditions (see cos.ParseCondWrite) to OCI request fields;
// OCI expects bare If-Match ETag
func ociWriteConditions(r *http.Request) (match, none *string, err error) {
	if r == nil {
		return nil, nil, nil
	}
	cw, err := cos.ParseCondWrite(r.Header)
	if err != nil {
		return nil, nil, err
	}
	if cw.IfMatch != "" {
		match = &cw.IfMatch
	}
	if cw.IfNoneMatch {
		v := "*"
		none = &v
	}
	return match, none, nil
}

// preserve the native error while identifying a retryable conditional-write conflict.
func ociWriteError(op, bucket, object string, err error, response any, match, none *string) (int, error) {
	status, mapped := ociErrorToAISError(op, bucket, object, "", err, response)
	var serviceErr ocicmn.ServiceError
	if status == http.StatusConflict && (match != nil || none != nil) &&
		errors.As(err, &serviceErr) && serviceErr.GetCode() == "ConcurrentObjectUpdate" {
		wire := cmn.NewErrHTTP(nil, mapped, status)
		wire.TypeCode = cmn.TcodeCondWriteConflict
		return status, wire
	}
	return status, mapped
}
