//go:build !aws

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"io"
	"net/http"
	"net/url"

	s3types "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/stats"
)

func NewAWS(core.TargetPut, stats.Tracker, bool) (core.Backend, error) {
	return nil, &cmn.ErrInitBackend{Provider: apc.AWS}
}

func StartMptAWS(*core.LOM, *http.Request, url.Values) (string, int, error) {
	return "", http.StatusBadRequest, cmn.NewErrUnsupp("start-mpt", mock)
}

func PutMptPartAWS(*core.LOM, io.ReadCloser, *http.Request, url.Values, string, int64, int32) (string, int, error) {
	return "", http.StatusBadRequest, cmn.NewErrUnsupp("put-mpt-part", mock)
}

func CompleteMptAWS(*core.LOM, *http.Request, url.Values, string, []byte, *s3types.CompleteMptUpload) (string, string, int, error) {
	return "", "", http.StatusBadRequest, cmn.NewErrUnsupp("complete-part", mock)
}

func AbortMptAWS(*core.LOM, *http.Request, url.Values, string) (int, error) {
	return http.StatusBadRequest, cmn.NewErrUnsupp("abort-mpt", mock)
}
