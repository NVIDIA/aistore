//go:build !oci

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"net/http"
	"net/url"

	s3types "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/stats"
)

func NewOCI(core.TargetPut, stats.Tracker, bool) (core.Backend, error) {
	return nil, &cmn.ErrInitBackend{Provider: apc.OCI}
}

func StartMptOCI(core.Backend, *core.LOM, *http.Request, url.Values) (string, int, error) {
	return "", http.StatusBadRequest, cmn.NewErrUnsupp("start-mpt", mock)
}

func PutMptPartOCI(core.Backend, *core.LOM, cos.ReadOpenCloser, *http.Request, url.Values, string, int64, int32) (string, int, error) {
	return "", http.StatusBadRequest, cmn.NewErrUnsupp("put-mpt-part", mock)
}

func CompleteMptOCI(core.Backend, *core.LOM, *http.Request, url.Values, string, []byte, *s3types.CompleteMptUpload) (string, string, int, error) {
	return "", "", http.StatusBadRequest, cmn.NewErrUnsupp("complete-mpt", mock)
}

func AbortMptOCI(core.Backend, *core.LOM, *http.Request, url.Values, string) (int, error) {
	return http.StatusBadRequest, cmn.NewErrUnsupp("abort-mpt", mock)
}
