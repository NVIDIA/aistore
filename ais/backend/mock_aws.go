//go:build !aws

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"net/http"
	"os"

	s3types "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
)

func NewAWS(_ core.TargetPut) (core.BackendProvider, error) {
	return nil, newErrInitBackend(apc.AWS)
}

func StartMpt(*core.LOM) (string, int, error) {
	return "", http.StatusBadRequest, cmn.NewErrUnsupp("start-mpt", mock)
}

func PutMptPart(*core.LOM, *os.File, string, int64, int64) (string, int, error) {
	return "", http.StatusBadRequest, cmn.NewErrUnsupp("put-mpt-part", mock)
}

func CompleteMpt(*core.LOM, string, *s3types.CompleteMptUpload) (string, int, error) {
	return "", http.StatusBadRequest, cmn.NewErrUnsupp("complete-part", mock)
}

func AbortMpt(*core.LOM, string) (int, error) {
	return http.StatusBadRequest, cmn.NewErrUnsupp("abort-mpt", mock)
}
