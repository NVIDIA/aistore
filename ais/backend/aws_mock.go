//go:build !aws

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core"
)

func NewAWS(_ core.TargetPut) (core.BackendProvider, error) {
	return nil, newErrInitBackend(apc.AWS)
}
