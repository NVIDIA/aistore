//go:build !gcp

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core"
)

func NewGCP(_ core.TargetPut) (core.Backend, error) {
	return nil, newErrInitBackend(apc.GCP)
}
