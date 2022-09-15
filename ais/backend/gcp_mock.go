//go:build !gcp

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
)

func NewGCP(_ cluster.Target) (cluster.BackendProvider, error) {
	return nil, newErrInitBackend(apc.GCP)
}
