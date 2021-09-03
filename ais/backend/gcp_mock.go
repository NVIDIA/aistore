//go:build !gcp
// +build !gcp

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

func NewGCP(_ cluster.Target) (cluster.BackendProvider, error) {
	return nil, newErrInitBackend(cmn.ProviderGoogle)
}
