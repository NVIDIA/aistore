//go:build !hdfs

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
)

func NewHDFS(_ cluster.Target) (cluster.BackendProvider, error) {
	return nil, newErrInitBackend(apc.ProviderHDFS)
}
