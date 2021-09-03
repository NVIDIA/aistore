//go:build !hdfs
// +build !hdfs

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

func NewHDFS(_ cluster.Target) (cluster.BackendProvider, error) {
	return nil, newErrInitBackend(cmn.ProviderHDFS)
}
