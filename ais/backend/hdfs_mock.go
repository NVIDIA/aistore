//go:build !hdfs

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
)

func NewHDFS(_ cluster.TargetPut) (cluster.BackendProvider, error) {
	return nil, newErrInitBackend(apc.HDFS)
}
