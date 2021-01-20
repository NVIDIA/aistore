// +build !hdfs

// Package cloud contains implementation of various backend providers.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

func NewHDFS(_ cluster.Target) (cluster.CloudProvider, error) {
	return nil, newInitCloudErr(cmn.ProviderHDFS)
}
