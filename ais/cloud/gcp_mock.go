// +build !gcp

// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

func NewGCP(_ cluster.Target) (cluster.CloudProvider, error) {
	return nil, newInitCloudErr(cmn.ProviderGoogle)
}
