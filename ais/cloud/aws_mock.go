// +build !aws

// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import "github.com/NVIDIA/aistore/cluster"

type (
	awsProvider struct { // mock
		dummyCloudProvider
		t cluster.Target
	}
)

func NewAWS(t cluster.Target) (cluster.CloudProvider, error) {
	return &awsProvider{dummyCloudProvider{}, t}, nil
}
