// +build !azure

// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"github.com/NVIDIA/aistore/cluster"
)

type (
	azureProvider struct {
		dummyCloudProvider
		t cluster.Target
	}
)

func NewAzure(t cluster.Target) (cluster.CloudProvider, error) {
	return &azureProvider{dummyCloudProvider{}, t}, nil
}
