// +build !aws

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

type (
	awsProvider struct { // mock
		emptyCloudProvider
		t *targetrunner
	}
)

func newAWSProvider(t *targetrunner) (cloudProvider, error) {
	return &awsProvider{emptyCloudProvider{}, t}, nil
}
