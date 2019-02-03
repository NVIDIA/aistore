// +build !aws

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

type (
	awsimpl struct { // mock
		emptyCloud
		t *targetrunner
	}
)

func newAWSProvider(t *targetrunner) *awsimpl { return &awsimpl{emptyCloud{}, t} }
