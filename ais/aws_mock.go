// +build !aws

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

const (
	notEnabledAWS = "AWS cloud is not enabled in this build"
)

type (
	awsimpl struct { // mock
		emptyCloud
		t *targetrunner
	}
)

func newAWSProvider(t *targetrunner) *awsimpl { return &awsimpl{emptyCloud{notEnabledAWS}, t} }
