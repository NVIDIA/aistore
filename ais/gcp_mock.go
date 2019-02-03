// +build !gcp

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

type (
	gcpimpl struct { // mock
		emptyCloud
		t *targetrunner
	}
)

func newGCPProvider(t *targetrunner) *gcpimpl { return &gcpimpl{emptyCloud{}, t} }
