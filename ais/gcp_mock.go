// +build !gcp

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

type (
	gcpProvider struct {
		emptyCloudProvider
		t *targetrunner
	}
)

func newGCPProvider(t *targetrunner) *gcpProvider { return &gcpProvider{emptyCloudProvider{}, t} }
