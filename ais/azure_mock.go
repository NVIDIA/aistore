// +build !azure

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

type (
	azureProvider struct {
		emptyCloudProvider
		t *targetrunner
	}
)

func newAzureProvider(t *targetrunner) (cloudProvider, error) {
	return &azureProvider{emptyCloudProvider{}, t}, nil
}
