// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "strings"

// consolidates all bucket provider related enums and functions
// see also: target.go validateBucket - it checks provider, too

// Cloud Provider enum
const (
	Cloud = "cloud" // used only for API

	ProviderAmazon = "aws"
	ProviderGoogle = "gcp"
	ProviderAIS    = "ais"
)

var (
	Providers = map[string]struct{}{
		ProviderAIS:    {},
		ProviderGoogle: {},
		ProviderAmazon: {},
	}
	CloudProviders = []string{ProviderAmazon, ProviderGoogle}
)

func IsProviderAIS(provider string) bool {
	return provider == ProviderAIS
}

func IsProviderCloud(provider string) bool {
	return provider == Cloud || provider == ProviderAmazon || provider == ProviderGoogle
}

func IsValidProvider(provider string) bool {
	_, ok := Providers[provider]
	return ok
}

func ListProviders() string {
	keys := make([]string, 0, len(Providers))
	for k := range Providers {
		keys = append(keys, k)
	}
	return strings.Join(keys, ", ")
}
