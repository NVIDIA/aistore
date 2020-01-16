// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// consolidates all bucket provider related enums and functions
// see also: target.go validateBucket - it checks provider, too

import (
	"errors"
	"strings"
)

// Cloud Provider enum
const (
	Cloud = "cloud" // used only for API

	ProviderAmazon = "aws"
	ProviderGoogle = "gcp"
	ProviderAIS    = "ais"
)

var (
	Providers      = []string{ProviderAmazon, ProviderGoogle, ProviderAIS}
	CloudProviders = []string{ProviderAmazon, ProviderGoogle}
)

var (
	providerMap = map[string]string{
		Cloud:          Cloud,
		ProviderAmazon: Cloud,
		ProviderGoogle: Cloud,
		ProviderAIS:    ProviderAIS,
		"":             "",
	}
)

// TODO: should be removed after on-disk structure change
func ProviderFromStr(provider string) (val string, err error) {
	var ok bool
	val, ok = providerMap[strings.ToLower(provider)]
	if !ok {
		err = errors.New("invalid bucket provider '" + provider + "'")
	}
	return
}

func IsProviderAIS(provider string) bool {
	return provider == ProviderAIS
}

func IsProviderCloud(provider string) bool {
	return provider == Cloud || provider == ProviderAmazon || provider == ProviderGoogle
}

func IsValidProvider(provider string) bool {
	return StringInSlice(provider, Providers)
}
