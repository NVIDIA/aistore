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
	ProviderAmazon = "aws"
	ProviderGoogle = "gcp"
	ProviderAIS    = "ais"
	// maps to one of:
	Cloud = "cloud"
	AIS   = ProviderAIS
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
		ProviderAIS:    AIS,
		"":             "",
	}
)

func ProviderFromStr(provider string) (val string, err error) {
	var ok bool
	val, ok = providerMap[strings.ToLower(provider)]
	if !ok {
		err = errors.New("invalid bucket provider '" + provider + "'")
	}
	return
}

// TODO: should only accept `ProviderAIS`
func IsProviderAIS(provider string) bool {
	return provider == AIS || provider == ProviderAIS
}

// TODO: should only accept `ProviderAmazon` and `ProviderGoogle` (CloudProviders)
func IsProviderCloud(provider string) bool {
	return provider == Cloud || provider == ProviderAmazon || provider == ProviderGoogle
}

func IsValidProvider(provider string) bool {
	return StringInSlice(provider, Providers)
}
