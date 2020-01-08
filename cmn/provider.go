// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// consolidates all bucket provider related enums and functions
// see also: target.go validateBucket - it checks provider, too

import (
	"errors"
	"fmt"
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
	Providers = []string{ProviderAmazon, ProviderGoogle, ProviderAIS}
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

func ProviderFromBool(isais bool) string {
	if isais {
		return AIS
	}
	return Cloud
}

func ProviderFromStr(provider string) (val string, err error) {
	var ok bool
	val, ok = providerMap[strings.ToLower(provider)]
	if !ok {
		err = errors.New("invalid bucket provider '" + provider + "'")
	}
	return
}

func IsProviderAIS(provider string) bool {
	return provider == AIS || provider == ProviderAIS
}

func IsProviderCloud(provider string) bool {
	return provider == Cloud || provider == ProviderAmazon || provider == ProviderGoogle
}

func validateCloudProvider(provider string, bckIsAIS bool) error {
	if provider != "" && provider != ProviderAmazon && provider != ProviderGoogle && provider != ProviderAIS {
		return fmt.Errorf("invalid cloud provider: %s, must be one of (%s | %s | %s)", provider,
			ProviderAmazon, ProviderGoogle, ProviderAIS)
	} else if bckIsAIS && provider != ProviderAIS && provider != "" {
		return fmt.Errorf("ais bucket can only have '%s' as the cloud provider", ProviderAIS)
	}
	return nil
}
