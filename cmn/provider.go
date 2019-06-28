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
)

// as in: mountpath/<content-type>/<CloudBs|LocalBs>/<bucket-name>/...
const (
	CloudBs = "cloud"
	LocalBs = "local"
)

var (
	// Translates the various query values for URLParamBckProvider for cluster use
	bckProviderMap = map[string]string{
		// Cloud values
		CloudBs:        CloudBs,
		ProviderAmazon: CloudBs,
		ProviderGoogle: CloudBs,

		// Local values
		LocalBs:     LocalBs,
		ProviderAIS: LocalBs,

		// unset
		"": "",
	}
)

// bucket-is-local to provider helper
func ProviderFromLoc(isLocal bool) string {
	if isLocal {
		return LocalBs
	}
	return CloudBs
}

func ProviderFromStr(provider string) (val string, err error) {
	var ok bool
	val, ok = bckProviderMap[strings.ToLower(provider)]
	if !ok {
		err = errors.New("invalid bucket provider '" + provider + "'")
	}
	return
}

func IsProviderLocal(provider string) bool {
	return provider == LocalBs || provider == ProviderAIS
}

func IsProviderCloud(provider string) bool {
	return provider == CloudBs || provider == ProviderAmazon || provider == ProviderGoogle
}

func validateCloudProvider(provider string, bckIsLocal bool) error {
	if provider != "" && provider != ProviderAmazon && provider != ProviderGoogle && provider != ProviderAIS {
		return fmt.Errorf("invalid cloud provider: %s, must be one of (%s | %s | %s)", provider,
			ProviderAmazon, ProviderGoogle, ProviderAIS)
	} else if bckIsLocal && provider != ProviderAIS && provider != "" {
		return fmt.Errorf("local bucket can only have '%s' as the cloud provider", ProviderAIS)
	}
	return nil
}
