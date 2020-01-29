// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"strings"
)

// consolidates all bucket provider related enums and functions
// see also: target.go validateBucket - it checks provider, too

// Cloud Provider enum
const (
	Cloud = "cloud" // used only for API

	ProviderAmazon = "aws"
	ProviderGoogle = "gcp"
	ProviderAIS    = "ais"
)

// Namespace enum
const (
	NsGlobal = ""
)

type (
	Bck struct {
		Name     string `json:"name"`
		Provider string `json:"provider"`
		Ns       string `json:"namespace"`
	}
)

var (
	Providers = map[string]struct{}{
		ProviderAIS:    {},
		ProviderGoogle: {},
		ProviderAmazon: {},
	}
	CloudProviders = []string{ProviderAmazon, ProviderGoogle}
)

func (b Bck) Valid() bool {
	return ValidateBucketName(b.Name) == nil && IsValidProvider(b.Provider)
}

func (b Bck) Equal(other Bck) bool {
	return b.Name == other.Name && b.Provider == other.Provider && b.Ns == other.Ns
}

func (b Bck) String() string {
	if b.Ns == "" {
		return fmt.Sprintf("%s/%s", b.Provider, b.Name)
	}
	return fmt.Sprintf("%s/%s/%s", b.Provider, b.Ns, b.Name)
}

func IsProviderAIS(provider string) bool {
	return provider == ProviderAIS
}

func IsProviderCloud(provider string, acceptAnon bool) bool {
	return (!IsProviderAIS(provider) && IsValidProvider(provider)) || (acceptAnon && provider == Cloud)
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
