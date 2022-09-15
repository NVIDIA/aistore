// Package apc: API constants and message types
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Backend Provider enum
const (
	ProviderAIS    = "ais"
	ProviderAmazon = "aws"
	ProviderAzure  = "azure"
	ProviderGoogle = "gcp"
	ProviderHDFS   = "hdfs"
	ProviderHTTP   = "ht"

	AllProviders = "ais, aws (s3://), gcp (gs://), azure (az://), hdfs://, ht://" // NOTE: must include all

	NsUUIDPrefix = '@' // BEWARE: used by on-disk layout
	NsNamePrefix = '#' // BEWARE: used by on-disk layout

	BckProviderSeparator = "://"
	BckObjnameSeparator  = "/"

	// scheme://
	DefaultScheme = "https"
	GSScheme      = "gs"
	S3Scheme      = "s3"
	AZScheme      = "az"
	AISScheme     = "ais"
)

var Providers = cos.NewStringSet(
	ProviderAIS,
	ProviderGoogle,
	ProviderAmazon,
	ProviderAzure,
	ProviderHDFS,
	ProviderHTTP,
)

func IsProvider(p string) bool { return Providers.Contains(p) }

func ToScheme(p string) string {
	debug.Assert(IsProvider(p))
	switch p {
	case ProviderAmazon:
		return S3Scheme
	case ProviderAzure:
		return AZScheme
	case ProviderGoogle:
		return GSScheme
	default:
		return p
	}
}

func NormalizeProvider(p string) string {
	if IsProvider(p) {
		return p
	}
	switch p {
	case "":
		return ProviderAIS // NOTE: ais is the default provider
	case S3Scheme:
		return ProviderAmazon
	case AZScheme:
		return ProviderAzure
	case GSScheme:
		return ProviderGoogle
	default:
		return ""
	}
}
