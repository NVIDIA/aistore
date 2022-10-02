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
	AIS   = "ais"
	AWS   = "aws"
	Azure = "azure"
	GCP   = "gcp"
	HDFS  = "hdfs"
	HTTP  = "ht"

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

var Providers = cos.NewStrSet(AIS, GCP, AWS, Azure, HDFS, HTTP)

func IsProvider(p string) bool { return Providers.Contains(p) }

func ToScheme(p string) string {
	debug.Assert(IsProvider(p))
	switch p {
	case AWS:
		return S3Scheme
	case Azure:
		return AZScheme
	case GCP:
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
		return AIS // NOTE: ais is the default provider
	case S3Scheme:
		return AWS
	case AZScheme:
		return Azure
	case GSScheme:
		return GCP
	default:
		return ""
	}
}

func DisplayProvider(p string) string {
	switch p {
	case AIS:
		return "AIS"
	case AWS, S3Scheme:
		return "AWS"
	case Azure, AZScheme:
		return "Azure"
	case GCP, GSScheme:
		return "GCP"
	case HDFS:
		return "HDFS"
	case HTTP:
		return "HTTP(S)"
	default:
		return p
	}
}
