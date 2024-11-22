// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Backend Provider enum
const (
	AIS   = "ais"
	AWS   = "aws"
	Azure = "azure"
	GCP   = "gcp"
	OCI   = "oci"
	HT    = "ht"

	AllProviders = "ais, aws (s3://), gcp (gs://), azure (az://), oci (oc://), ht://" // NOTE: must include all

	NsUUIDPrefix = '@' // BEWARE: used by on-disk layout
	NsNamePrefix = '#' // BEWARE: used by on-disk layout

	// consistent with rfc2396.txt "Uniform Resource Identifiers (URI): Generic Syntax"
	BckProviderSeparator = "://"

	// scheme://
	DefaultScheme = "https"
	OCIScheme     = "oc"
	GSScheme      = "gs"
	S3Scheme      = "s3"
	AZScheme      = "az"
	AISScheme     = "ais"
)

const RemAIS = "remais" // to differentiate ais vs "remote" ais; also, default (remote ais cluster) alias

var Providers = cos.NewStrSet(AIS, GCP, AWS, Azure, OCI, HT)

func IsProvider(p string) bool { return Providers.Contains(p) }

func IsCloudProvider(p string) bool {
	return p == AWS || p == GCP || p == Azure || p == OCI
}

// NOTE: not to confuse w/ bck.IsRemote() which also includes remote AIS
func IsRemoteProvider(p string) bool {
	return IsCloudProvider(p) || p == HT
}

func ToScheme(p string) string {
	switch p {
	case AWS:
		return S3Scheme
	case Azure:
		return AZScheme
	case GCP:
		return GSScheme
	case OCI:
		return OCIScheme
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
	case OCIScheme:
		return OCI
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
	case OCI, OCIScheme:
		return "OCI"
	case HT:
		return "HTTP(S)"
	default:
		return p
	}
}
