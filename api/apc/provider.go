// Package apc: API constants and message types
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import "github.com/NVIDIA/aistore/cmn/cos"

// Backend Provider enum
const (
	ProviderAIS    = "ais"
	ProviderAmazon = "aws"
	ProviderAzure  = "azure"
	ProviderGoogle = "gcp"
	ProviderHDFS   = "hdfs"
	ProviderHTTP   = "ht"

	AllProviders = "ais, aws (s3://), gcp (gs://), azure (az://), hdfs://, ht://" // NOTE

	NsUUIDPrefix = '@' // BEWARE: used by on-disk layout
	NsNamePrefix = '#' // BEWARE: used by on-disk layout

	BckProviderSeparator = "://"
	BckObjnameSeparator  = "/"

	// Scheme parsing
	DefaultScheme = "https"
	GSScheme      = "gs"
	S3Scheme      = "s3"
	AZScheme      = "az"
	AISScheme     = "ais"
)

// + AllProviders (above)
var Providers = cos.NewStringSet(
	ProviderAIS,
	ProviderGoogle,
	ProviderAmazon,
	ProviderAzure,
	ProviderHDFS,
	ProviderHTTP,
)
