// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "strings"

const AwsDefaultRegion = "us-east-1"

// from https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
// "The ETag may or may not be an MD5 digest of the object data. Whether or
// not it is depends on how the object was created and how it is encrypted..."
const AwsMultipartDelim = "-"

func IsS3MultipartEtag(etag string) bool {
	return strings.Contains(etag, AwsMultipartDelim)
}
