// Package s3compat provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package s3compat

const (
	AISRegion = "ais"
	AISSever  = "AIS"

	// AWS URL params
	URLParamVersioning  = "versioning"
	URLParamLifecycle   = "lifecycle"
	URLParamCORS        = "cors"
	URLParamPolicy      = "policy"
	URLParamACL         = "acl"
	URLParamMultiDelete = "delete"

	versioningEnabled  = "Enabled"
	versioningDisabled = "Suspended"

	s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01"
	// TODO: can it be omitted? // storageClass = "STANDARD"

	// Headers
	headerETag   = "ETag"
	HeaderObjSrc = "x-amz-copy-source"
)
