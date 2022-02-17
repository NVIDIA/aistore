// Package s3compat provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package s3compat

const (
	AISRegion = "ais"
	AISSever  = "AIS"

	// AWS URL params
	QparamVersioning  = "versioning"
	QparamLifecycle   = "lifecycle"
	QparamCORS        = "cors"
	QparamPolicy      = "policy"
	QparamACL         = "acl"
	QparamMultiDelete = "delete"

	versioningEnabled  = "Enabled"
	versioningDisabled = "Suspended"

	s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01"

	// Headers
	headerETag   = "ETag"
	HeaderObjSrc = "x-amz-copy-source"
)
