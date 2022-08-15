// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

const (
	AISRegion = "ais"
	AISSever  = "AIS"

	// AWS URL params
	QparamVersioning        = "versioning"
	QparamLifecycle         = "lifecycle"
	QparamCORS              = "cors"
	QparamPolicy            = "policy"
	QparamACL               = "acl"
	QparamMultiDelete       = "delete"
	QparamMultipartUploads  = "uploads"
	QparamMultipartUploadID = "uploadId"
	QparamMultipartPartNo   = "partNumber"

	versioningEnabled  = "Enabled"
	versioningDisabled = "Suspended"

	s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01"

	// Headers
	HeaderObjSrc = "x-amz-copy-source"
)
