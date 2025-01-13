// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import "github.com/NVIDIA/aistore/cmn"

const (
	// AWS URL params
	QparamVersioning        = "versioning"
	QparamLifecycle         = "lifecycle"
	QparamCORS              = "cors"
	QparamPolicy            = "policy"
	QparamACL               = "acl"
	QparamMultiDelete       = "delete"
	QparamMaxKeys           = "max-keys"
	QparamPrefix            = "prefix"
	QparamContinuationToken = "continuation-token"
	QparamStartAfter        = "start-after"
	QparamDelimiter         = "delimiter"

	// multipart
	QparamMptUploads        = "uploads"
	QparamMptUploadID       = "uploadId"
	QparamMptPartNo         = "partNumber"
	QparamMptMaxUploads     = "max-uploads"
	QparamMptUploadIDMarker = "upload-id-marker"

	QparamAccessKeyID = "AWSAccessKeyId"
	QparamExpires     = "Expires"
	QparamSignature   = "Signature"
	QparamXID         = "x-id"

	HeaderPrefix = "X-Amz-"

	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata
	HeaderMetaPrefix = cmn.AwsHeaderMetaPrefix

	HeaderCredentials = "X-Amz-Credential" //nolint:gosec // This is just a header name definition...

	versioningEnabled  = "Enabled"
	versioningDisabled = "Suspended"

	// Maximum number of parts per upload
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
	MaxPartsPerUpload = 10000

	s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01"

	AISRegion = "ais"
	AISServer = "AIStore"
)
