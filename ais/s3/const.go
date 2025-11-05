// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	// AWS URL params
	QparamVersioning        = "versioning" // Configure or retrieve bucket versioning settings
	QparamLifecycle         = "lifecycle"
	QparamCORS              = "cors"
	QparamPolicy            = "policy"
	QparamACL               = "acl"
	QparamMultiDelete       = "delete"             // Delete multiple objects in a single request
	QparamMaxKeys           = "max-keys"           // Maximum number of objects to return in listing
	QparamPrefix            = "prefix"             // Filter objects by key prefix
	QparamContinuationToken = "continuation-token" // Pagination token for continued listing
	QparamStartAfter        = "start-after"        // Start listing after this object key
	QparamDelimiter         = "delimiter"          // Delimiter for grouping object keys

	// multipart
	QparamMptUploads        = "uploads"    // Start multipart upload or list active uploads
	QparamMptUploadID       = "uploadId"   // Complete, abort, or list parts of specific multipart upload
	QparamMptPartNo         = "partNumber" // Part number for multipart upload
	QparamMptMaxUploads     = "max-uploads"
	QparamMptUploadIDMarker = "upload-id-marker"

	QparamAccessKeyID = "AWSAccessKeyId"
	QparamExpires     = "Expires"
	QparamSignature   = "Signature"
	QparamXID         = "x-id"

	HeaderPrefix = "X-Amz-"

	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata
	HeaderMetaPrefix = cmn.AwsHeaderMetaPrefix

	HeaderCredentials   = "X-Amz-Credential"     //nolint:gosec // This is just a header name definition...
	HeaderSecurityToken = "X-Amz-Security-Token" // AWS temporary security token (used for JWT in compatibility mode)

	versioningEnabled  = "Enabled"
	versioningDisabled = "Suspended"

	DefaultPartSize = 128 * cos.MiB

	s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01"

	AISRegion = "ais"
	AISServer = "AIStore"
)
