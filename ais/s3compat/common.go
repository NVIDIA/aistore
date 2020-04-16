// Package s3compat provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package s3compat

const (
	Root        = "s3"
	ContentType = "application/xml"

	s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01"
	// TODO: can it be omitted? // storageClass = "STANDARD"

	// Headers
	headerSize    = "Content-Length"
	headerETag    = "ETag"
	headerVersion = "x-amz-version-id"
	headerAtime   = "Last-Modified"
)
