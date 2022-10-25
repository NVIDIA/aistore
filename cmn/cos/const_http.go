// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cos

// References:
// - Standard HTTP headers: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers
// - AIS HTTP headers:      api package and api/apc/headers.go source

const (
	HdrRange                 = "Range" // Ref: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	HdrRangeValPrefix        = "bytes="
	HdrContentRange          = "Content-Range"
	HdrContentRangeValPrefix = "bytes " // Ref: https://tools.ietf.org/html/rfc7233#section-4.2
	HdrAcceptRanges          = "Accept-Ranges"
	HdrContentType           = "Content-Type"
	HdrContentTypeOptions    = "X-Content-Type-Options"
	HdrContentLength         = "Content-Length"
	HdrUserAgent             = "User-Agent"
	HdrAccept                = "Accept"
	HdrLocation              = "Location"
	HdrServer                = "Server"
	HdrETag                  = "ETag" // Ref: https://developer.mozilla.org/en-US/docs/Web/HTTP/Hdrs/ETag
)

// provider-specific headers (=> custom props, and more)
const (
	// https://cloud.google.com/storage/docs/xml-api/reference-headers
	GsCksumHeader   = "x-goog-hash"
	GsVersionHeader = "x-goog-generation"

	// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
	S3CksumHeader   = "ETag"
	S3VersionHeader = "x-amz-version-id"

	// s3 api request headers
	S3HdrObjSrc        = "x-amz-copy-source"
	S3HdrMptCnt        = "x-amz-mp-parts-count"
	S3HdrContentSHA256 = "x-amz-content-sha256"
	S3HdrBckRegion     = "x-amz-bucket-region"

	S3ChecksumCRC32  = "x-amz-checksum-crc32"
	S3ChecksumCRC32C = "x-amz-checksum-crc32c"
	S3ChecksumSHA1   = "x-amz-checksum-sha1"
	S3ChecksumSHA256 = "x-amz-checksum-sha256"
	S3LastModified   = "Last-Modified"

	S3MetadataChecksumType = "x-amz-meta-ais-cksum-type"
	S3MetadataChecksumVal  = "x-amz-meta-ais-cksum-val"

	// https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties#response-headers
	AzCksumHeader   = "Content-MD5"
	AzVersionHeader = "ETag"
)

// Ref: https://www.iana.org/assignments/media-types/media-types.xhtml
const (
	ContentJSON           = "application/json"
	ContentJSONCharsetUTF = "application/json; charset=utf-8"
	ContentMsgPack        = "application/msgpack"
	ContentXML            = "application/xml"
	ContentBinary         = "application/octet-stream"
)
