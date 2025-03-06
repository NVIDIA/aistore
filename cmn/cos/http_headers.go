// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

// standard MIME types
// - https://www.iana.org/assignments/media-types/media-types.xhtml
// - https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
const (
	ContentJSON           = "application/json"
	ContentJSONCharsetUTF = "application/json; charset=utf-8"
	ContentMsgPack        = "application/msgpack"
	ContentXML            = "application/xml"
	ContentBinary         = "application/octet-stream"

	// not present in IANA registry
	// mozilla.org has it though, and also https://en.wikipedia.org/wiki/List_of_archive_formats
	ContentTar = "application/x-tar"
)

// Ref: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers
const (
	// range to read:
	HdrRange          = "Range" // Ref: https://www.rfc-editor.org/rfc/rfc7233#section-2.1
	HdrRangeValPrefix = "bytes="
	// range read response:
	HdrContentRange          = "Content-Range"
	HdrContentRangeValPrefix = "bytes " // Ref: https://tools.ietf.org/html/rfc7233#section-4.2
	HdrAcceptRanges          = "Accept-Ranges"

	// content length & type
	HdrContentType        = "Content-Type"
	HdrContentTypeOptions = "X-Content-Type-Options"
	HdrContentLength      = "Content-Length"

	// misc. gen
	HdrUserAgent = "User-Agent"
	HdrAccept    = "Accept"
	HdrLocation  = "Location"
	HdrServer    = "Server"
	HdrETag      = "ETag" // Ref: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag

	HdrHSTS = "Strict-Transport-Security"
)

//
// provider-specific headers (=> custom props, and more)
//

const (
	// https://cloud.google.com/storage/docs/xml-api/reference-headers
	GsCksumHeader   = "x-goog-hash"
	GsVersionHeader = "x-goog-generation"
)

const (
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
	S3CksumHeader   = HdrETag
	S3VersionHeader = "x-amz-version-id"

	// s3 api request headers
	S3HdrObjSrc = "x-amz-copy-source"

	// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
	S3UnsignedPayload  = "UNSIGNED-PAYLOAD"
	S3HdrContentSHA256 = "x-amz-content-sha256"
	S3HdrSignedHeaders = "x-ams-signedheaders"

	S3HdrBckRegion = "x-amz-bucket-region"

	S3MetadataChecksumType = "x-amz-meta-ais-cksum-type"
	S3MetadataChecksumVal  = "x-amz-meta-ais-cksum-val"

	S3LastModified = "Last-Modified"
)

const (
	// https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties#response-headers
	AzCksumHeader   = "Content-MD5"
	AzVersionHeader = HdrETag
)

// NOTE: for AIS headers, see api/apc/headers.go
