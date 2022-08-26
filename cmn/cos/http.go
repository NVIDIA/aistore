// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cos

// References:
// - Standard HTTP headers: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers
// - AIS HTTP headers: api package and api/apc/const.go

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
	HdrETag                  = "ETag" // Ref: https://developer.mozilla.org/en-US/docs/Web/HTTP/Hdrs/ETag
)

// Ref: https://www.iana.org/assignments/media-types/media-types.xhtml
const (
	ContentJSON           = "application/json"
	ContentJSONCharsetUTF = "application/json; charset=utf-8"
	ContentMsgPack        = "application/msgpack"
	ContentXML            = "application/xml"
	ContentBinary         = "application/octet-stream"
)
