// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cos

// Ref: "HTTP headers" at https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers

const (
	HdrRange                 = "Range" // Ref: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	HdrRangeValPrefix        = "bytes="
	HdrContentRange          = "Content-Range"
	HdrContentRangeValPrefix = "bytes " // Ref: https://tools.ietf.org/html/rfc7233#section-4.2
	HdrAcceptRanges          = "Accept-Ranges"
	HdrContentType           = "Content-Type"
	HdrContentLength         = "Content-Length"
	HdrAccept                = "Accept"
	HdrLocation              = "Location"
	HdrETag                  = "ETag" // Ref: https://developer.mozilla.org/en-US/docs/Web/HTTP/Hdrs/ETag
	HdrError                 = "Hdr-Error"
)

// Ref: https://www.iana.org/assignments/media-types/media-types.xhtml
const (
	ContentJSON    = "application/json"
	ContentMsgPack = "application/msgpack"
	ContentXML     = "application/xml"
	ContentBinary  = "application/octet-stream"
)
