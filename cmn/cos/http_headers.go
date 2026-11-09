// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"net/http"
	"strings"
)

// this source contains standard and 3rd party HTTP headers
// for AIS headers, see api/apc/headers.go

// standard MIME types
// - https://www.iana.org/assignments/media-types/media-types.xhtml
// - https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
const (
	ContentJSON           = "application/json"
	ContentJSONCharsetUTF = "application/json; charset=utf-8"
	ContentMsgPack        = "application/msgpack" // see AcceptsMsgPack() parser below
	ContentXML            = "application/xml"
	ContentBinary         = "application/octet-stream" // the default
	ContentBinaryS3       = "binary/octet-stream"      // S3 default for untyped objects (recognized, never emitted)
	ContentZip            = "application/zip"

	// not present in IANA registry
	// mozilla.org has it though, and also https://en.wikipedia.org/wiki/List_of_archive_formats
	ContentTar  = "application/x-tar"
	ContentGzip = "application/gzip" // widely used for .tar.gz and .tgz
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

	// conditional write (see CondWrite)
	HdrIfMatch     = "If-Match"      // Ref: https://www.rfc-editor.org/rfc/rfc9110#section-13.1.1
	HdrIfNoneMatch = "If-None-Match" // Ref: https://www.rfc-editor.org/rfc/rfc9110#section-13.1.2

	HdrHSTS = "Strict-Transport-Security"

	// RFC1123GMT or, same, http.TimeFormat ("Mon, 02 Jan 2006 15:04:05 GMT")
	// see also, and separately, cmn.LsoLastModified (list-objects)
	HdrLastModified = "Last-Modified"
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

	// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
	S3HdrDecodedContentLength = "x-amz-decoded-content-length"

	S3HdrBckRegion = "x-amz-bucket-region"

	S3MetadataChecksumType = "x-amz-meta-ais-cksum-type"
	S3MetadataChecksumVal  = "x-amz-meta-ais-cksum-val"
)

const (
	// https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties#response-headers
	AzCksumHeader   = "Content-MD5"
	AzVersionHeader = HdrETag
)

// parse HdrAccept for ContentMsgPack
// e.g.: "application/json", "application/msgpack;q=0.5", "application/msgpack, */*"
func AcceptsMsgPack(hdr http.Header) bool {
	v := hdr.Get(HdrAccept)
	if v == "" {
		return false
	}
	for part := range strings.SplitSeq(v, ",") {
		mt := part
		if i := strings.IndexByte(mt, ';'); i >= 0 {
			mt = mt[:i]
		}
		if strings.TrimSpace(mt) == ContentMsgPack {
			return true
		}
	}
	return false
}

//
// conditional write
//

// write conditions, forwarded as is to backends that support them
// (see core.Backend.CondWrite); the remote store evaluates and validates
type CondWrite struct {
	IfMatch     string // single ETag unquoted; other forms as supplied
	IfNoneMatch string // e.g., "*"
	MatchETag   bool   // IfMatch is a single ETag that can be re-quoted
}

// direct map lookup: net/http canonicalizes request header keys (and both constants are canonical)
func IsCondWrite(hdr http.Header) bool {
	return len(hdr[HdrIfMatch]) != 0 || len(hdr[HdrIfNoneMatch]) != 0
}

func ParseCondWrite(hdr http.Header) (CondWrite, error) {
	match, none := hdr[HdrIfMatch], hdr[HdrIfNoneMatch]
	if len(match) == 0 && len(none) == 0 {
		return CondWrite{}, nil
	}
	return parseCondWrite(match, none)
}

// trim both values; normalize only a single If-Match ETag
func parseCondWrite(match, none []string) (cw CondWrite, err error) {
	if cw.IfMatch, err = condValue(match, HdrIfMatch); err != nil {
		return cw, err
	}
	if etag := cw.IfMatch; etag != "" {
		quoted := len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"'
		if quoted {
			etag = etag[1 : len(etag)-1]
			if etag == "" {
				return cw, fmt.Errorf("invalid %s (empty ETag)", HdrIfMatch)
			}
		}
		// single ETag: not a wildcard, no inner quotes or spaces, and not a bare (unquoted) list
		if etag != "*" && !strings.ContainsAny(etag, "\" \t") && (quoted || !strings.ContainsRune(etag, ',')) {
			cw.IfMatch, cw.MatchETag = etag, true
		}
	}
	cw.IfNoneMatch, err = condValue(none, HdrIfNoneMatch)
	return cw, err
}

// return the single (trimmed) value, if present
func condValue(vals []string, key string) (string, error) {
	switch len(vals) {
	case 0:
		return "", nil
	case 1:
		if v := strings.TrimSpace(vals[0]); v != "" {
			return v, nil
		}
	}
	return "", fmt.Errorf("invalid %s %q", key, vals)
}
