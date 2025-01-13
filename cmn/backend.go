// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

type backendFuncs struct {
	EncodeVersion  func(v any) (version string, isSet bool)
	EncodeETag     func(v any) (etag string, isSet bool)
	EncodeCksum    func(v any) (cksumValue string, isSet bool)
	EncodeMetadata func(metadata map[string]string) (header map[string]string)

	DecodeMetadata func(header http.Header) (metadata map[string]string)
}

// from https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
// "The ETag may or may not be an MD5 digest of the object data. Whether or
// not it is depends on how the object was created and how it is encrypted..."
const AwsMultipartDelim = "-"

// Due to import cycle we need to define "x-amz-meta" header here
// See also:
// - ais/s3/const.go
// - https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata
const AwsHeaderMetaPrefix = "X-Amz-Meta-"

func isS3MultipartEtag(etag string) bool {
	return strings.Contains(etag, AwsMultipartDelim)
}

func awsIsVersionSet(version *string) bool {
	return version != nil && *version != "" && *version != "null"
}

// unquote checksum, ETag, and version
// e.g., https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
func UnquoteCEV(val string) string { return strings.Trim(val, "\"") }

var BackendHelpers = struct {
	Amazon backendFuncs
	Azure  backendFuncs
	Google backendFuncs
	OCI    backendFuncs
	HTTP   backendFuncs
}{
	Amazon: backendFuncs{
		EncodeVersion: func(v any) (string, bool) {
			switch x := v.(type) {
			case *string:
				if awsIsVersionSet(x) {
					return *x, true
				}
				return "", false
			case string:
				if awsIsVersionSet(&x) {
					return x, true
				}
				return x, false
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
		// ETag is set whenever it is non-empty. Store with quotes.
		EncodeETag: func(v any) (string, bool) {
			switch x := v.(type) {
			case *string:
				return *x, *x != ""
			case string:
				return x, x != ""
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
		// Cksum is set whenever it is non-empty, and it is not a multipart etag,
		// we just need to remove quotes.
		// From https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html:
		// - "The entity tag is a hash of the object. The ETag reflects changes only
		//    to the contents of an object, not its metadata."
		// - "The ETag may or may not be an MD5 digest of the object data. Whether or
		//    not it is depends on how the object was created and how it is encrypted..."
		EncodeCksum: func(v any) (string, bool) {
			switch x := v.(type) {
			case *string:
				if x == nil || *x == "" {
					return "", false
				}
				if isS3MultipartEtag(*x) {
					return "", false
				}
				return UnquoteCEV(*x), true
			case string:
				if x == "" {
					return "", false
				}
				if isS3MultipartEtag(x) {
					return "", false
				}
				return UnquoteCEV(x), true
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
		EncodeMetadata: func(metadata map[string]string) (header map[string]string) {
			if len(metadata) == 0 {
				return
			}
			header = make(map[string]string, len(metadata))
			for k, v := range metadata {
				key := http.CanonicalHeaderKey(AwsHeaderMetaPrefix + k)
				header[key] = v
			}
			return
		},

		DecodeMetadata: func(header http.Header) (metadata map[string]string) {
			for headerKey := range header {
				if strings.HasPrefix(headerKey, AwsHeaderMetaPrefix) {
					if metadata == nil {
						metadata = make(map[string]string)
					}
					key := strings.TrimPrefix(headerKey, AwsHeaderMetaPrefix)
					value := header.Get(headerKey)
					metadata[key] = value
				}
			}
			return
		},
	},
	Google: backendFuncs{
		EncodeVersion: func(v any) (string, bool) {
			switch x := v.(type) {
			case string:
				return x, x != ""
			case int64:
				return strconv.FormatInt(x, 10), true
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
		EncodeETag: func(v any) (string, bool) {
			switch x := v.(type) {
			case string:
				return x, x != ""
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
		EncodeCksum: func(v any) (string, bool) {
			switch x := v.(type) {
			case string:
				decoded, err := base64.StdEncoding.DecodeString(x)
				if err != nil {
					return "", false
				}
				return hex.EncodeToString(decoded), true
			case []byte:
				return hex.EncodeToString(x), true
			case uint32:
				// Encode a uint32 as Base64 in big-endian byte order.
				// See: https://cloud.google.com/storage/docs/xml-api/reference-headers#xgooghash.
				b := []byte{byte(x >> 24), byte(x >> 16), byte(x >> 8), byte(x)}
				return base64.StdEncoding.EncodeToString(b), true
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
	},
	OCI: backendFuncs{
		EncodeVersion: func(_ any) (string, bool) {
			return "", false
		},
		EncodeETag: func(v any) (string, bool) {
			switch x := v.(type) {
			case *string:
				if x == nil || *x == "" {
					return "", false
				}
				return UnquoteCEV(*x), true
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
		EncodeCksum: func(v any) (string, bool) {
			switch x := v.(type) {
			case *string:
				if x == nil || *x == "" {
					return "", false
				}
				return UnquoteCEV(*x), true
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
	},
	HTTP: backendFuncs{
		EncodeETag: func(v any) (string, bool) {
			switch x := v.(type) {
			case string:
				// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
				return x, x != ""
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
	},
}
