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

const (
	// from https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
	// "The ETag may or may not be an MD5 digest of the object data. Whether or
	// not it is depends on how the object was created and how it is encrypted..."
	AwsMultipartDelim = "-"

	// Due to import cycle we need to define "x-amz-meta" header here
	// See also:
	// - ais/s3/const.go
	// - https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata
	AwsHeaderMetaPrefix = "X-Amz-Meta-"

	// OCI Object Storage user metadata
	// from: https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/managingobjects.htm
	// User metadata uses the "opc-meta-" prefix (canonicalized to "Opc-Meta-").
	OCIHeaderMetaPrefix = "Opc-Meta-"
)

type backendFuncs struct {
	EncodeVersion  func(v any) (version string, isSet bool)
	EncodeETag     func(v any) (etag string, isSet bool)
	EncodeCksum    func(v any) (cksumValue string, isSet bool)
	EncodeMetadata func(metadata map[string]string) (header map[string]string)
	DecodeMetadata func(header http.Header) (metadata map[string]string)
}

// backend-specific encoding/decoding functions
var BackendHelpers = struct {
	Amazon backendFuncs
	Azure  backendFuncs
	Google backendFuncs
	OCI    backendFuncs
	HTTP   backendFuncs
}{
	Amazon: backendFuncs{
		EncodeVersion:  awsEncodeVersion,
		EncodeETag:     awsEncodeETag,
		EncodeCksum:    awsEncodeCksum,
		EncodeMetadata: awsEncodeMetadata,
		DecodeMetadata: awsDecodeMetadata,
	},
	Azure: backendFuncs{
		EncodeETag:  azureEncodeETag,
		EncodeCksum: azureEncodeCksum,
	},
	Google: backendFuncs{
		EncodeVersion: googleEncodeVersion,
		EncodeETag:    _encStr,
		EncodeCksum:   googleEncodeCksum,
	},
	OCI: backendFuncs{
		EncodeVersion:  ociEncodeVersion,
		EncodeETag:     ociEncodeETag,
		EncodeCksum:    ociEncodeCksum,
		EncodeMetadata: ociEncodeMetadata,
		DecodeMetadata: ociDecodeMetadata,
	},
	HTTP: backendFuncs{
		EncodeETag: httpEncodeETag,
	},
}

func isS3MultipartEtag(etag string) bool {
	return strings.Contains(etag, AwsMultipartDelim)
}

func awsIsVersionSet(version *string) bool {
	return version != nil && *version != "" && *version != "null"
}

// unquote checksum, ETag, and version
// e.g., https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
func UnquoteCEV(val string) string {
	return strings.Trim(val, "\"")
}

//
// common
//

func _encValidate(v any, validator func(string) bool) (string, bool) {
	switch x := v.(type) {
	case *string:
		if x == nil || *x == "" {
			return "", false
		}
		if validator != nil && !validator(*x) {
			return "", false
		}
		return *x, true
	case string:
		if x == "" {
			return "", false
		}
		if validator != nil && !validator(x) {
			return "", false
		}
		return x, true
	default:
		debug.FailTypeCast(v)
		return "", false
	}
}

func _encStr(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, x != ""
	default:
		debug.FailTypeCast(v)
		return "", false
	}
}

func _encMeta(metadata map[string]string, prefix string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	header := make(map[string]string, len(metadata))
	for k, v := range metadata {
		key := http.CanonicalHeaderKey(prefix + k)
		header[key] = v
	}
	return header
}

func _decMeta(header http.Header, prefix string) map[string]string {
	var metadata map[string]string
	for k := range header {
		if strings.HasPrefix(k, prefix) {
			if metadata == nil {
				metadata = make(map[string]string)
			}
			key := strings.TrimPrefix(k, prefix)
			value := header.Get(k)
			metadata[key] = value
		}
	}
	return metadata
}

// AWS

func awsEncodeVersion(v any) (string, bool) {
	return _encValidate(v, func(s string) bool {
		return awsIsVersionSet(&s)
	})
}

func awsEncodeETag(v any) (string, bool) {
	return _encValidate(v, nil) // no additional validation
}

func awsEncodeCksum(v any) (string, bool) {
	value, isSet := _encValidate(v, func(s string) bool {
		return !isS3MultipartEtag(s) // reject multipart etags
	})
	if !isSet {
		return "", false
	}
	return UnquoteCEV(value), true
}

func awsEncodeMetadata(metadata map[string]string) map[string]string {
	return _encMeta(metadata, AwsHeaderMetaPrefix)
}

func awsDecodeMetadata(header http.Header) map[string]string {
	return _decMeta(header, AwsHeaderMetaPrefix)
}

// Azure

func azureEncodeETag(v any) (string, bool) {
	return _encStr(v)
}

func azureEncodeCksum(v any) (string, bool) {
	switch x := v.(type) {
	case []byte:
		if len(x) == 0 {
			return "", false
		}
		return hex.EncodeToString(x), true
	default:
		debug.FailTypeCast(v)
		return "", false
	}
}

// Google

func googleEncodeVersion(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, x != ""
	case int64:
		return strconv.FormatInt(x, 10), true
	default:
		debug.FailTypeCast(v)
		return "", false
	}
}

func googleEncodeCksum(v any) (string, bool) {
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
}

// OCI

func ociEncodeVersion(_ any) (string, bool) {
	return "", false
}

func ociEncodeETag(v any) (string, bool) {
	value, isSet := _encValidate(v, nil)
	if !isSet {
		return "", false
	}
	return UnquoteCEV(value), true
}

func ociEncodeCksum(v any) (string, bool) {
	value, isSet := _encValidate(v, nil)
	if !isSet {
		return "", false
	}
	return UnquoteCEV(value), true
}

func ociEncodeMetadata(metadata map[string]string) map[string]string {
	return _encMeta(metadata, OCIHeaderMetaPrefix)
}

func ociDecodeMetadata(header http.Header) map[string]string {
	return _decMeta(header, OCIHeaderMetaPrefix)
}

// HTTP

func httpEncodeETag(v any) (string, bool) {
	return _encStr(v)
}
