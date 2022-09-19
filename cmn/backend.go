// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

const AwsMultipartDelim = "-"

type backendFuncs struct {
	EncodeVersion func(v any) (version string, isSet bool)
	EncodeCksum   func(v any) (cksumValue string, isSet bool)
}

func awsIsVersionSet(version *string) bool {
	return version != nil && *version != "" && *version != "null"
}

var BackendHelpers = struct {
	Amazon backendFuncs
	Azure  backendFuncs
	Google backendFuncs
	HDFS   backendFuncs
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
		EncodeCksum: func(v any) (string, bool) {
			switch x := v.(type) {
			case *string:
				if strings.Contains(*x, AwsMultipartDelim) {
					return *x, true // return as-is multipart
				}
				cksum, _ := strconv.Unquote(*x)
				return cksum, true
			case string:
				return x, true
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
	},
	Azure: backendFuncs{
		EncodeVersion: func(v any) (string, bool) {
			switch x := v.(type) {
			case string:
				x = strings.Trim(x, "\"")
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
			default:
				debug.FailTypeCast(v)
				return "", false
			}
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
	HDFS: backendFuncs{
		EncodeCksum: func(v any) (cksumValue string, isSet bool) {
			switch x := v.(type) {
			case []byte:
				return hex.EncodeToString(x), true
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
	},
	HTTP: backendFuncs{
		EncodeVersion: func(v any) (string, bool) {
			switch x := v.(type) {
			case string:
				// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
				x = strings.TrimPrefix(x, "W/")
				x = strings.Trim(x, "\"")
				return x, x != ""
			default:
				debug.FailTypeCast(v)
				return "", false
			}
		},
	},
}
