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
)

const (
	awsMultipartDelim = "-"
)

type (
	backendFuncs struct {
		EncodeVersion func(v interface{}) (version string, isSet bool)
		EncodeCksum   func(v interface{}) (cksumValue string, isSet bool)
	}
)

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
		EncodeVersion: func(v interface{}) (string, bool) {
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
				panic(v)
			}
		},
		EncodeCksum: func(v interface{}) (string, bool) {
			switch x := v.(type) {
			case *string:
				cksum, _ := strconv.Unquote(*x)
				// FIXME: multipart
				return cksum, !strings.Contains(cksum, awsMultipartDelim)
			case string:
				// FIXME: multipart
				return x, !strings.Contains(x, awsMultipartDelim)
			default:
				panic(v)
			}
		},
	},
	Azure: backendFuncs{
		EncodeVersion: func(v interface{}) (string, bool) {
			switch x := v.(type) {
			case string:
				x = strings.Trim(x, "\"")
				return x, x != ""
			default:
				panic(v)
			}
		},
		EncodeCksum: func(v interface{}) (string, bool) {
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
				panic(v)
			}
		},
	},
	Google: backendFuncs{
		EncodeVersion: func(v interface{}) (string, bool) {
			switch x := v.(type) {
			case string:
				return x, x != ""
			case int64:
				return strconv.FormatInt(x, 10), true
			default:
				panic(v)
			}
		},
		EncodeCksum: func(v interface{}) (string, bool) {
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
				panic(v)
			}
		},
	},
	HDFS: backendFuncs{
		EncodeCksum: func(v interface{}) (cksumValue string, isSet bool) {
			switch x := v.(type) {
			case []byte:
				return hex.EncodeToString(x), true
			default:
				panic(v)
			}
		},
	},
	HTTP: backendFuncs{
		EncodeVersion: func(v interface{}) (string, bool) {
			switch x := v.(type) {
			case string:
				// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
				x = strings.TrimPrefix(x, "W/")
				x = strings.Trim(x, "\"")
				return x, x != ""
			default:
				panic(v)
			}
		},
	},
}
