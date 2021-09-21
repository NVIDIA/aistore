// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// LOM custom metadata stored under `lomCustomMD`.
const (
	SourceObjMD = "source"
	AmazonObjMD = ProviderAmazon
	AzureObjMD  = ProviderAzure
	GoogleObjMD = ProviderGoogle
	HDFSObjMD   = ProviderHDFS
	HTTPObjMD   = ProviderHTTP
	WebObjMD    = "web"

	VersionObjMD = "remote-version"

	CRC32CObjMD = cos.ChecksumCRC32C
	MD5ObjMD    = cos.ChecksumMD5

	ETag = "ETag"

	OrigURLObjMD = "orig_url"
)

// provider-specific header keys
const (
	// https://cloud.google.com/storage/docs/xml-api/reference-headers
	GsCksumHeader   = "x-goog-hash"
	GsVersionHeader = "x-goog-generation"

	// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
	S3CksumHeader   = "ETag"
	S3VersionHeader = "x-amz-version-id"

	// https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties#response-headers
	AzCksumHeader   = "Content-MD5"
	AzVersionHeader = "ETag"
)

type (
	ObjAttrsHolder interface {
		SizeBytes(special ...bool) int64
		Version(special ...bool) string
		Checksum() *cos.Cksum
		AtimeUnix() int64
		GetCustomMD() cos.SimpleKVs
		SetCustomKey(k, v string)
	}
	ObjAttrs struct {
		Atime    int64         // access time (nanoseconds since UNIX epoch)
		Size     int64         // object size (bytes)
		Ver      string        // object version
		ETag     string        // ETag
		Cksum    *cos.Cksum    // object checksum (NOTE: m.b. cloned)
		customMD cos.SimpleKVs // custom metadata
	}
)

// interface guard
var _ ObjAttrsHolder = (*ObjAttrs)(nil)

func (oa *ObjAttrs) SizeBytes(_ ...bool) int64 { return oa.Size }
func (oa *ObjAttrs) Version(_ ...bool) string  { return oa.Ver }
func (oa *ObjAttrs) AtimeUnix() int64          { return oa.Atime }
func (oa *ObjAttrs) Checksum() *cos.Cksum      { return oa.Cksum }
func (oa *ObjAttrs) SetCksum(ty, val string)   { oa.Cksum = cos.NewCksum(ty, val) }

// custom metadata
func (oa *ObjAttrs) GetCustomMD() cos.SimpleKVs   { return oa.customMD }
func (oa *ObjAttrs) SetCustomMD(md cos.SimpleKVs) { oa.customMD = md }

func (oa *ObjAttrs) GetCustomKey(key string) (val string, exists bool) {
	val, exists = oa.customMD[key]
	return
}

func (oa *ObjAttrs) SetCustomKey(k, v string) {
	debug.Assert(k != "")
	if oa.customMD == nil {
		oa.customMD = make(cos.SimpleKVs, 4)
	}
	oa.customMD[k] = v
}

// ObjAttrsHolder => ObjAttrs; see also: lom.CopyAttrs
func (oa *ObjAttrs) CopyFrom(oah ObjAttrsHolder, skipCksum ...bool) {
	oa.Atime = oah.AtimeUnix()
	oa.Size = oah.SizeBytes()
	oa.Ver = oah.Version()
	if len(skipCksum) == 0 || !skipCksum[0] {
		debug.Assert(oah.Checksum() != nil)
		oa.Cksum = oah.Checksum().Clone() // TODO: checksum by value (***)
	}
	oa.customMD = oah.GetCustomMD()
}

// ObjAttrsHolder => http header
func ToHTTPHdr(oah ObjAttrsHolder, hdrs ...http.Header) (hdr http.Header) {
	if len(hdrs) > 0 && hdrs[0] != nil {
		hdr = hdrs[0]
	} else {
		hdr = make(http.Header, 8)
	}
	if cksum := oah.Checksum(); !cksum.IsEmpty() {
		hdr.Set(HdrObjCksumType, cksum.Ty())
		hdr.Set(HdrObjCksumVal, cksum.Val())
	}
	if ts := oah.AtimeUnix(); ts != 0 {
		hdr.Set(HdrObjAtime, cos.UnixNano2S(ts))
	}
	if n := oah.SizeBytes(true); n > 0 {
		hdr.Set(HdrContentLength, strconv.FormatInt(n, 10))
	}
	if v := oah.Version(true); v != "" {
		hdr.Set(HdrObjVersion, v)
	}
	custom := oah.GetCustomMD()
	for k, v := range custom {
		debug.Assert(k != "")
		hdr.Add(HdrObjCustomMD, strings.Join([]string{k, v}, "="))
	}
	return
}
