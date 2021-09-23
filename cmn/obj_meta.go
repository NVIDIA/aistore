// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
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
		GetCustomKey(key string) (val string, exists bool)
		SetCustomKey(k, v string)
		String() string
	}
	ObjAttrs struct {
		Atime    int64         // access time (nanoseconds since UNIX epoch)
		Size     int64         // object size (bytes)
		Ver      string        // object version
		Cksum    *cos.Cksum    // object checksum (NOTE: m.b. cloned)
		customMD cos.SimpleKVs // custom metadata: ETag, MD5, CRC, user-defined ...
	}
)

// interface guard
var _ ObjAttrsHolder = (*ObjAttrs)(nil)

func (oa *ObjAttrs) String() string { return fmt.Sprintf("%+v, size=%d", oa.customMD, oa.Size) }

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

//
// to and from HTTP header converters (as in: HEAD /object)
//

func ToHeader(oah ObjAttrsHolder, hdr http.Header) {
	if cksum := oah.Checksum(); !cksum.IsEmpty() {
		hdr.Set(HdrObjCksumType, cksum.Ty())
		hdr.Set(HdrObjCksumVal, cksum.Val())
	}
	if at := oah.AtimeUnix(); at != 0 {
		hdr.Set(HdrObjAtime, cos.UnixNano2S(at))
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
		entry := strings.Join([]string{k, v}, "=")
		hdr.Add(HdrObjCustomMD, entry)
	}
}

func (oa *ObjAttrs) ToHeader(hdr http.Header) {
	ToHeader(oa, hdr)
}

func (oa *ObjAttrs) FromHeader(hdr http.Header) (cksum *cos.Cksum) {
	// NOTE: returning checksum separately for subsequent validation
	if ty := hdr.Get(HdrObjCksumType); ty != "" {
		val := hdr.Get(HdrObjCksumVal)
		cksum = cos.NewCksum(ty, val)
	}

	if at := hdr.Get(HdrObjAtime); at != "" {
		atime, err := cos.S2UnixNano(at)
		debug.AssertNoErr(err)
		oa.Atime = atime
	}
	if sz := hdr.Get(HdrContentLength); sz != "" {
		size, err := strconv.ParseInt(sz, 10, 64)
		debug.AssertNoErr(err)
		oa.Size = size
	}
	if v := hdr.Get(HdrObjVersion); v != "" {
		oa.Ver = v
	}
	if customMD := hdr[http.CanonicalHeaderKey(HdrObjCustomMD)]; len(customMD) > 0 {
		for _, v := range customMD {
			entry := strings.SplitN(v, "=", 2)
			oa.SetCustomKey(entry[0], entry[1])
		}
	}
	return
}

//
// Equality
//

func (oa *ObjAttrs) Equal(rem ObjAttrsHolder) (equal bool) {
	var remVok, locVok, remEok, locEok, rem5ok, loc5ok, remCok, locCok bool
	// provider ("source") check
	remSrc, ok := rem.GetCustomKey(SourceObjMD)
	if !ok {
		return
	}
	locSrc, ok := oa.GetCustomKey(SourceObjMD)
	if !ok || locSrc != remSrc {
		return
	}
	// version check
	var remMeta, locMeta string
	if remMeta, remVok = rem.GetCustomKey(VersionObjMD); remVok {
		if locMeta, locVok = oa.GetCustomKey(VersionObjMD); !locVok || remMeta != locMeta {
			return
		}
	}
	// ETag check
	if remMeta, remEok = rem.GetCustomKey(ETag); remEok {
		if locMeta, locEok = oa.GetCustomKey(ETag); !locEok || remMeta != locMeta {
			return
		}
	}
	// MD5 check
	if remMeta, rem5ok = rem.GetCustomKey(MD5ObjMD); rem5ok {
		if locMeta, loc5ok = oa.GetCustomKey(MD5ObjMD); !loc5ok || remMeta != locMeta {
			return
		}
	}
	// finally, CRC check
	if remMeta, remCok = rem.GetCustomKey(CRC32CObjMD); remCok {
		if locMeta, locCok = oa.GetCustomKey(CRC32CObjMD); !locCok || remMeta != locMeta {
			return
		}
	}
	//
	// NOTE: err on the of caution and assume objects are different
	//       if none of the {version, ETag, MD5, CRC} is present
	//
	equal = remVok || remEok || rem5ok || remCok
	return
}
