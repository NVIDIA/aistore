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

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// LOM custom metadata stored under `lomCustomMD`.
const (
	// source of the cold-GET and download; the values include all
	// 3rd party backend providers (remote AIS not including)
	SourceObjMD = "source"

	// downloader' source is "web"
	WebObjMD = "web"

	// system-supported custom attrs
	VersionObjMD = "remote-version"
	CRC32CObjMD  = cos.ChecksumCRC32C
	MD5ObjMD     = cos.ChecksumMD5
	ETag         = "ETag"

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

// object properties (NOTE: embeds system `ObjAttrs` that in turn includes custom user-defined)
type ObjectProps struct {
	ObjAttrs
	Name   string `json:"name"`
	Bck    Bck    `json:"bucket"`
	Mirror struct {
		Copies int      `json:"copies,omitempty"`
		Paths  []string `json:"paths,omitempty"`
	} `json:"mirror"`
	EC struct {
		Generation   int64 `json:"generation"`
		DataSlices   int   `json:"data"`
		ParitySlices int   `json:"parity"`
		IsECCopy     bool  `json:"replicated"`
	} `json:"ec"`
	DaemonID string `json:"daemon_id"`
	Present  bool   `json:"present"`
}

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
		Atime    int64         `json:"access_time,omitempty"` // access time (nanoseconds since UNIX epoch)
		Size     int64         `json:"size,omitempty"`        // object size (bytes)
		Ver      string        `json:"version,omitempty"`     // object version
		Cksum    *cos.Cksum    `json:"checksum,omitempty"`    // object checksum (NOTE: m.b. cloned)
		CustomMD cos.SimpleKVs `json:"custom,omitempty"`      // custom metadata: ETag, MD5, CRC, user-defined ...
	}
)

// interface guard
var _ ObjAttrsHolder = (*ObjAttrs)(nil)

func (oa *ObjAttrs) String() string {
	return fmt.Sprintf("%dB, v%q, %s, %+v", oa.Size, oa.Ver, oa.Cksum, oa.CustomMD)
}

func (oa *ObjAttrs) SizeBytes(_ ...bool) int64 { return oa.Size }
func (oa *ObjAttrs) Version(_ ...bool) string  { return oa.Ver }
func (oa *ObjAttrs) AtimeUnix() int64          { return oa.Atime }
func (oa *ObjAttrs) Checksum() *cos.Cksum      { return oa.Cksum }
func (oa *ObjAttrs) SetCksum(ty, val string)   { oa.Cksum = cos.NewCksum(ty, val) }

// custom metadata
func (oa *ObjAttrs) GetCustomMD() cos.SimpleKVs   { return oa.CustomMD }
func (oa *ObjAttrs) SetCustomMD(md cos.SimpleKVs) { oa.CustomMD = md }

func (oa *ObjAttrs) GetCustomKey(key string) (val string, exists bool) {
	val, exists = oa.CustomMD[key]
	return
}

func (oa *ObjAttrs) SetCustomKey(k, v string) {
	debug.Assert(k != "")
	if oa.CustomMD == nil {
		oa.CustomMD = make(cos.SimpleKVs, 6)
	}
	oa.CustomMD[k] = v
}

func (oa *ObjAttrs) DelCustomKeys(keys ...string) {
	for _, key := range keys {
		delete(oa.CustomMD, key)
	}
}

// clone ObjAttrsHolder => ObjAttrs (see also lom.CopyAttrs)
func (oa *ObjAttrs) CopyFrom(oah ObjAttrsHolder, skipCksum ...bool) {
	oa.Atime = oah.AtimeUnix()
	oa.Size = oah.SizeBytes()
	oa.Ver = oah.Version()
	if len(skipCksum) == 0 || !skipCksum[0] {
		debug.Assert(oah.Checksum() != nil)
		oa.Cksum = oah.Checksum().Clone() // TODO: checksum by value (***)
	}
	for k, v := range oah.GetCustomMD() {
		oa.SetCustomKey(k, v)
	}
}

//
// to and from HTTP header converters (as in: HEAD /object)
//

func ToHeader(oah ObjAttrsHolder, hdr http.Header) {
	if cksum := oah.Checksum(); !cksum.IsEmpty() {
		hdr.Set(apc.HdrObjCksumType, cksum.Ty())
		hdr.Set(apc.HdrObjCksumVal, cksum.Val())
	}
	if at := oah.AtimeUnix(); at != 0 {
		hdr.Set(apc.HdrObjAtime, cos.UnixNano2S(at))
	}
	if n := oah.SizeBytes(true); n > 0 {
		hdr.Set(HdrContentLength, strconv.FormatInt(n, 10))
	}
	if v := oah.Version(true); v != "" {
		hdr.Set(apc.HdrObjVersion, v)
	}
	custom := oah.GetCustomMD()
	for k, v := range custom {
		debug.Assert(k != "")
		hdr.Add(apc.HdrObjCustomMD, k+"="+v)
	}
}

func (oa *ObjAttrs) ToHeader(hdr http.Header) {
	ToHeader(oa, hdr)
}

// NOTE: returning checksum separately for subsequent validation
func (oa *ObjAttrs) FromHeader(hdr http.Header) (cksum *cos.Cksum) {
	if ty := hdr.Get(apc.HdrObjCksumType); ty != "" {
		val := hdr.Get(apc.HdrObjCksumVal)
		cksum = cos.NewCksum(ty, val)
	}

	if at := hdr.Get(apc.HdrObjAtime); at != "" {
		atime, err := cos.S2UnixNano(at)
		debug.AssertNoErr(err)
		oa.Atime = atime
	}
	if sz := hdr.Get(HdrContentLength); sz != "" {
		size, err := strconv.ParseInt(sz, 10, 64)
		debug.AssertNoErr(err)
		oa.Size = size
	}
	if v := hdr.Get(apc.HdrObjVersion); v != "" {
		oa.Ver = v
	}
	custom := hdr[http.CanonicalHeaderKey(apc.HdrObjCustomMD)]
	for _, v := range custom {
		entry := strings.SplitN(v, "=", 2)
		debug.Assert(len(entry) == 2)
		oa.SetCustomKey(entry[0], entry[1])
	}
	return
}

// local <=> remote equality in the context of cold-GET and download. This function
// decides whether we need to go ahead and re-read the object from its remote location.
//
// Other than a "binary" size and version checks, rest logic goes as follows: objects are
// considered equal if they have a) the same version and at least one matching checksum, or
// b) the same remote "source" and at least one matching checksum, or c) two matching checksums.
// (See also note below.)
//
// Note that mismatch in any given checksum type immediately renders inequality and return
// from the function.
func (oa *ObjAttrs) Equal(rem ObjAttrsHolder) (equal bool) {
	var (
		count   int
		sameVer bool
	)
	if oa.Size != 0 && rem.SizeBytes(true) != 0 && oa.Size != rem.SizeBytes(true) {
		return
	}
	// version check
	var ver string
	if oa.Ver != "" && rem.Version(true) != "" {
		if oa.Ver != rem.Version(true) {
			return
		}
		sameVer = true
		ver = oa.Ver
	} else if remMeta, ok := rem.GetCustomKey(VersionObjMD); ok && remMeta != "" {
		if locMeta, ok := oa.GetCustomKey(VersionObjMD); ok && locMeta != "" {
			if remMeta != locMeta {
				return
			}
			sameVer = true
			ver = locMeta
		}
	}
	// AIS checksum check
	if rem.Checksum().Equal(oa.Cksum) {
		count++
	}
	// MD5 check
	var md5 string
	if remMeta, ok := rem.GetCustomKey(MD5ObjMD); ok && remMeta != "" {
		if locMeta, ok := oa.GetCustomKey(MD5ObjMD); ok {
			if remMeta != locMeta {
				return
			}
			md5 = locMeta
			count++
		}
	}
	// ETag check
	if remMeta, ok := rem.GetCustomKey(ETag); ok && remMeta != "" {
		if locMeta, ok := oa.GetCustomKey(ETag); ok {
			if remMeta != locMeta {
				return
			}
			if md5 != locMeta && ver != locMeta { // against double-counting
				count++
			}
		}
	}
	// CRC check
	if remMeta, ok := rem.GetCustomKey(CRC32CObjMD); ok && remMeta != "" {
		if locMeta, ok := oa.GetCustomKey(CRC32CObjMD); ok {
			if remMeta != locMeta {
				return
			}
			count++
		}
	}
	// NOTE: this is not good enough - need config knob (w/ default=false) to
	//       explicitly allow the "source" to play such role.
	if count == 1 {
		if sameVer {
			count++
		}
		// remote source check
		if remMeta, ok := rem.GetCustomKey(SourceObjMD); ok {
			if locMeta, ok := oa.GetCustomKey(SourceObjMD); ok && remMeta == locMeta {
				count++
			}
		}
	}
	equal = count >= 2
	return
}
