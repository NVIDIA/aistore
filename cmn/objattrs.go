// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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
	// 3rd party backend providers
	SourceObjMD = "source"

	// downloader' source is "web"
	WebObjMD = "web"

	// system-supported custom attrs
	// NOTE: for provider specific HTTP headers, see cmn/cos/const_http.go

	VersionObjMD = "version" // "generation" for GCP, "version" for AWS but only if the bucket is versioned, etc.
	CRC32CObjMD  = cos.ChecksumCRC32C
	MD5ObjMD     = cos.ChecksumMD5
	ETag         = cos.HdrETag

	OrigURLObjMD = "orig_url"

	// additional backend
	LastModified = "LastModified"
)

// object properties
// NOTE: embeds system `ObjAttrs` that in turn includes custom user-defined
// NOTE: compare with `apc.LsoMsg`
type ObjectProps struct {
	Bck Bck `json:"bucket"`
	ObjAttrs
	Name     string `json:"name"`
	Location string `json:"location"` // see also `GetPropsLocation`
	Mirror   struct {
		Paths  []string `json:"paths,omitempty"`
		Copies int      `json:"copies,omitempty"`
	} `json:"mirror"`
	EC struct {
		Generation   int64 `json:"generation"`
		DataSlices   int   `json:"data"`
		ParitySlices int   `json:"parity"`
		IsECCopy     bool  `json:"replicated"`
	} `json:"ec"`
	Present bool `json:"present"`
}

// see also apc.HdrObjAtime et al. @ api/apc/const.go (and note that naming must be consistent)
type ObjAttrs struct {
	Cksum    *cos.Cksum `json:"checksum,omitempty"`  // object checksum (cloned)
	CustomMD cos.StrKVs `json:"custom-md,omitempty"` // custom metadata: ETag, MD5, CRC, user-defined ...
	Ver      *string    `json:"version,omitempty"`   // object version
	Atime    int64      `json:"atime,omitempty"`     // access time (nanoseconds since UNIX epoch)
	Size     int64      `json:"size,omitempty"`      // object size (bytes)
}

// interface guard
var _ cos.OAH = (*ObjAttrs)(nil)

// static map: [internal (json) obj prop => canonical http header]
var (
	props2hdr cos.StrKVs
)

func InitObjProps2Hdr() {
	props2hdr = make(cos.StrKVs, 18)

	op := &ObjectProps{}
	err := IterFields(op, func(tag string, _ IterField) (error, bool) {
		name := apc.PropToHeader(tag)
		props2hdr[tag] = name // internal (json) obj prop => canonical http header
		return nil, false
	}, IterOpts{OnlyRead: false})

	debug.Assert(err == nil && len(props2hdr) <= 18, "err: ", err, " len: ", len(props2hdr))
}

// (compare with apc.PropToHeader)
func PropToHeader(prop string) string {
	headerName, ok := props2hdr[prop]
	debug.Assert(ok, "unknown obj prop: ", prop) // NOTE: assuming, InitObjProps2Hdr captures all statically
	return headerName
}

func (oa *ObjAttrs) String() string {
	return fmt.Sprintf("%dB, v%q, %s, %+v", oa.Size, oa.Version(), oa.Cksum, oa.CustomMD)
}

func (oa *ObjAttrs) Lsize(_ ...bool) int64   { return oa.Size }
func (oa *ObjAttrs) AtimeUnix() int64        { return oa.Atime }
func (oa *ObjAttrs) Checksum() *cos.Cksum    { return oa.Cksum }
func (oa *ObjAttrs) SetCksum(ty, val string) { oa.Cksum = cos.NewCksum(ty, val) }

func (oa *ObjAttrs) Version(_ ...bool) string {
	if oa.Ver == nil {
		return ""
	}
	return *oa.Ver
}

func (oa *ObjAttrs) VersionPtr() *string     { return oa.Ver }
func (oa *ObjAttrs) CopyVersion(oah cos.OAH) { oa.Ver = oah.VersionPtr() }

func (oa *ObjAttrs) SetVersion(ver string) {
	if ver == "" {
		oa.Ver = nil
	} else {
		oa.Ver = &ver
	}
}

func (oa *ObjAttrs) SetSize(size int64) {
	debug.Assert(oa.Size == 0)
	oa.Size = size
}

func (oa *ObjAttrs) GetCustomMD() cos.StrKVs   { return oa.CustomMD }
func (oa *ObjAttrs) SetCustomMD(md cos.StrKVs) { oa.CustomMD = md }

func (oa *ObjAttrs) GetCustomKey(key string) (val string, exists bool) {
	val, exists = oa.CustomMD[key]
	return
}

func (oa *ObjAttrs) SetCustomKey(k, v string) {
	debug.Assert(k != "")
	if oa.CustomMD == nil {
		oa.CustomMD = make(cos.StrKVs, 6)
	}
	oa.CustomMD[k] = v
}

func (oa *ObjAttrs) DelCustomKeys(keys ...string) {
	for _, key := range keys {
		delete(oa.CustomMD, key)
	}
}

// clone OAH => ObjAttrs (see also lom.CopyAttrs)
func (oa *ObjAttrs) CopyFrom(oah cos.OAH, skipCksum bool) {
	oa.Atime = oah.AtimeUnix()
	oa.Size = oah.Lsize()
	oa.CopyVersion(oah)
	if !skipCksum {
		oa.Cksum = oah.Checksum().Clone()
	}
	for k, v := range oah.GetCustomMD() {
		oa.SetCustomKey(k, v)
	}
}

//
// to and from HTTP header converters (as in: HEAD /object)
//

// may set headers:
// - standard cos.HdrContentLength ("Content-Length") & cos.HdrETag ("ETag")
// - atime, version, etc. - all the rest "ais-" prefixed
func ToHeader(oah cos.OAH, hdr http.Header, size int64, cksums ...*cos.Cksum) {
	var cksum *cos.Cksum
	if len(cksums) > 0 {
		// - range checksum, or
		// - archived file checksum, or
		// - object checksum (when read range is _not_ checksummed)
		cksum = cksums[0]
	} else {
		cksum = oah.Checksum()
	}
	if !cksum.IsEmpty() {
		hdr.Set(apc.HdrObjCksumType, cksum.Ty())
		hdr.Set(apc.HdrObjCksumVal, cksum.Val())
	}
	if at := oah.AtimeUnix(); at != 0 {
		hdr.Set(apc.HdrObjAtime, cos.UnixNano2S(at))
	}
	if size > 0 {
		// "response to a HEAD method should not have a body", and so
		// using "Content-Length" to deliver object size (attribute)
		// may look controversial (and it is), but s3 does it, etc.
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
		hdr.Set(cos.HdrContentLength, strconv.FormatInt(size, 10))
	}
	if v := oah.Version(true); v != "" {
		hdr.Set(apc.HdrObjVersion, v)
	}
	custom := oah.GetCustomMD()
	for k, v := range custom {
		hdr.Add(apc.HdrObjCustomMD, k+"="+v)
		if k == ETag {
			// TODO: redundant vs CustomMD - maybe extend cos.OAH to include get/set(ETag)
			hdr.Set(cos.HdrETag, v)
		}
	}
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
	if sz := hdr.Get(cos.HdrContentLength); sz != "" {
		size, err := strconv.ParseInt(sz, 10, 64)
		debug.AssertNoErr(err)
		oa.Size = size
	}
	if v := hdr.Get(apc.HdrObjVersion); v != "" {
		oa.Ver = &v
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
func (oa *ObjAttrs) CheckEq(rem cos.OAH) error {
	var (
		ver      string
		md5      string
		etag     string
		cksumVal string
		count    int
		sameEtag bool
	)
	// size check
	if remSize := rem.Lsize(true); oa.Size != 0 && remSize != 0 && oa.Size != remSize {
		return fmt.Errorf("size %d != %d remote", oa.Size, remSize)
	}

	// version check
	if remVer, v := rem.Version(true), oa.Version(); remVer != "" && v != "" {
		if v != remVer {
			return fmt.Errorf("version %s != %s remote", oa.Version(), remVer)
		}
		ver = v
		// NOTE: ais own version is, currently, a nonunique sequence number - not counting
		if remSrc, _ := rem.GetCustomKey(SourceObjMD); remSrc != apc.AIS {
			count++
		}
	} else if remMeta, ok := rem.GetCustomKey(VersionObjMD); ok && remMeta != "" {
		if locMeta, ok := oa.GetCustomKey(VersionObjMD); ok && locMeta != "" {
			if remMeta != locMeta {
				return fmt.Errorf("version-md %s != %s remote", locMeta, remMeta)
			}
			count++
			ver = locMeta
		}
	}

	// checksum check
	if a, b := rem.Checksum(), oa.Cksum; !a.IsEmpty() && !b.IsEmpty() && a.Ty() == b.Ty() {
		if !a.Equal(b) {
			return fmt.Errorf("%s checksum %s != %s remote", a.Ty(), b, a)
		}
		cksumVal = a.Val()
		count++
	}

	// custom MD: ETag check
	if remMeta, ok := rem.GetCustomKey(ETag); ok && remMeta != "" {
		if locMeta, ok := oa.GetCustomKey(ETag); ok && locMeta != "" {
			if remMeta != locMeta {
				return fmt.Errorf("ETag %s != %s remote", locMeta, remMeta)
			}
			etag = locMeta
			if ver != locMeta && cksumVal != locMeta { // against double-counting
				count++
				sameEtag = true
			}
		}
	}
	// custom MD: CRC check
	if remMeta, ok := rem.GetCustomKey(CRC32CObjMD); ok && remMeta != "" {
		if locMeta, ok := oa.GetCustomKey(CRC32CObjMD); ok && locMeta != "" {
			if remMeta != locMeta {
				return fmt.Errorf("CRC32C %s != %s remote", locMeta, remMeta)
			}
			if cksumVal != locMeta {
				count++
			}
		}
	}

	// custom MD: MD5 check iff count < 2
	// (ETag ambiguity, see: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.htm)
	if !sameEtag {
		if remMeta, ok := rem.GetCustomKey(MD5ObjMD); ok && remMeta != "" {
			if locMeta, ok := oa.GetCustomKey(MD5ObjMD); ok && locMeta != "" {
				if remMeta != locMeta {
					return fmt.Errorf("MD5 %s != %s remote", locMeta, remMeta)
				}
				md5 = locMeta
				if etag != md5 && cksumVal != md5 {
					count++ //  (ditto)
				}
			}
		}
	}

	switch {
	case count >= 2: // e.g., equal because they have the same (version & md5, where version != md5)
		return nil
	case count == 0:
	default:
		// same version or ETag from the same (remote) backend
		// (arguably, must be configurable)
		if remMeta, ok := rem.GetCustomKey(SourceObjMD); ok && remMeta != "" {
			if locMeta, ok := oa.GetCustomKey(SourceObjMD); ok && locMeta != "" {
				if (ver != "" || etag != "") && remMeta == locMeta {
					return nil
				}
			}
		}
	}

	return fmt.Errorf("local (%v) vs remote (%v)", oa.GetCustomMD(), rem.GetCustomMD())
}
