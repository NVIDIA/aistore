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
	Ver      string     `json:"version,omitempty"`   // object version
	Atime    int64      `json:"atime,omitempty"`     // access time (nanoseconds since UNIX epoch)
	Size     int64      `json:"size,omitempty"`      // object size (bytes)
}

// interface guard
var _ cos.OAH = (*ObjAttrs)(nil)

func (oa *ObjAttrs) String() string {
	return fmt.Sprintf("%dB, v%q, %s, %+v", oa.Size, oa.Ver, oa.Cksum, oa.CustomMD)
}

func (oa *ObjAttrs) SizeBytes(_ ...bool) int64 { return oa.Size }
func (oa *ObjAttrs) Version(_ ...bool) string  { return oa.Ver }
func (oa *ObjAttrs) AtimeUnix() int64          { return oa.Atime }
func (oa *ObjAttrs) Checksum() *cos.Cksum      { return oa.Cksum }
func (oa *ObjAttrs) SetCksum(ty, val string)   { oa.Cksum = cos.NewCksum(ty, val) }

func (oa *ObjAttrs) SetSize(size int64) {
	debug.Assert(oa.Size == 0)
	oa.Size = size
}

//
// custom metadata
//

func CustomMD2S(md cos.StrKVs) string { return fmt.Sprintf("%+v", md) }

func S2CustomMD(custom, version string) (md cos.StrKVs) {
	if len(custom) < 8 || !strings.HasPrefix(custom, "map[") { // Sprintf above
		return nil
	}
	s := custom[4 : len(custom)-1]
	lst := strings.Split(s, " ")
	md = make(cos.StrKVs, len(lst))
	md[VersionObjMD] = version
	parseCustom(md, lst, SourceObjMD)
	parseCustom(md, lst, CRC32CObjMD)
	parseCustom(md, lst, MD5ObjMD)
	parseCustom(md, lst, ETag)
	return md
}

func parseCustom(md cos.StrKVs, lst []string, key string) {
	keyX := key + ":"
	for _, kv := range lst {
		if strings.HasPrefix(kv, keyX) {
			md[key] = kv[len(keyX):]
			return
		}
	}
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
	oa.Size = oah.SizeBytes()
	oa.Ver = oah.Version()
	if !skipCksum {
		debug.Assert(oah.Checksum() != nil, oah.String())
		oa.Cksum = oah.Checksum().Clone() // checksum by value (***)
	}
	for k, v := range oah.GetCustomMD() {
		oa.SetCustomKey(k, v)
	}
}

//
// to and from HTTP header converters (as in: HEAD /object)
//

func ToHeader(oah cos.OAH, hdr http.Header) {
	if cksum := oah.Checksum(); !cksum.IsEmpty() {
		hdr.Set(apc.HdrObjCksumType, cksum.Ty())
		hdr.Set(apc.HdrObjCksumVal, cksum.Val())
	}
	if at := oah.AtimeUnix(); at != 0 {
		hdr.Set(apc.HdrObjAtime, cos.UnixNano2S(at))
	}
	if n := oah.SizeBytes(true); n > 0 {
		hdr.Set(cos.HdrContentLength, strconv.FormatInt(n, 10))
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

func (oa *ObjAttrs) FromLsoEntry(e *LsoEntry) {
	oa.Size = e.Size
	oa.Ver = e.Version

	// entry.Custom = cmn.CustomMD2S(custom)
	_ = CustomMD2S(nil)
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
func (oa *ObjAttrs) Equal(rem cos.OAH) (eq bool) {
	var (
		ver      string
		md5      string
		etag     string
		cksumVal string
		count    int
		sameEtag bool
	)
	// size check
	if remSize := rem.SizeBytes(true); oa.Size != 0 && remSize != 0 && oa.Size != remSize {
		return false
	}

	// version check
	if remVer := rem.Version(true); oa.Ver != "" && remVer != "" {
		if oa.Ver != remVer {
			return false
		}
		ver = oa.Ver
		// NOTE: ais own version is, currently, a nonunique sequence number - not counting
		if remSrc, _ := rem.GetCustomKey(SourceObjMD); remSrc != apc.AIS {
			count++
		}
	} else if remMeta, ok := rem.GetCustomKey(VersionObjMD); ok && remMeta != "" {
		if locMeta, ok := oa.GetCustomKey(VersionObjMD); ok && locMeta != "" {
			if remMeta != locMeta {
				return false
			}
			count++
			ver = locMeta
		}
	}

	// checksum check
	if a, b := rem.Checksum(), oa.Cksum; !a.IsEmpty() && !b.IsEmpty() && a.Ty() == b.Ty() {
		if !a.Equal(b) {
			return false
		}
		cksumVal = a.Val()
		count++
	}

	// custom MD: ETag check
	if remMeta, ok := rem.GetCustomKey(ETag); ok && remMeta != "" {
		if locMeta, ok := oa.GetCustomKey(ETag); ok && locMeta != "" {
			if remMeta != locMeta {
				return false
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
				return false
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
					return
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
		return true
	case count == 0:
		return false
	default:
		// same version or ETag from the same (remote) backend
		// (arguably, must be configurable)
		if remMeta, ok := rem.GetCustomKey(SourceObjMD); ok && remMeta != "" {
			if locMeta, ok := oa.GetCustomKey(SourceObjMD); ok && locMeta != "" {
				if (ver != "" || etag != "") && remMeta == locMeta {
					return true
				}
			}
		}
	}
	return eq
}
