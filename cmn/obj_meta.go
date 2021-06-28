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

type (
	ObjAttrsHolder interface {
		SizeBytes(special ...bool) int64
		Version(special ...bool) string
		Checksum() *cos.Cksum
		AtimeUnix() int64
		Custom() cos.SimpleKVs
	}
	ObjAttrs struct {
		Atime int64         // access time (nanoseconds since UNIX epoch)
		Size  int64         // object size (bytes)
		Ver   string        // object version
		Cksum *cos.Cksum    // object checksum (NOTE: m.b. cloned)
		AddMD cos.SimpleKVs // custom md
	}
)

// interface guard
var _ ObjAttrsHolder = (*ObjAttrs)(nil)

func (oa *ObjAttrs) SizeBytes(_ ...bool) int64 { return oa.Size }
func (oa *ObjAttrs) Version(_ ...bool) string  { return oa.Ver }
func (oa *ObjAttrs) AtimeUnix() int64          { return oa.Atime }
func (oa *ObjAttrs) Checksum() *cos.Cksum      { return oa.Cksum }
func (oa *ObjAttrs) SetCksum(ty, val string)   { oa.Cksum = cos.NewCksum(ty, val) }
func (oa *ObjAttrs) Custom() cos.SimpleKVs     { return oa.AddMD }

func (oa *ObjAttrs) SetCustom(k, v string) {
	debug.Assert(k != "")
	if oa.AddMD == nil {
		oa.AddMD = make(cos.SimpleKVs, 4)
	}
	oa.AddMD[k] = v
}

// ObjAttrsHolder => ObjAttrs; see also: lom.CopyAttrs
func (oa *ObjAttrs) CopyFrom(oah ObjAttrsHolder, skipCksum ...bool) {
	oa.Atime = oah.AtimeUnix()
	oa.Size = oah.SizeBytes()
	oa.Ver = oah.Version()
	if len(skipCksum) == 0 || !skipCksum[0] {
		debug.Assert(oah.Checksum() != nil)
		oa.Cksum = oah.Checksum().Clone() // TODO: checksum by value
	}
	oa.AddMD = oah.Custom()
}

// ObjAttrsHolder => http header
func ToHTTPHdr(oah ObjAttrsHolder, hdrs ...http.Header) (hdr http.Header) {
	if len(hdrs) > 0 && hdrs[0] != nil {
		hdr = hdrs[0]
	} else {
		hdr = make(http.Header, 6)
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
	for k, v := range oah.Custom() {
		debug.Assert(k != "")
		hdr.Add(HdrObjCustomMD, strings.Join([]string{k, v}, "="))
	}
	return
}
