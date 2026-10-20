// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"github.com/NVIDIA/aistore/cmn/binpack"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Binary pack and unpack of the object attributes (str: see cmn/binpack)
//
//	attrs: | size int64 | atime int64 | cksum-type str | cksum-value str | version str | (key str, value str)... | "" |

func (oa *ObjAttrs) PackedAttrsSize() int {
	ty, val := attrsCksum(oa)
	n := cos.SizeofI64 + cos.SizeofI64 + binpack.StrSize(ty) + binpack.StrSize(val) + binpack.StrSize(oa.Version())
	for k, v := range oa.GetCustomMD() {
		n += binpack.StrSize(k) + binpack.StrSize(v)
	}
	return n + binpack.StrSize("") // term
}

func (oa *ObjAttrs) InsAttrs(off int, to []byte) int {
	off = binpack.InsInt64(off, to, oa.Size)
	off = binpack.InsInt64(off, to, oa.Atime)
	ty, val := attrsCksum(oa)
	off = binpack.InsString(off, to, ty)
	off = binpack.InsString(off, to, val)
	off = binpack.InsString(off, to, oa.Version())
	for k, v := range oa.GetCustomMD() {
		debug.Assert(k != "")
		off = binpack.InsString(off, to, k)
		off = binpack.InsString(off, to, v)
	}
	return binpack.InsString(off, to, "") // term
}

func (oa *ObjAttrs) ExtAttrs(d *binpack.Decoder, prev *ObjAttrs) {
	var prevCksumTyp, prevVersion string
	if prev != nil {
		if cksum := prev.Checksum(); cksum != nil {
			prevCksumTyp = cksum.Ty()
		}
		prevVersion = prev.Version()
	}

	oa.Size = d.Int64()
	oa.Atime = d.Int64()
	cksumTyp := d.StrCached(prevCksumTyp)
	cksumVal := d.Str()
	version := d.StrCached(prevVersion)
	if d.Bad() {
		return
	}

	oa.SetCksum(cksumTyp, cksumVal)
	oa.SetVersion(version)
	for {
		// custom-MD keys are never cached
		k := d.Str()
		if d.Bad() || k == "" {
			return
		}
		v := d.Str()
		if d.Bad() {
			return
		}
		oa.SetCustomKey(k, v)
	}
}

func attrsCksum(oa *ObjAttrs) (ty, val string) {
	if cksum := oa.Checksum(); cksum != nil {
		ty, val = cksum.Ty(), cksum.Val()
	}
	return ty, val
}
