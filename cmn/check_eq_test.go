// Package cmn provides common types and utilities for AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn_test

import (
	"slices"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// NOTE: duplicated it here for the test
var supportedRemAttrs = [...]string{cmn.VersionObjMD, cmn.CRC32CObjMD, cmn.MD5ObjMD, cmn.ETag, cos.HdrLastModified, cos.HdrContentType}

func TestCheckEqKeys(t *testing.T) {
	for _, key := range append(supportedRemAttrs[:], cmn.SourceObjMD) {
		var a, b cmn.ObjAttrs
		for _, oa := range []*cmn.ObjAttrs{&a, &b} { // the same size and checksum
			oa.Size = 1
			oa.SetCksum(cos.ChecksumOneXxh, "0123456789abcdef")
		}
		a.SetCustomKey(key, "a")
		b.SetCustomKey(key, "b")
		reads, listed := a.CheckEq(&b) != nil, slices.Contains(cmn.CheckEqKeys[:], key)
		if reads != listed {
			t.Errorf("%q: CheckEq reads it: %t, CheckEqKeys has it: %t", key, reads, listed)
		}
	}
}

func TestCheckEq(t *testing.T) {
	const (
		xxh  = "0123456789abcdef"
		crc  = "89abcdef"
		etag = "e7a3c1d5f0b2948e7a3c1d5f0b2948ee" // unquoted, as stored in custom MD
	)
	type md struct {
		size     int64
		ty, val  string // checksum
		ver, tag string // version, ETag
	}
	mk := func(m md) *cmn.ObjAttrs {
		oa := &cmn.ObjAttrs{Size: m.size}
		if m.ty != "" {
			oa.SetCksum(m.ty, m.val)
		}
		if m.ver != "" {
			oa.SetCustomKey(cmn.VersionObjMD, m.ver)
		}
		if m.tag != "" {
			oa.SetCustomKey(cmn.ETag, m.tag)
		}
		return oa
	}
	for _, tc := range []struct {
		name     string
		loc, rem md
		eq       bool
	}{
		// checksum of the same type
		{"same-cksum", md{ty: cos.ChecksumCesXxh, val: xxh}, md{ty: cos.ChecksumCesXxh, val: xxh}, true},
		{"diff-cksum", md{ty: cos.ChecksumCesXxh, val: xxh, ver: "1"}, md{ty: cos.ChecksumCesXxh, val: "fedcba9876543210", ver: "1"}, false},
		{"crc-alone", md{ty: cos.ChecksumCRC32C, val: crc}, md{ty: cos.ChecksumCRC32C, val: crc}, false}, // not trusted
		{"crc-and-version", md{ty: cos.ChecksumCRC32C, val: crc, ver: "1"}, md{ty: cos.ChecksumCRC32C, val: crc, ver: "1"}, true},

		// checksums of different types: no information
		{"cross-type-alone", md{ty: cos.ChecksumCRC32C, val: crc}, md{ty: cos.ChecksumCesXxh, val: xxh}, false},
		{"cross-type-and-version", md{ty: cos.ChecksumCRC32C, val: crc, ver: "1"}, md{ty: cos.ChecksumCesXxh, val: xxh, ver: "1"}, false},
		{"cross-type-version-etag", md{ty: cos.ChecksumCRC32C, val: crc, ver: "1", tag: etag}, md{ty: cos.ChecksumCesXxh, val: xxh, ver: "1", tag: etag}, true},
		{"cross-type-etag", md{ty: cos.ChecksumCRC32C, val: crc, tag: etag}, md{ty: cos.ChecksumCesXxh, val: xxh, tag: etag}, true},

		// other
		{"size", md{size: 1, ver: "1", tag: etag}, md{size: 2, ver: "1", tag: etag}, false},
		{"version", md{ver: "1", tag: etag}, md{ver: "2", tag: etag}, false},
		{"etag", md{tag: etag}, md{tag: "f8b4d2e6a1c3059f8b4d2e6a1c3059ff"}, false},
		{"no-local-md", md{}, md{size: 1, ty: cos.ChecksumCesXxh, val: xxh, ver: "1", tag: etag}, false}, // e.g., not present
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := mk(tc.loc).CheckEq(mk(tc.rem))
			if (err == nil) != tc.eq {
				t.Errorf("expected eq=%t, got %v", tc.eq, err)
			}
		})
	}
}
