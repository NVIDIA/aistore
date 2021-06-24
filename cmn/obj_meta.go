// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "github.com/NVIDIA/aistore/cmn/cos"

type (
	ObjAttrsHolder interface {
		SizeBytes(special ...bool) int64
		Version(special ...bool) string
		Checksum() *cos.Cksum
		AtimeUnix() int64
		CustomMD() cos.SimpleKVs
	}
	ObjAttrs struct {
		Atime int64         // access time (nanoseconds since UNIX epoch)
		Size  int64         // object size (bytes)
		Ver   string        // object version
		Cksum *cos.Cksum    // object checksum
		AddMD cos.SimpleKVs // custom md
	}
)

// interface guard
var _ ObjAttrsHolder = (*ObjAttrs)(nil)

func (oa *ObjAttrs) SizeBytes(_ ...bool) int64 { return oa.Size }
func (oa *ObjAttrs) Version(_ ...bool) string  { return oa.Ver }
func (oa *ObjAttrs) Checksum() *cos.Cksum      { return oa.Cksum }
func (oa *ObjAttrs) AtimeUnix() int64          { return oa.Atime }
func (*ObjAttrs) CustomMD() cos.SimpleKVs      { return nil }
