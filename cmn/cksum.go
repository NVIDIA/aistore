// Package cmn provides common low-level types and utilities for all dfcpub projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

//
// typed checksum value
//
type (
	CksumValue interface {
		Get() (string, string)
		String() string
	}
	Cksumvalxxhash struct {
		kind string
		val  string
	}
	Cksumvalmd5 struct {
		kind string
		val  string
	}
)

func NewCksum(kind string, val string) CksumValue {
	if kind == "" {
		return nil
	}
	if val == "" {
		return nil
	}
	if kind == ChecksumXXHash {
		return Cksumvalxxhash{kind, val}
	}
	Assert(kind == ChecksumMD5)
	return Cksumvalmd5{kind, val}
}

func EqCksum(a, b CksumValue) bool {
	if a == nil || b == nil {
		return false
	}
	t1, v1 := a.Get()
	t2, v2 := b.Get()
	return t1 == t2 && v1 == v2
}

func (v Cksumvalxxhash) Get() (string, string) { return v.kind, v.val }
func (v Cksumvalmd5) Get() (string, string)    { return v.kind, v.val }
func (v Cksumvalxxhash) String() string        { return "(" + v.kind + ", " + v.val[:8] + "...)" }
func (v Cksumvalmd5) String() string           { return "(" + v.kind + ", " + v.val[:8] + "...)" }
