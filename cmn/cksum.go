// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"encoding/hex"
	"fmt"
	"hash"
)

//
// typed checksum value
//
type (
	CksumProvider interface {
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

func NewCksum(kind string, val string) CksumProvider {
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

func HashToStr(h hash.Hash) string {
	return hex.EncodeToString(h.Sum(nil))
}

func EqCksum(a, b CksumProvider) bool {
	if a == nil || b == nil {
		return false
	}
	t1, v1 := a.Get()
	t2, v2 := b.Get()
	return t1 == t2 && v1 == v2
}

func (v Cksumvalxxhash) Get() (string, string) { return v.kind, v.val }
func (v Cksumvalmd5) Get() (string, string)    { return v.kind, v.val }
func (v Cksumvalxxhash) String() string {
	return fmt.Sprintf("(%s,%s...)", v.kind, v.val[:Min(8, len(v.val))])
}
func (v Cksumvalmd5) String() string {
	return fmt.Sprintf("(%s,%s...)", v.kind, v.val[:Min(8, len(v.val))])
}
