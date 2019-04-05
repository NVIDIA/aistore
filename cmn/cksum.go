// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
)

func NewCRC32C() hash.Hash {
	return crc32.New(crc32.MakeTable(crc32.Castagnoli))
}

//
// typed checksum value
//
type (
	Cksummer interface {
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
	Cksumvalcrc32c struct {
		kind string
		val  string
	}
)

func NewCksum(kind string, val string) Cksummer {
	if kind == "" {
		return nil
	}
	if val == "" {
		return nil
	}
	if kind == ChecksumXXHash {
		return Cksumvalxxhash{kind, val}
	}
	if kind == ChecksumMD5 {
		return Cksumvalmd5{kind, val}
	}
	Assert(kind == ChecksumCRC32C)
	return Cksumvalcrc32c{kind, val}
}

func HashToStr(h hash.Hash) string {
	return hex.EncodeToString(h.Sum(nil))
}

func EqCksum(a, b Cksummer) bool {
	if a == nil || b == nil {
		return false
	}
	t1, v1 := a.Get()
	t2, v2 := b.Get()
	return t1 == t2 && v1 == v2
}

func (v Cksumvalxxhash) Get() (string, string) { return v.kind, v.val }
func (v Cksumvalmd5) Get() (string, string)    { return v.kind, v.val }
func (v Cksumvalcrc32c) Get() (string, string) { return v.kind, v.val }
func (v Cksumvalxxhash) String() string {
	return fmt.Sprintf("(%s,%s...)", v.kind, v.val[:Min(8, len(v.val))])
}
func (v Cksumvalmd5) String() string {
	return fmt.Sprintf("(%s,%s...)", v.kind, v.val[:Min(8, len(v.val))])
}
func (v Cksumvalcrc32c) String() string {
	return fmt.Sprintf("(%s,%s...)", v.kind, v.val[:Min(8, len(v.val))])
}
