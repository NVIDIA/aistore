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

const (
	badDataCksumPrefix = "BAD DATA CHECKSUM:"
	badMetaCksumPrefix = "BAD META CHECKSUM:"
	MLCG32             = 1103515245 // xxhash seed
)

type (
	BadCksumError struct {
		prefix  string
		a, b    interface{}
		context string
	}
)

func NewBadDataCksumError(a, b *Cksum, context ...string) error {
	ctx := ""
	if len(context) > 0 {
		ctx = context[0]
	}
	return &BadCksumError{prefix: badDataCksumPrefix, a: a, b: b, context: ctx}
}
func NewBadMetaCksumError(a, b uint64, context ...string) error {
	ctx := ""
	if len(context) > 0 {
		ctx = context[0]
	}
	return &BadCksumError{prefix: badMetaCksumPrefix, a: a, b: b, context: ctx}
}

func (e *BadCksumError) Error() string {
	context := ""
	if e.context != "" {
		context = fmt.Sprintf(" (context: %s)", e.context)
	}

	_, ok1 := e.a.(*Cksum)
	_, ok2 := e.b.(*Cksum)
	if ok1 && ok2 {
		if e.a != nil && e.b == nil {
			return fmt.Sprintf("%s (%s != %v)%s", e.prefix, e.a, e.b, context)
		} else if e.a == nil && e.b != nil {
			return fmt.Sprintf("%s (%v != %s)%s", e.prefix, e.a, e.b, context)
		} else if e.a == nil && e.b == nil {
			return fmt.Sprintf("%s (nil != nil)%s", e.prefix, context)
		}
		t1, v1 := e.a.(*Cksum).Get()
		t2, v2 := e.b.(*Cksum).Get()
		if t1 == t2 {
			return fmt.Sprintf("%s %s(%s != %s)%s", e.prefix, t1, v1, v2, context)
		}
	}

	return fmt.Sprintf("%s (%s != %s)%s", e.prefix, e.a, e.b, context)
}

func NewCRC32C() hash.Hash {
	return crc32.New(crc32.MakeTable(crc32.Castagnoli))
}

type (
	Cksum struct {
		ty    string
		value string
	}
)

func NewCksum(ty, value string) *Cksum {
	if ty == "" || value == "" {
		return nil
	}
	if ty != ChecksumXXHash && ty != ChecksumMD5 && ty != ChecksumCRC32C {
		AssertMsg(false, fmt.Sprintf("invalid checksum type: %s (with value of: %s)", ty, value))
	}
	return &Cksum{ty, value}
}

func HashToStr(h hash.Hash) string {
	return hex.EncodeToString(h.Sum(nil))
}

func EqCksum(a, b *Cksum) bool {
	if a == nil || b == nil {
		return false
	}
	t1, v1 := a.Get()
	t2, v2 := b.Get()
	return t1 == t2 && v1 == v2
}

func (v *Cksum) Get() (string, string) { return v.ty, v.value }
func (v *Cksum) Type() string          { return v.ty }
func (v *Cksum) Value() string         { return v.value }
func (v *Cksum) String() string {
	return fmt.Sprintf("(%s,%s...)", v.ty, v.value[:Min(10, len(v.value))])
}
