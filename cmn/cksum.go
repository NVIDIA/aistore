// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"

	"github.com/OneOfOne/xxhash"
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
	Cksum struct {
		ty    string
		value string
	}
	CksumHash struct {
		Cksum
		H   hash.Hash
		sum []byte
	}
)

var (
	checksums = map[string]struct{}{
		ChecksumNone:   {},
		ChecksumXXHash: {},
		ChecksumMD5:    {},
		ChecksumCRC32C: {},
	}
)

///////////
// Cksum //
///////////

func NewCksum(ty, value string) *Cksum {
	if ty == "" || value == "" {
		return nil
	}
	if _, ok := checksums[ty]; !ok {
		AssertMsg(false, "invalid checksum type: '"+ty+"'")
	}
	return &Cksum{ty, value}
}

func NewCksumHash(ty string) *CksumHash {
	switch ty {
	case ChecksumNone:
		return nil
	case ChecksumXXHash:
		return &CksumHash{Cksum{ty: ty}, xxhash.New64(), nil}
	case ChecksumMD5:
		return &CksumHash{Cksum{ty: ty}, md5.New(), nil}
	case ChecksumCRC32C:
		return &CksumHash{Cksum{ty: ty}, NewCRC32C(), nil}
	default:
		AssertMsg(false, "invalid checksum type: '"+ty+"'")
	}
	return nil
}

func (ck *Cksum) Equal(to *Cksum) bool {
	if ck == nil {
		return to == nil
	}
	t1, v1 := ck.Get()
	t2, v2 := to.Get()
	return t1 == t2 && v1 == v2
}

func (ck *Cksum) Get() (string, string) { return ck.ty, ck.value }
func (ck *Cksum) Type() string          { return ck.ty }
func (ck *Cksum) Value() string         { return ck.value }
func (ck *Cksum) Clone() *Cksum         { return &Cksum{ty: ck.ty, value: ck.value} }
func (ck *Cksum) String() string {
	if ck == nil {
		return "checksum <nil>"
	}
	return fmt.Sprintf("(%s,%s...)", ck.ty, ck.value[:Min(10, len(ck.value))])
}

func (ck *CksumHash) Equal(to *Cksum) bool { return ck.Cksum.Equal(to) }
func (ck *CksumHash) Sum() []byte          { return ck.sum }
func (ck *CksumHash) Finalize() {
	ck.sum = ck.H.Sum(nil)
	ck.value = hex.EncodeToString(ck.sum)
}

/////////////
// helpers //
/////////////

func NewCRC32C() hash.Hash {
	return crc32.New(crc32.MakeTable(crc32.Castagnoli))
}

func HashToStr(h hash.Hash) string {
	return hex.EncodeToString(h.Sum(nil))
}

////////////
// errors //
////////////

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
	var context string
	if e.context != "" {
		context = " (context: " + e.context + ")"
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
	return fmt.Sprintf("%s (%v != %v)%s", e.prefix, e.a, e.b, context)
}

func (e *BadCksumError) Is(target error) bool {
	_, ok := target.(*BadCksumError)
	return ok
}
