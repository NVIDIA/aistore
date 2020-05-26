// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"crypto/md5"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"sort"

	"github.com/OneOfOne/xxhash"
)

// NOTE: not supporting SHA-3 family is its current golang.org/x/crypto/sha3 source
//       doesn't implement BinaryMarshaler & BinaryUnmarshaler interfaces
//       (see also https://golang.org/pkg/encoding)

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
	checksums = StringSet{
		ChecksumNone:   {},
		ChecksumXXHash: {},
		ChecksumMD5:    {},
		ChecksumCRC32C: {},
		ChecksumSHA256: {},
		ChecksumSHA512: {},
	}
)

/////////////
// helpers //
/////////////
func SupportedChecksums() (types []string) {
	types = make([]string, 0, len(checksums))
	for ty := range checksums {
		types = append(types, ty)
	}
	sort.Strings(types)
	for i := range types {
		if types[i] == ChecksumNone {
			copy(types[i:], types[i+1:])
			types[len(types)-1] = ChecksumNone
		}
	}
	return
}

func ValidateCksumType(ty string) (err error) {
	if ty != "" && !checksums.Contains(ty) {
		err = fmt.Errorf("invalid checksum type %q (expecting %v)", ty, SupportedChecksums())
	}
	return
}

///////////
// Cksum //
///////////

func NewCksum(ty, value string) *Cksum {
	if err := ValidateCksumType(ty); err != nil {
		AssertMsg(false, err.Error())
	}
	if ty == "" {
		AssertMsg(value == "", "type='' & value="+value)
		ty = ChecksumNone
	}
	return &Cksum{ty, value}
}

func NewCksumHash(ty string) *CksumHash {
	switch ty {
	case ChecksumNone, "":
		return &CksumHash{Cksum{ty: ChecksumNone}, nil, nil}
	case ChecksumXXHash:
		return &CksumHash{Cksum{ty: ty}, xxhash.New64(), nil}
	case ChecksumMD5:
		return &CksumHash{Cksum{ty: ty}, md5.New(), nil}
	case ChecksumCRC32C:
		return &CksumHash{Cksum{ty: ty}, NewCRC32C(), nil}
	case ChecksumSHA256:
		return &CksumHash{Cksum{ty: ty}, sha512.New512_256(), nil}
	case ChecksumSHA512:
		return &CksumHash{Cksum{ty: ty}, sha512.New(), nil}
	default:
		AssertMsg(false, ValidateCksumType(ty).Error())
	}
	return nil
}

func (ck *Cksum) Equal(to *Cksum) bool {
	if ck == nil || to == nil {
		return false
	}
	t1, v1 := ck.Get()
	if t1 == ChecksumNone || t1 == "" || v1 == "" {
		return false
	}
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
	cka, ok1 := e.a.(*Cksum)
	ckb, ok2 := e.b.(*Cksum)
	if ok1 && ok2 {
		if cka != nil && ckb == nil {
			return fmt.Sprintf("%s (%s != %v)%s", e.prefix, cka, ckb, context)
		} else if cka == nil && ckb != nil {
			return fmt.Sprintf("%s (%v != %s)%s", e.prefix, cka, ckb, context)
		} else if cka == nil && ckb == nil {
			return fmt.Sprintf("%s (nil != nil)%s", e.prefix, context)
		}
		t1, v1 := cka.Get()
		t2, v2 := ckb.Get()
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
