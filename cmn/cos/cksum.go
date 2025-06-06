// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"crypto/md5" //nolint:gosec // G501 have to support Cloud's MD5
	"crypto/sha256"
	"crypto/sha512"
	"encoding"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"sort"

	onexxh "github.com/OneOfOne/xxhash"
	cesxxh "github.com/cespare/xxhash/v2"
	jsoniter "github.com/json-iterator/go"
)

// [NOTE]
// - currently, we have only two crypto-secure types: sha256 and sha512 (SHA-2 family)
// - see related object comparison logic in cmn/objattrs
// - now that SHA-3 is in the standard library, it can be easily added (as in: ck.H = sha3.New512())
//   not adding it yet, though, as there's no pressing need

// supported checksums
const (
	ChecksumNone   = "none"
	ChecksumOneXxh = "xxhash"
	ChecksumCesXxh = "xxhash2"
	ChecksumMD5    = "md5"
	ChecksumCRC32C = "crc32c"
	ChecksumSHA256 = "sha256" // crypto.SHA512_256 (SHA-2)
	ChecksumSHA512 = "sha512" // crypto.SHA512 (SHA-2)
)

const (
	badDataCksumPrefix = "BAD DATA CHECKSUM:"
	badMetaCksumPrefix = "BAD META CHECKSUM:"
)

type (
	noopHash struct{}

	// in-cluster checksum validation (compare with cmn.ErrInvalidCksum)
	ErrBadCksum struct {
		prefix  string
		a, b    any
		context string
	}
	Cksum struct {
		ty    string `json:"-"` // Without "json" tag, IterFields function panics
		value string `json:"-"`
	}
	CksumHash struct {
		Cksum
		H   hash.Hash
		sum []byte
	}
	CksumHashSize struct {
		CksumHash
		Size int64
	}
)

var checksums = StrSet{
	ChecksumNone:   {},
	ChecksumOneXxh: {},
	ChecksumCesXxh: {},
	ChecksumMD5:    {},
	ChecksumCRC32C: {},
	ChecksumSHA256: {},
	ChecksumSHA512: {},
}

// interface guard
var (
	_ hash.Hash                  = (*noopHash)(nil)
	_ encoding.BinaryUnmarshaler = (*noopHash)(nil)
	_ encoding.BinaryUnmarshaler = (*noopHash)(nil)

	_ io.Writer = (*CksumHashSize)(nil)
)

var NoneCksum = NewCksum(ChecksumNone, "")

///////////////
// CksumHash //
///////////////

// convenience method (compare with xxhash.Checksum64S)
func ChecksumB2S(in []byte, ty string) string {
	cksum := NewCksumHash(ty)
	cksum.H.Write(in)
	cksum.Finalize()
	return cksum.Val()
}

func NewCksumHash(ty string) (ck *CksumHash) {
	ck = &CksumHash{}
	ck.Init(ty)
	return
}

func (ck *Cksum) MarshalJSON() ([]byte, error) {
	if ck == nil {
		return nil, nil
	}
	return jsoniter.Marshal(struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	}{Type: ck.ty, Value: ck.value})
}

func (ck *CksumHash) Init(ty string) {
	Assert(ck.H == nil)
	ck.ty = ty
	switch ty {
	case ChecksumNone, "":
		ck.ty, ck.H = ChecksumNone, newNoopHash()
	case ChecksumOneXxh:
		ck.H = onexxh.New64()
	case ChecksumCesXxh:
		ck.H = cesxxh.New()
	case ChecksumMD5:
		ck.H = md5.New() //nolint:gosec // G401 ditto (see G501 above)
	case ChecksumCRC32C:
		ck.H = NewCRC32C()
	case ChecksumSHA256:
		ck.H = sha256.New()
	case ChecksumSHA512:
		ck.H = sha512.New()
	default:
		AssertMsg(false, "unknown checksum type: "+ty)
	}
}

// NOTE [caution]: empty checksums are equal
func (ck *CksumHash) Equal(to *Cksum) bool { return ck.Cksum.Equal(to) }

func (ck *CksumHash) Sum() []byte { return ck.sum }

func (ck *CksumHash) Finalize() {
	ck.sum = ck.H.Sum(nil)
	ck.value = hex.EncodeToString(ck.sum)
}

///////////////////
// CksumHashSize //
///////////////////

func (ck *CksumHashSize) Write(b []byte) (n int, err error) {
	n, err = ck.H.Write(b)
	ck.Size += int64(n)
	return
}

///////////
// Cksum //
///////////

func (ck *Cksum) IsEmpty() bool {
	return ck == nil || ck.ty == "" || ck.ty == ChecksumNone || ck.value == ""
}

func NewCksum(ty, value string) *Cksum {
	if err := ValidateCksumType(ty, true /*empty OK*/); err != nil {
		AssertMsg(false, err.Error())
	}
	if ty == "" {
		Assert(value == "")
	}
	return &Cksum{ty, value}
}

// NOTE [caution]: empty checksums are also equal (compare with lom.EqCksum and friends)
func (ck *Cksum) Equal(to *Cksum) bool {
	if ck.IsEmpty() || to.IsEmpty() {
		return ck.IsEmpty() == to.IsEmpty()
	}
	return ck.ty == to.ty && ck.value == to.value
}

func (ck *Cksum) Get() (string, string) {
	if ck == nil {
		return ChecksumNone, ""
	}
	return ck.ty, ck.value
}

func (ck *Cksum) Ty() string { return ck.ty }

func (ck *Cksum) Type() string {
	if ck == nil {
		return ChecksumNone
	}
	return ck.ty
}

func (ck *Cksum) Val() string { return ck.value }

func (ck *Cksum) Value() string {
	if ck == nil {
		return ""
	}
	return ck.value
}

func (ck *Cksum) Clone() *Cksum {
	return &Cksum{value: ck.Value(), ty: ck.Type()}
}

func (ck *Cksum) String() string {
	if ck == nil {
		return "checksum <nil>"
	}
	if ck.ty == "" || ck.ty == ChecksumNone {
		return "checksum <none>"
	}
	return ck.ty + "[" + SHead(ck.value) + "]"
}

//
// helpers
//

func NewCRC32C() hash.Hash {
	return crc32.New(crc32.MakeTable(crc32.Castagnoli))
}

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

func ValidateCksumType(ty string, emptyOK ...bool) (err error) {
	if ty == "" && len(emptyOK) > 0 && emptyOK[0] {
		return
	}
	if !checksums.Contains(ty) {
		err = fmt.Errorf("invalid checksum type %q (expecting %v)", ty, SupportedChecksums())
	}
	return
}

//
// noopHash
//

func newNoopHash() hash.Hash                     { return &noopHash{} }
func (*noopHash) Write(b []byte) (int, error)    { return len(b), nil }
func (*noopHash) Sum([]byte) []byte              { return nil }
func (*noopHash) Reset()                         {}
func (*noopHash) Size() int                      { return 0 }
func (*noopHash) BlockSize() int                 { return KiB }
func (*noopHash) MarshalBinary() ([]byte, error) { return nil, nil }
func (*noopHash) UnmarshalBinary([]byte) error   { return nil }

//
// errors
//

func NewErrDataCksum(a, b *Cksum, context ...string) error {
	ctx := ""
	if len(context) > 0 {
		ctx = context[0]
	}
	return &ErrBadCksum{prefix: badDataCksumPrefix, a: a, b: b, context: ctx}
}

func NewErrMetaCksum(a, b uint64, context ...string) error {
	ctx := ""
	if len(context) > 0 {
		ctx = context[0]
	}
	return &ErrBadCksum{prefix: badMetaCksumPrefix, a: a, b: b, context: ctx}
}

func (e *ErrBadCksum) Error() string {
	var context string
	if e.context != "" {
		context = " (context: " + e.context + ")"
	}
	cka, ok1 := e.a.(*Cksum)
	ckb, ok2 := e.b.(*Cksum)
	if ok1 && ok2 {
		switch {
		case cka != nil && ckb == nil:
			return fmt.Sprintf("%s (%s != %v)%s", e.prefix, cka, ckb, context)
		case cka == nil && ckb != nil:
			return fmt.Sprintf("%s (%v != %s)%s", e.prefix, cka, ckb, context)
		case cka == nil && ckb == nil:
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

func IsErrBadCksum(err error) bool {
	_, ok := err.(*ErrBadCksum)
	return ok
}
