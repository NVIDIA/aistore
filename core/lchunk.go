// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"

	onexxh "github.com/OneOfOne/xxhash"
)

const (
	umetaver = 1
)

type (
	Uchunk struct {
		Siz      int64
		CksumVal string
	}
	Ufest struct {
		Size     int64
		Num      uint16
		CksumTyp string
		Chunks   []Uchunk
	}
)

// on-disk xattr
const (
	xattrChunk = "user.ais.chunk"
)

const (
	utag = "chunk-manifest"
)

// interface guard
var (
	_ cos.Unpacker = (*Ufest)(nil)
	_ cos.Packer   = (*Ufest)(nil)
)

// TODO -- FIXME: 4K only; reuse memsys
func (u *Ufest) Load(lom *LOM) error {
	b, err := fs.GetXattr(lom.FQN, xattrChunk)
	if err != nil {
		return _ucerr(lom.Cname(), err)
	}
	unpacker := cos.NewUnpacker(b)
	if err := unpacker.ReadAny(u); err != nil {
		return _ucerr(lom.Cname(), err)
	}
	return nil
}

func _ucerr(cname string, err error) error {
	tag := utag + "[" + cname + "]"
	if cos.IsErrXattrNotFound(err) {
		return cmn.NewErrLmetaNotFound(tag, err)
	}
	return os.NewSyscallError(getxattr, fmt.Errorf("%s, err: %w", tag, err))
}

func (u *Ufest) Store(lom *LOM) error {
	// validate
	if u.Num == 0 || int(u.Num) != len(u.Chunks) {
		return fmt.Errorf("%s[%s]: invalid manifest num=%d, len=%d", utag, lom.Cname(), u.Num, len(u.Chunks))
	}
	var total int64
	for i := range u.Num {
		total += u.Chunks[i].Siz
	}
	if u.Size == 0 {
		u.Size = total
	} else if total != u.Size {
		return fmt.Errorf("%s total size mismatch: %d vs %d", utag, total, u.Size)
	}

	// pack
	psize := u.PackedSize()
	if psize > xattrMaxSize { // TODO -- FIXME: see 4K above
		return fmt.Errorf("%s[%s]: manifest too large (%d > %d)", utag, lom.Cname(), psize, xattrMaxSize)
	}

	packer := cos.NewPacker(nil, psize)
	u.Pack(packer)

	// write
	if err := fs.SetXattr(lom.FQN, xattrChunk, packer.Bytes()); err != nil {
		return err
	}

	lom.md.lid.setlmfl(lmflChunk) // TODO -- FIXME: persist
	return nil
}

func (u *Ufest) Pack(packer *cos.BytePack) {
	packer.WriteByte(umetaver)
	packer.WriteInt64(u.Size)
	packer.WriteUint16(u.Num)
	packer.WriteString(u.CksumTyp)
	for _, c := range u.Chunks {
		packer.WriteInt64(c.Siz)
		packer.WriteString(c.CksumVal)
	}
	h := onexxh.Checksum64S(packer.Bytes(), cos.MLCG32)
	packer.WriteUint64(h)
}

func (u *Ufest) PackedSize() (ll int) {
	for _, c := range u.Chunks {
		ll += cos.SizeofI64 + cos.PackedStrLen(c.CksumVal)
	}
	return ll + 1 + cos.SizeofI64 + cos.SizeofI16 + cos.PackedStrLen(u.CksumTyp) + cos.SizeofI64
}

func (u *Ufest) Unpack(unpacker *cos.ByteUnpack) (err error) {
	var metaver byte
	if metaver, err = unpacker.ReadByte(); err != nil {
		return fmt.Errorf("invalid %s (too short)", utag)
	}
	if metaver != umetaver {
		return fmt.Errorf("unsupported %s meta-version %d (expecting %d)", utag, metaver, umetaver)
	}
	if u.Size, err = unpacker.ReadInt64(); err != nil {
		return fmt.Errorf("invalid %s size: %v", utag, err)
	}
	if u.Num, err = unpacker.ReadUint16(); err != nil {
		return fmt.Errorf("invalid %s num: %v", utag, err)
	}
	if u.CksumTyp, err = unpacker.ReadString(); err != nil {
		return fmt.Errorf("invalid %s cksum type: %v", utag, err)
	}

	u.Chunks = make([]Uchunk, u.Num)
	var total int64
	for i := range u.Num {
		c := &u.Chunks[i]
		if c.Siz, err = unpacker.ReadInt64(); err != nil {
			return fmt.Errorf("invalid %s chunk idx %d size: %v", utag, i, err)
		}
		if c.CksumVal, err = unpacker.ReadString(); err != nil {
			return fmt.Errorf("invalid %s chunk idx %d cksum val: %v", utag, i, err)
		}
		total += c.Siz
	}
	if total != u.Size {
		return fmt.Errorf("%s total size mismatch: %d vs %d", utag, total, u.Size)
	}

	var h uint64
	if h, err = unpacker.ReadUint64(); err != nil {
		return fmt.Errorf("%s corrupted: %v", utag, err)
	}
	b := unpacker.Bytes()
	hh := onexxh.Checksum64S(b[:len(b)-cos.SizeofI64], cos.MLCG32)
	if h != hh {
		return cos.NewErrMetaCksum(h, hh, utag)
	}

	return nil
}
