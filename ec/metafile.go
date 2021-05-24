// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"io"
	"os"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Metadata - EC information stored in metafiles for every encoded object
type Metadata struct {
	Size       int64            // obj size (after EC'ing sum size of slices differs from the original)
	Version    uint64           // Metadata version
	ObjCksum   string           // checksum of the original object
	ObjVersion string           // object version
	CksumType  string           // slice checksum type
	CksumValue string           // slice checksum of the slice if EC is used
	Data       int              // the number of data slices
	Parity     int              // the number of parity slices
	SliceID    int              // 0 for full replica, 1 to N for slices
	Daemons    cos.MapStrUint16 // Locations of all slices: DaemonID <-> SliceID
	IsCopy     bool             // object is replicated(true) or encoded(false)
}

// interface guard
var (
	_ cos.Unpacker = (*Metadata)(nil)
	_ cos.Packer   = (*Metadata)(nil)
)

// LoadMetadata loads and parses EC metadata from a file
func LoadMetadata(fqn string) (*Metadata, error) {
	b, err := os.ReadFile(fqn)
	if err != nil {
		return nil, err
	}
	md := &Metadata{}
	unpacker := cos.NewUnpacker(b)
	if err := unpacker.ReadAny(md); err != nil {
		err := fmt.Errorf("damaged metafile %q: %v", fqn, err)
		return nil, err
	}

	return md, nil
}

func MetaFromReader(reader io.Reader) (*Metadata, error) {
	b, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	md := &Metadata{}
	unpacker := cos.NewUnpacker(b)
	err = unpacker.ReadAny(md)
	return md, err
}

// Do not use MM.SGL for a byte buffer: as the buffer is sent via
// HTTP, it can result in hard to debug errors when SGL is freed.
// For details:  https://gitlab-master.nvidia.com/aistorage/aistore/issues/472#note_4212419
func (md *Metadata) NewPack() []byte {
	var (
		buf []byte
		l   = md.PackedSize()
	)
	packer := cos.NewPacker(buf, l)
	packer.WriteAny(md)
	return packer.Bytes()
}

func (md *Metadata) Clone() *Metadata {
	clone := &Metadata{}
	cos.CopyStruct(clone, md)
	return clone
}

func MetaToString(md *Metadata) string {
	if md == nil {
		return ""
	}
	return string(md.NewPack())
}

func StringToMeta(s string) (*Metadata, error) {
	md := &Metadata{}
	unpacker := cos.NewUnpacker([]byte(s))
	err := unpacker.ReadAny(md)
	if err == nil {
		return md, nil
	}
	return nil, err
}

// ObjectMetadata returns metadata for an object or its slice if any exists
func ObjectMetadata(bck *cluster.Bck, objName string) (*Metadata, error) {
	fqn, _, err := cluster.HrwFQN(bck, MetaType, objName)
	if err != nil {
		return nil, err
	}
	return LoadMetadata(fqn)
}

func (md *Metadata) Unpack(unpacker *cos.ByteUnpack) (err error) {
	var i uint16
	if md.Size, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if i, err = unpacker.ReadUint16(); err != nil {
		return
	}
	md.Data = int(i)
	if i, err = unpacker.ReadUint16(); err != nil {
		return
	}
	md.Parity = int(i)
	if i, err = unpacker.ReadUint16(); err != nil {
		return
	}
	md.SliceID = int(i)
	if md.IsCopy, err = unpacker.ReadBool(); err != nil {
		return
	}
	if md.ObjCksum, err = unpacker.ReadString(); err != nil {
		return
	}
	if md.ObjVersion, err = unpacker.ReadString(); err != nil {
		return
	}
	if md.CksumType, err = unpacker.ReadString(); err != nil {
		return
	}
	if md.CksumValue, err = unpacker.ReadString(); err != nil {
		return
	}
	if md.Version, err = unpacker.ReadUint64(); err != nil {
		return
	}
	md.Daemons, err = unpacker.ReadMapStrUint16()
	return
}

func (md *Metadata) Pack(packer *cos.BytePack) {
	packer.WriteInt64(md.Size)
	packer.WriteUint16(uint16(md.Data))
	packer.WriteUint16(uint16(md.Parity))
	packer.WriteUint16(uint16(md.SliceID))
	packer.WriteBool(md.IsCopy)
	packer.WriteString(md.ObjCksum)
	packer.WriteString(md.ObjVersion)
	packer.WriteString(md.CksumType)
	packer.WriteString(md.CksumValue)
	packer.WriteUint64(md.Version)
	packer.WriteMapStrUint16(md.Daemons)
}

func (md *Metadata) PackedSize() int {
	daemonListSz := cos.SizeofLen
	for k := range md.Daemons {
		daemonListSz += cos.PackedStrLen(k) + cos.SizeofI16
	}
	return cos.SizeofI64 + cos.SizeofI16*3 + 1 +
		cos.PackedStrLen(md.ObjCksum) + cos.PackedStrLen(md.ObjVersion) +
		cos.PackedStrLen(md.CksumType) + cos.PackedStrLen(md.CksumValue) +
		cos.SizeofI64 + daemonListSz
}
