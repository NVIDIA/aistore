// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

// Metadata - EC information stored in metafiles for every encoded object
type Metadata struct {
	Size       int64            `json:"size"`                      // obj size (after EC'ing sum size of slices differs from the original)
	Version    uint64           `json:"version,omitempty"`         // Metadata version
	ObjCksum   string           `json:"obj_chk"`                   // checksum of the original object
	ObjVersion string           `json:"obj_version,omitempty"`     // object version
	CksumType  string           `json:"slice_ck_type,omitempty"`   // slice checksum type
	CksumValue string           `json:"slice_chk_value,omitempty"` // slice checksum of the slice if EC is used
	Data       int              `json:"data"`                      // the number of data slices
	Parity     int              `json:"parity"`                    // the number of parity slices
	SliceID    int              `json:"sliceid,omitempty"`         // 0 for full replica, 1 to N for slices
	Daemons    cos.MapStrUint16 `json:"daemons,omitempty"`         // Locations of all slices: DaemonID <-> SliceID
	IsCopy     bool             `json:"copy"`                      // object is replicated(true) or encoded(false)
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
	if err := jsoniter.Unmarshal(b, md); err != nil {
		err := fmt.Errorf("damaged metafile %q: %v", fqn, err)
		return nil, err
	}

	return md, nil
}

func (md *Metadata) Clone() *Metadata {
	clone := &Metadata{}
	cos.CopyStruct(clone, md)
	return clone
}

func (md *Metadata) Marshal() []byte {
	return cos.MustMarshal(md)
}

func MetaToString(md *Metadata) string {
	if md == nil {
		return ""
	}
	return string(md.Marshal())
}

func StringToMeta(s string) (*Metadata, error) {
	var md Metadata
	err := jsoniter.Unmarshal([]byte(s), &md)
	if err == nil {
		return &md, nil
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
