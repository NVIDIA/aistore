// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"io/ioutil"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

// Metadata - EC information stored in metafiles for every encoded object
type Metadata struct {
	Size       int64  `json:"size"`                      // size of original file (after EC'ing the total size of slices differs from original)
	ObjCksum   string `json:"obj_chk"`                   // checksum of the original object
	ObjVersion string `json:"obj_version,omitempty"`     // object version
	CksumType  string `json:"slice_ck_type,omitempty"`   // slice checksum type
	CksumValue string `json:"slice_chk_value,omitempty"` // slice checksum of the slice if EC is used
	Data       int    `json:"data"`                      // the number of data slices
	Parity     int    `json:"parity"`                    // the number of parity slices
	SliceID    int    `json:"sliceid,omitempty"`         // 0 for full replica, 1 to N for slices
	IsCopy     bool   `json:"copy"`                      // object is replicated(true) or encoded(false)
}

var (
	// interface guard
	_ cmn.Unpacker = &Metadata{}
	_ cmn.Packer   = &Metadata{}
)

// LoadMetadata loads and parses EC metadata from a file
func LoadMetadata(fqn string) (*Metadata, error) {
	b, err := ioutil.ReadFile(fqn)
	if err != nil {
		err = fmt.Errorf("Failed to read metafile %q: %v", fqn, err)
		return nil, err
	}
	md := &Metadata{}
	if err := jsoniter.Unmarshal(b, md); err != nil {
		err := fmt.Errorf("Damaged metafile %q: %v", fqn, err)
		return nil, err
	}

	return md, nil
}

func (m *Metadata) Marshal() []byte {
	return cmn.MustMarshal(m)
}

func MetaToString(m *Metadata) string {
	if m == nil {
		return ""
	}
	return string(m.Marshal())
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
	fqn, _, err := cluster.HrwFQN(MetaType, bck, objName)
	if err != nil {
		return nil, err
	}
	return LoadMetadata(fqn)
}

func (md *Metadata) Unpack(unpacker *cmn.ByteUnpack) (err error) {
	var (
		i uint16
		b byte
	)
	if md.Size, err = unpacker.ReadInt64(); err != nil {
		return
	}
	i, err = unpacker.ReadUint16()
	if err != nil {
		return
	}
	md.Data = int(i)
	i, err = unpacker.ReadUint16()
	if err != nil {
		return
	}
	md.Parity = int(i)
	i, err = unpacker.ReadUint16()
	if err != nil {
		return
	}
	md.SliceID = int(i)
	b, err = unpacker.ReadByte()
	if err != nil {
		return
	}
	md.IsCopy = b != 0
	if md.ObjCksum, err = unpacker.ReadString(); err != nil {
		return
	}
	if md.ObjVersion, err = unpacker.ReadString(); err != nil {
		return
	}
	if md.CksumType, err = unpacker.ReadString(); err != nil {
		return
	}
	md.CksumValue, err = unpacker.ReadString()
	return
}

func (md *Metadata) Pack(packer *cmn.BytePack) {
	packer.WriteInt64(md.Size)
	packer.WriteUint16(uint16(md.Data))
	packer.WriteUint16(uint16(md.Parity))
	packer.WriteUint16(uint16(md.SliceID))
	if md.IsCopy {
		packer.WriteByte(1)
	} else {
		packer.WriteByte(0)
	}
	packer.WriteString(md.ObjCksum)
	packer.WriteString(md.ObjVersion)
	packer.WriteString(md.CksumType)
	packer.WriteString(md.CksumValue)
}

// int16 is sufficient to keep Data,Parity, and SliceID, so:
//    int64 + 3*int16 + bool + 4 strings
func (md *Metadata) PackedSize() int {
	return cmn.SizeofI64 + cmn.SizeofI16*3 + 1 + cmn.SizeofLen*4 +
		len(md.ObjCksum) + len(md.ObjVersion) + len(md.CksumType) + len(md.CksumValue)
}
