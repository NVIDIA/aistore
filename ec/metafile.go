// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"io"
	"os"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/OneOfOne/xxhash"
)

const MDVersionLast = 1 // current version of metadata

// Metadata - EC information stored in metafiles for every encoded object
type Metadata struct {
	Size        int64            `json:"obj_size"`      // obj size (after EC'ing sum size of slices differs from the original)
	Generation  int64            `json:"generation"`    // Timestamp when the object was EC'ed
	ObjCksum    string           `json:"obj_cksum"`     // checksum of the original object
	ObjVersion  string           `json:"obj_version"`   // object version
	CksumType   string           `json:"cksum_type"`    // slice checksum type
	CksumValue  string           `json:"slice_cksum"`   // slice checksum of the slice if EC is used
	FullReplica string           `json:"replica_node"`  // daemon ID where full(main) replica is
	Daemons     cos.MapStrUint16 `json:"nodes"`         // Locations of all slices: DaemonID <-> SliceID
	Data        int              `json:"data_slices"`   // the number of data slices
	Parity      int              `json:"parity_slices"` // the number of parity slices
	SliceID     int              `json:"slice_id"`      // 0 for full replica, 1 to N for slices
	MDVersion   uint32           `json:"md_version"`    // Metadata format version
	IsCopy      bool             `json:"is_copy"`       // object is replicated(true) or encoded(false)
}

// interface guard
var (
	_ cos.Unpacker = (*Metadata)(nil)
	_ cos.Packer   = (*Metadata)(nil)
)

func NewMetadata() *Metadata {
	return &Metadata{MDVersion: MDVersionLast}
}

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

// RemoteTargets returns list of Snodes that contain a slice or replica.
// This target(`t`) is removed from the list.
func (md *Metadata) RemoteTargets() []*meta.Snode {
	if len(md.Daemons) == 0 {
		return nil
	}
	nodes := make([]*meta.Snode, 0, len(md.Daemons))
	smap := cluster.T.Sowner().Get()
	for tid := range md.Daemons {
		if tid == cluster.T.SID() {
			continue
		}
		tsi := smap.GetTarget(tid)
		if tsi != nil {
			nodes = append(nodes, tsi)
		}
	}
	return nodes
}

// TODO: use 'buf, slab = smm.Alloc()'
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

// ObjectMetadata returns metadata for an object or its slice if any exists
func ObjectMetadata(bck *meta.Bck, objName string) (*Metadata, error) {
	fqn, _, err := cluster.HrwFQN(bck.Bucket(), fs.ECMetaType, objName)
	if err != nil {
		return nil, err
	}
	return LoadMetadata(fqn)
}

func (md *Metadata) Unpack(unpacker *cos.ByteUnpack) (err error) {
	var cksum uint64
	if md.MDVersion, err = unpacker.ReadUint32(); err != nil {
		return
	}
	switch md.MDVersion {
	case MDVersionLast:
		err = md.unpackLastVersion(unpacker)
	default:
		err = fmt.Errorf("unsupported metadata format version %d. Only %d supported",
			md.MDVersion, MDVersionLast)
	}
	if err != nil {
		return
	}

	if cksum, err = unpacker.ReadUint64(); err != nil {
		return
	}
	b := unpacker.Bytes()
	calcCksum := xxhash.Checksum64S(b[:len(b)-cos.SizeofI64], cos.MLCG32)
	if cksum != calcCksum {
		err = cos.NewErrMetaCksum(cksum, calcCksum, "EC metadata")
	}
	return err
}

func (md *Metadata) unpackLastVersion(unpacker *cos.ByteUnpack) (err error) {
	var i16 uint16
	if md.Generation, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if md.Size, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if i16, err = unpacker.ReadUint16(); err != nil {
		return
	}
	md.Data = int(i16)
	if i16, err = unpacker.ReadUint16(); err != nil {
		return
	}
	md.Parity = int(i16)
	if i16, err = unpacker.ReadUint16(); err != nil {
		return
	}
	md.SliceID = int(i16)
	if md.IsCopy, err = unpacker.ReadBool(); err != nil {
		return
	}
	if md.FullReplica, err = unpacker.ReadString(); err != nil {
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
	md.Daemons, err = unpacker.ReadMapStrUint16()
	return
}

func (md *Metadata) Pack(packer *cos.BytePack) {
	packer.WriteUint32(md.MDVersion)
	packer.WriteInt64(md.Generation)
	packer.WriteInt64(md.Size)
	packer.WriteUint16(uint16(md.Data))
	packer.WriteUint16(uint16(md.Parity))
	packer.WriteUint16(uint16(md.SliceID))
	packer.WriteBool(md.IsCopy)
	packer.WriteString(md.FullReplica)
	packer.WriteString(md.ObjCksum)
	packer.WriteString(md.ObjVersion)
	packer.WriteString(md.CksumType)
	packer.WriteString(md.CksumValue)
	packer.WriteMapStrUint16(md.Daemons)
	h := xxhash.Checksum64S(packer.Bytes(), cos.MLCG32)
	packer.WriteUint64(h)
}

func (md *Metadata) PackedSize() int {
	daemonListSz := cos.SizeofLen
	for k := range md.Daemons {
		daemonListSz += cos.PackedStrLen(k) + cos.SizeofI16
	}
	return cos.SizeofI32 + cos.SizeofI64*2 + cos.SizeofI16*3 + 1 /*isCopy*/ +
		cos.PackedStrLen(md.ObjCksum) + cos.PackedStrLen(md.ObjVersion) +
		cos.PackedStrLen(md.CksumType) + cos.PackedStrLen(md.CksumValue) +
		cos.PackedStrLen(md.FullReplica) + daemonListSz + cos.SizeofI64 /*md cksum*/
}
