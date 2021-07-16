// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	// a target sends a replica or slice to store on another target
	// the destionation does not have to respond
	reqPut intraReqType = iota
	// response for requested slice/replica by another target
	respPut
	// a target requests a slice or replica from another target
	// if the destination has the object/slice it sends it back, otherwise
	//    it sets Exists=false in response header
	reqGet
	// a target cleans up the object and notifies all other targets to do
	// cleanup as well. Destinations do not have to respond
	reqDel
)

type (
	// type of EC request between targets. If the destination has to respond it
	// must set the same request type in response header
	intraReqType = int

	// An EC request sent via transport using Opaque field of transport.ObjHdr
	// between targets inside a cluster
	intraReq struct {
		// Request type
		act intraReqType
		// Sender's daemonID, used by the destination to send the response
		// to the correct target
		sender string
		// Object metadata, used when a target copies replicas/slices after
		// encoding or restoring the object data
		meta *Metadata
		// Used only by destination to answer to the sender if the destination
		// has the requested metafile or replica/slice
		exists bool
		// The sent data is slice or full replica
		isSlice bool
		// bucket ID
		bid uint64
	}
)

// interface guard
var (
	_ cos.Unpacker = (*intraReq)(nil)
	_ cos.Packer   = (*intraReq)(nil)
)

func (r *intraReq) PackedSize() int {
	if r.meta == nil {
		// int8(type)+sender(string)+int8+int8+ptr_marker
		return cos.PackedStrLen(r.sender) + 4 + cos.SizeofI64
	}
	// int8(type)+sender(string)+int8+int8+ptr_marker+sizeof(meta)
	return cos.PackedStrLen(r.sender) + r.meta.PackedSize() + 4 + cos.SizeofI64
}

func (r *intraReq) Pack(packer *cos.BytePack) {
	packer.WriteByte(uint8(r.act))
	packer.WriteString(r.sender)
	packer.WriteBool(r.exists)
	packer.WriteBool(r.isSlice)
	packer.WriteUint64(r.bid)
	if r.meta == nil {
		packer.WriteByte(0)
	} else {
		packer.WriteByte(1)
		packer.WriteAny(r.meta)
	}
}

func (r *intraReq) Unpack(unpacker *cos.ByteUnpack) error {
	var (
		i   byte
		err error
	)
	if i, err = unpacker.ReadByte(); err != nil {
		return err
	}
	r.act = intraReqType(i)
	if r.sender, err = unpacker.ReadString(); err != nil {
		return err
	}
	if r.exists, err = unpacker.ReadBool(); err != nil {
		return err
	}
	if r.isSlice, err = unpacker.ReadBool(); err != nil {
		return err
	}
	if r.bid, err = unpacker.ReadUint64(); err != nil {
		return err
	}
	if i, err = unpacker.ReadByte(); err != nil {
		return err
	}
	if i == 0 {
		r.meta = nil
		return nil
	}
	r.meta = NewMetadata()
	return unpacker.ReadAny(r.meta)
}

func (r *intraReq) NewPack(mm *memsys.MMSA) []byte {
	var (
		buf []byte
		l   = r.PackedSize()
	)
	if mm != nil {
		buf, _ = mm.AllocSize(int64(l))
	}
	packer := cos.NewPacker(buf, l)
	packer.WriteAny(r)
	return packer.Bytes()
}
