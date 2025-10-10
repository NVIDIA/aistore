// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	// a target sends a replica or slice to store on another target
	// the destination does not have to respond
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

// Create a request header: initializes the `Sender` field with local target's
// daemon ID, and sets `Exists:true` that means "local object exists".
// Later `Exists` can be changed to `false` if local file is unreadable or does
// not exist
func newIntraReq(act intraReqType, md *Metadata, bck *meta.Bck) *intraReq {
	req := &intraReq{
		meta:   md,
		exists: true,
	}
	if bck != nil && bck.Props != nil {
		req.bid = bck.Props.BID
	}
	if act == reqGet && md != nil {
		req.isSlice = !md.IsCopy
	}
	return req
}

func (r *intraReq) PackedSize() int {
	if r.meta == nil {
		// int8+int8+ptr_marker
		return 3 + cos.SizeofI64
	}
	// int8+int8+ptr_marker+sizeof(meta)
	return r.meta.PackedSize() + 3 + cos.SizeofI64
}

func (r *intraReq) Pack(packer *cos.BytePack) {
	packer.WriteBool(r.exists)
	packer.WriteBool(r.isSlice)
	packer.WriteUint64(r.bid)
	if r.meta == nil {
		packer.WriteUint8(0)
	} else {
		packer.WriteUint8(1)
		packer.WriteAny(r.meta)
	}
}

func (r *intraReq) Unpack(unpacker *cos.ByteUnpack) error {
	var (
		i   byte
		err error
	)
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
