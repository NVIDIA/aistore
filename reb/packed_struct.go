// Package reb provides local resilver and global rebalance for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ec"
)

// Rebalance message types (for ACK or sending files)
const (
	rebMsgRegular   = iota // regular rebalance: acknowledge/Object
	rebMsgEC               // EC rebalance: acknowledge/CT/Namespace
	rebMsgPushStage        // push notification of target moved to the next stage
)
const rebMsgKindSize = 1

type (
	regularAck struct {
		rebID    int64
		daemonID string // sender's DaemonID
	}
	ecAck struct {
		rebID    int64
		daemonID string // sender's DaemonID
		sliceID  uint16
	}

	// push notification struct - a target sends it when it enters `stage`
	pushReq struct {
		rebID    int64  // sender's rebalance ID
		daemonID string // sender's ID
		batch    int    // batch when restoring
		stage    uint32 // stage the sender has just reached
		md       *ec.Metadata
	}
)

// interface guard
var (
	_ cos.Unpacker = (*regularAck)(nil)
	_ cos.Unpacker = (*ecAck)(nil)
	_ cos.Packer   = (*regularAck)(nil)
	_ cos.Packer   = (*ecAck)(nil)
	_ cos.Packer   = (*pushReq)(nil)
	_ cos.Unpacker = (*pushReq)(nil)
)

func (rack *regularAck) Unpack(unpacker *cos.ByteUnpack) (err error) {
	if rack.rebID, err = unpacker.ReadInt64(); err != nil {
		return
	}
	rack.daemonID, err = unpacker.ReadString()
	return
}

func (rack *regularAck) Pack(packer *cos.BytePack) {
	packer.WriteInt64(rack.rebID)
	packer.WriteString(rack.daemonID)
}

func (rack *regularAck) NewPack() []byte { // TODO: consider adding as another cos.Packer interface
	l := rebMsgKindSize + rack.PackedSize()
	packer := cos.NewPacker(nil, l)
	packer.WriteByte(rebMsgRegular)
	packer.WriteAny(rack)
	return packer.Bytes()
}

// rebID + length of DaemonID + Daemon
func (rack *regularAck) PackedSize() int {
	return cos.SizeofI64 + cos.SizeofLen + len(rack.daemonID)
}

func (eack *ecAck) Unpack(unpacker *cos.ByteUnpack) (err error) {
	if eack.rebID, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if eack.sliceID, err = unpacker.ReadUint16(); err != nil {
		return
	}
	eack.daemonID, err = unpacker.ReadString()
	return
}

func (eack *ecAck) Pack(packer *cos.BytePack) {
	packer.WriteInt64(eack.rebID)
	packer.WriteUint16(eack.sliceID)
	packer.WriteString(eack.daemonID)
}

func (eack *ecAck) NewPack() []byte {
	l := rebMsgKindSize + eack.PackedSize()
	packer := cos.NewPacker(nil, l)
	packer.WriteByte(rebMsgEC)
	packer.WriteAny(eack)
	return packer.Bytes()
}

// rebID + sliceID + length of DaemonID + Daemon
func (eack *ecAck) PackedSize() int {
	return cos.SizeofI64 + cos.SizeofI16 + cos.SizeofLen + len(eack.daemonID)
}

// int64*2 + int32 + string + marker + sizeof(ec.MD)
func (req *pushReq) PackedSize() int {
	total := cos.SizeofLen + cos.SizeofI64*2 + cos.SizeofI32 +
		len(req.daemonID) + 1
	if req.md != nil {
		total += req.md.PackedSize()
	}
	return total
}

func (req *pushReq) Pack(packer *cos.BytePack) {
	packer.WriteInt64(req.rebID)
	packer.WriteUint64(uint64(req.batch))
	packer.WriteUint32(req.stage)
	packer.WriteString(req.daemonID)
	if req.md == nil {
		packer.WriteByte(0)
	} else {
		packer.WriteByte(1)
		packer.WriteAny(req.md)
	}
}

func (req *pushReq) NewPack(kind byte) []byte {
	l := rebMsgKindSize + req.PackedSize()
	packer := cos.NewPacker(nil, l)
	packer.WriteByte(kind)
	packer.WriteAny(req)
	return packer.Bytes()
}

func (req *pushReq) Unpack(unpacker *cos.ByteUnpack) error {
	var (
		marker byte
		err    error
		batch  uint64
	)
	if req.rebID, err = unpacker.ReadInt64(); err != nil {
		return err
	}
	if batch, err = unpacker.ReadUint64(); err != nil {
		return err
	}
	req.batch = int(batch)
	if req.stage, err = unpacker.ReadUint32(); err != nil {
		return err
	}
	if req.daemonID, err = unpacker.ReadString(); err != nil {
		return err
	}
	marker, err = unpacker.ReadByte()
	if err != nil {
		return err
	}
	if marker == 0 {
		req.md = nil
		return nil
	}
	req.md = ec.NewMetadata()
	return unpacker.ReadAny(req.md)
}

func (*Manager) encodePushReq(req *pushReq) []byte {
	return req.NewPack(rebMsgPushStage)
}

func (*Manager) decodePushReq(buf []byte) (*pushReq, error) {
	var (
		req      = &pushReq{}
		unpacker = cos.NewUnpacker(buf)
		act, err = unpacker.ReadByte()
	)
	if err != nil {
		return nil, err
	}
	// at the moment, there is only one kind of push notifications (see above)
	if act != rebMsgPushStage {
		return nil, fmt.Errorf("expected %d (push notification), got %d", rebMsgPushStage, act)
	}
	err = unpacker.ReadAny(req)
	return req, err
}
