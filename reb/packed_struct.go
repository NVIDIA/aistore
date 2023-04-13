// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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
	rebMsgStageNtfn        // stage notification (of target transitioning to the next stage)
)
const rebMsgKindSize = 1
const (
	rebActRebCT    = iota // a CT moved to a correct target (regular rebalance)
	rebActMoveCT          // a CT moved from a target after slice conflict (a target received a CT and it had another CT)
	rebActUpdateMD        // a new MD to update existing local one
)

type (
	regularAck struct {
		daemonID string // sender's DaemonID
		rebID    int64
	}
	ecAck struct {
		daemonID string // sender's DaemonID
		rebID    int64
		sliceID  uint16
	}

	// stage notification struct - a target sends it when it enters `stage`
	stageNtfn struct {
		md       *ec.Metadata
		daemonID string // sender's ID
		rebID    int64  // sender's rebalance ID
		stage    uint32 // stage the sender has just reached
		action   uint32 // see rebAct* constants
	}
)

// interface guard
var (
	_ cos.Unpacker = (*regularAck)(nil)
	_ cos.Unpacker = (*ecAck)(nil)
	_ cos.Packer   = (*regularAck)(nil)
	_ cos.Packer   = (*ecAck)(nil)
	_ cos.Packer   = (*stageNtfn)(nil)
	_ cos.Unpacker = (*stageNtfn)(nil)
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

func (eack *ecAck) PackedSize() int {
	return cos.SizeofI64 + cos.SizeofI16 + cos.PackedStrLen(eack.daemonID)
}

func (ntfn *stageNtfn) PackedSize() int {
	total := cos.SizeofI64 + cos.SizeofI32*2 +
		cos.PackedStrLen(ntfn.daemonID) + 1
	if ntfn.md != nil {
		total += ntfn.md.PackedSize()
	}
	return total
}

func (ntfn *stageNtfn) Pack(packer *cos.BytePack) {
	packer.WriteInt64(ntfn.rebID)
	packer.WriteUint32(ntfn.action)
	packer.WriteUint32(ntfn.stage)
	packer.WriteString(ntfn.daemonID)
	if ntfn.md == nil {
		packer.WriteByte(0)
	} else {
		packer.WriteByte(1)
		packer.WriteAny(ntfn.md)
	}
}

func (ntfn *stageNtfn) NewPack(kind byte) []byte {
	l := rebMsgKindSize + ntfn.PackedSize()
	packer := cos.NewPacker(nil, l)
	packer.WriteByte(kind)
	packer.WriteAny(ntfn)
	return packer.Bytes()
}

func (ntfn *stageNtfn) Unpack(unpacker *cos.ByteUnpack) (err error) {
	if ntfn.rebID, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if ntfn.action, err = unpacker.ReadUint32(); err != nil {
		return
	}
	if ntfn.stage, err = unpacker.ReadUint32(); err != nil {
		return
	}
	if ntfn.daemonID, err = unpacker.ReadString(); err != nil {
		return
	}

	var marker byte
	if marker, err = unpacker.ReadByte(); err != nil {
		return
	}
	if marker == 0 {
		ntfn.md = nil
		return
	}
	ntfn.md = ec.NewMetadata()
	return unpacker.ReadAny(ntfn.md)
}

func (*Reb) encodeStageNtfn(ntfn *stageNtfn) []byte {
	return ntfn.NewPack(rebMsgStageNtfn)
}

func (*Reb) decodeStageNtfn(buf []byte) (*stageNtfn, error) {
	var (
		ntfn     = &stageNtfn{}
		unpacker = cos.NewUnpacker(buf)
		act, err = unpacker.ReadByte()
	)
	if err != nil {
		return nil, err
	}
	// at the moment, there is only one kind of stage notifications (see above)
	if act != rebMsgStageNtfn {
		return nil, fmt.Errorf("expected %d (stage notification), got %d", rebMsgStageNtfn, act)
	}
	err = unpacker.ReadAny(ntfn)
	return ntfn, err
}
