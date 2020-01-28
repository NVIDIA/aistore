// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"github.com/NVIDIA/aistore/cmn"
)

// Rebalance message types (for ACK or sending files)
const (
	rebMsgAckRegular = iota // acknowledge of regular rebalance
	rebMsgAckEC             // acknowledge of EC rebalance
	// TODO: reserved for the next MR (commented to make linter happy)
	// rebMsgObj               // regular rebalance sent an object
	// rebMsgCT                // EC rebalance sent object/replica/slice
)
const rebMsgKindSize = 1

type (
	regularAck struct {
		GlobRebID int64  `json:"reb_id"`
		DaemonID  string `json:"daemon"` // sender's DaemonID
	}
	ecAck struct {
		globRebID int64
		daemonID  string // sender's DaemonID
		sliceID   uint16
	}
)

var _ cmn.Unpacker = &regularAck{}
var _ cmn.Unpacker = &ecAck{}
var _ cmn.Packer = &regularAck{}
var _ cmn.Packer = &ecAck{}

func (rack *regularAck) Unpack(unpacker *cmn.ByteUnpack) (err error) {
	if rack.GlobRebID, err = unpacker.ReadInt64(); err != nil {
		return
	}
	rack.DaemonID, err = unpacker.ReadString()
	return
}

func (rack *regularAck) Pack(packer *cmn.BytePack) {
	packer.WriteInt64(rack.GlobRebID)
	packer.WriteString(rack.DaemonID)
}

// globRebID + length of DaemonID + Daemon
func (rack *regularAck) PackedSize() int {
	return cmn.SizeofI64 + cmn.SizeofLen + len(rack.DaemonID)
}

func (eack *ecAck) Unpack(unpacker *cmn.ByteUnpack) (err error) {
	if eack.globRebID, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if eack.sliceID, err = unpacker.ReadUint16(); err != nil {
		return
	}
	eack.daemonID, err = unpacker.ReadString()
	return
}

func (eack *ecAck) Pack(packer *cmn.BytePack) {
	packer.WriteInt64(eack.globRebID)
	packer.WriteUint16(eack.sliceID)
	packer.WriteString(eack.daemonID)
}

// globRebID + sliceID + length of DaemonID + Daemon
func (eack *ecAck) PackedSize() int {
	return cmn.SizeofI64 + cmn.SizeofI16 + cmn.SizeofLen + len(eack.daemonID)
}
