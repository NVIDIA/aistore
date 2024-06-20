// Package meta: cluster-level metadata
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package meta

const AisBID = uint64(1 << 63)

// Bucket ID (BID) is a 64-bit field in `cmn.Bck` and `Bck` structures, where
// only (52 + 1) bits are actually used for the value, and the remaining 11 bits
// are reserved for core.lomBID.
//
// See also: core.lomBID

type BID uint64

func NewBID(serial int64, isAis bool) uint64 {
	if isAis {
		return uint64(serial) | AisBID
	}
	return uint64(serial)
}

func (bid BID) serial() uint64 {
	return uint64(bid) & ^AisBID
}
