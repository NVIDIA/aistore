// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"math"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// LOM.md.lid layout ======================================================
//
// lomBID is a 64-bit field in the LOM - specifically, `lmeta` structure.
// As the name implies, lomBID is a union of two values:
// - bucket ID
// - bitwise flags that apply only and exclusively to _this_ object.
// As such, lomBID is persistent (with the rest of `lmeta`) and is used for
// two purposes:
// - uniquely associate a given object to its containing bucket;
// - carry optional flags.
//
// lomBID bitwise structure:
// * high bit is reserved for meta.AisBID
// * next 11 bits: bit flags
// * remaining (64 - 12) = bitshift bits contain the bucket's serial number.

const (
	AisBID = uint64(1 << 63)

	bitshift = 52
	flagmask = math.MaxUint64 >> bitshift       // 0xfff
	flagsBID = (flagmask << bitshift) & ^AisBID // 0x7ff0000000000000
)

type (
	lomFlags uint16
	lomBID   uint64
)

const (
	lmflFntl = lomFlags(1 << iota)
	lmflChunk
)

func NewBID(serial uint64, isAis bool) uint64 {
	// not adding runtime check given the time reasonably
	// required to create (1 << 52) buckets
	debug.Assert(serial&(flagmask<<bitshift) == 0)

	if isAis {
		return serial | AisBID
	}
	return serial
}

func (lid lomBID) bid() uint64     { return uint64(lid) & ^flagsBID }
func (lid lomBID) flags() lomFlags { return lomFlags((uint64(lid) & flagsBID) >> bitshift) }

func (lid lomBID) setbid(bid uint64) lomBID {
	debug.Assert(bid&flagsBID == 0, bid)
	return lomBID((uint64(lid) & flagsBID) | bid)
}

func (lid lomBID) setlmfl(fl lomFlags) lomBID {
	debug.Assert(fl <= lomFlags(flagsBID>>bitshift), fl)
	return lomBID(uint64(lid) | (uint64(fl) << bitshift))
}

func (lid lomBID) haslmfl(fl lomFlags) bool { return uint64(lid)&(uint64(fl)<<bitshift) != 0 }

func (lid lomBID) clrlmfl(fl lomFlags) lomBID {
	debug.Assert(fl <= lomFlags(flagsBID>>bitshift))
	return lomBID(uint64(lid) & ^(uint64(fl) << bitshift))
}
