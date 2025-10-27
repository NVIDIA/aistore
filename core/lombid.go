// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"math"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// LOM.md.lid (lomBID) MSB => LSB layout
//
// [63]=AisBID | [62:60]=legacy flags (3 bits) | [59:0]=serial

const (
	AisBID = uint64(1) << 63

	bitshift   = 60
	flagMaskV1 = math.MaxUint64 >> bitshift         // 0xf
	flagsV1    = (flagMaskV1 << bitshift) & ^AisBID // 0x7000000000000000
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
	debug.Assert(serial&(flagMaskV1<<bitshift) == 0)

	if isAis {
		return serial | AisBID
	}
	return serial
}

func (lid lomBID) bid() uint64     { return uint64(lid) & ^flagsV1 }
func (lid lomBID) flags() lomFlags { return lomFlags((uint64(lid) & flagsV1) >> bitshift) }

func (lid lomBID) setbid(bid uint64) lomBID {
	debug.Assert(bid&flagsV1 == 0, bid)
	return lomBID((uint64(lid) & flagsV1) | bid)
}

func (lid lomBID) setlmfl(fl lomFlags) lomBID {
	debug.Assert(fl <= lomFlags(flagsV1>>bitshift), fl)
	return lomBID(uint64(lid) | (uint64(fl) << bitshift))
}

func (lid lomBID) haslmfl(fl lomFlags) bool { return uint64(lid)&(uint64(fl)<<bitshift) != 0 }

func (lid lomBID) clrlmfl(fl lomFlags) lomBID {
	debug.Assert(fl <= lomFlags(flagsV1>>bitshift))
	return lomBID(uint64(lid) & ^(uint64(fl) << bitshift))
}
