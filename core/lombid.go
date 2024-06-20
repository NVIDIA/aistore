// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
)

const (
	bitshift = 52
	flagsBID = uint64(0xfff<<bitshift) & ^meta.AisBID
)

// lomBID is a 64-bit field in the LOM - specifically, `lmeta` structure.
// As the name implies, lomBID is a union of two values:
// - bucket ID
// - bitwise flags that apply only and exclusively to _this_ object.
// As such, lomBID is persistent (with the rest of `lmeta`) and is used for two purposes:
// - uniquely associate a given object to its containing bucket;
// - carry optional flags.
// lomBID bitwise structure:
// * high bit is reserved for meta.AisBID
// * next 11 bits: bit flags
// * remaining (64 - 12) = bitshift bits contain the bucket's serial number.

type lomBID meta.BID

func (lid lomBID) bid() uint64   { return uint64(lid) & ^flagsBID }
func (lid lomBID) flags() uint16 { return uint16((uint64(lid) & flagsBID) >> bitshift) }

func (lid lomBID) setbid(bid uint64) lomBID {
	debug.Assert(bid&flagsBID == 0)
	return lomBID((uint64(lid) & flagsBID) | bid)
}

func (lid lomBID) setflags(fl uint16) lomBID {
	debug.Assert(fl <= 0x7ff)
	return lomBID(uint64(lid) | (uint64(fl) << bitshift))
}

func (lid lomBID) clrflags(fl uint16) lomBID {
	debug.Assert(fl <= 0x7ff)
	return lomBID(uint64(lid) & ^(uint64(fl) << bitshift))
}
