// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/OneOfOne/xxhash"
)

var (
	PrimeTime atomic.Int64
	MyTime    atomic.Int64
)

// see related: cmn/cos/uuid.go

// "best-effort ID" - to independently and locally generate globally unique xaction ID
func GenBEID(div uint64, tag string) (beid string, xctn core.Xact, err error) {
	// primary's "now"
	now := uint64(time.Now().UnixNano() - MyTime.Load() + PrimeTime.Load())

	// compute
	val := now / div
	org := val
	val ^= xxhash.Checksum64S(cos.UnsafeB(tag), val)
	beid = cos.GenBEID(val, cos.LenShortID)

	// check vs registry
	xctn, err = GetXact(beid)
	if err != nil {
		beid = ""
		return // unlikely
	}
	if xctn == nil {
		return
	}

	// "idling" away, so try again but only once
	val ^= org
	beid = cos.GenBEID(val, cos.LenShortID)
	if xctn, err = GetXact(beid); err != nil || xctn != nil {
		beid = ""
	}
	return
}
