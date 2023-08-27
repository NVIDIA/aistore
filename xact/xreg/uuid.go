// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/OneOfOne/xxhash"
)

var (
	PrimeTime atomic.Int64
	MyTime    atomic.Int64
)

// see related: cmn/cos/uuid.go

// "best-effort ID" - to independently and locally generate globally unique xaction ID
func GenBEID(div uint64, tag string) (beid string) {
	now := uint64(time.Now().UnixNano() - MyTime.Load() + PrimeTime.Load())
	val := now / div
	val ^= xxhash.ChecksumString64S(tag, val)

	beid = cos.GenBEID(val)

	if xctn, err := GetXact(beid); err != nil /*unlikely*/ || xctn != nil {
		// idling away? fallback to common default
		beid = cos.GenUUID()
	}
	return
}
