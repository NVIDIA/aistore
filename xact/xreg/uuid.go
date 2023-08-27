// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"math/rand"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
)

var (
	PrimeTime atomic.Int64
	MyTime    atomic.Int64
)

// see related: cmn/cos/uuid.go

func GenBeUID(div, slt int64) (buid string) {
	now := time.Now().UnixNano() - MyTime.Load() + PrimeTime.Load()
	rem := now % div

	seed := now - rem + slt
	if seed < 0 {
		seed = now - rem - slt
	}
	rnd := rand.New(rand.NewSource(seed))
	buid = cos.RandStringWithSrc(rnd, cos.LenShortID)

	if xctn, err := GetXact(buid); err != nil /*unlikely*/ || xctn != nil /*idling away*/ {
		// fallback
		buid = cos.GenUUID()
	}
	return
}
