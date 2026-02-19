// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION. All rights reserved.
 */

package xreg

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
	"github.com/NVIDIA/aistore/core"

	onexxh "github.com/OneOfOne/xxhash"
)

// retry attempts when collision is detected in registry
const beidMaxRetry = 2

func GenBEID(div uint64, smapVer int64, tag []byte) (beid string, prev core.Xact, err error) {
	debug.Assert(div > 0)
	div = max(1, div)

	now := uint64(time.Now().UnixNano() - MyTime.Load() + PrimeTime.Load())
	bucket := now / div
	th := onexxh.Checksum64(tag)
	sv := uint64(smapVer)

	// likely cause for retries:
	// - starting the same xaction type (same tag)
	// - twice within the same minute (div)
	// - under same Smap version

	for attempt := uint64(0); attempt <= beidMaxRetry; attempt++ {
		seed := beidSeed(bucket, sv, th, attempt)
		val := xoshiro256.Hash(seed)
		beid = cos.GenBEID(val, cos.LenBEID)

		prev, err = GetXact(beid)
		if err != nil {
			return "", nil, err
		}
		if prev == nil {
			return beid, nil, nil
		}
	}

	// giving up (extremely unlikely)
	return "", prev, nil
}

// xoshiro256.Hash diffusion
func beidSeed(bucket, smapv, tagh, attempt uint64) uint64 {
	const (
		g = cos.GoldenRatio
		c = cos.Mix64Mul
	)
	seed := bucket*g ^ (smapv + c) ^ (tagh + g)
	if attempt != 0 {
		seed ^= attempt * c
	}
	return seed
}
