// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "github.com/NVIDIA/aistore/cmn/debug"

func DivCeil(a, b int64) int64 {
	d, r := a/b, a%b
	if r > 0 {
		return d + 1
	}
	return d
}

func DivRound(a, b int64) int64      { return (a + b/2) / b }
func DivRoundU64(a, b uint64) uint64 { return (a + b/2) / b }

// returns smallest number divisible by `align` that is greater or equal `val`
func CeilAlignI64(val, align int64) int64 {
	mod := val % align
	if mod != 0 {
		val += align - mod
	}
	return val
}

func RatioPct(high, low, curr int64) int64 {
	debug.Assert(high > low && low > 0)
	if curr <= low {
		return 0
	}
	if curr >= high {
		return 100
	}
	return (curr - low) * 100 / (high - low)
}
