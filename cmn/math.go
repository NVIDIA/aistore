// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"math"
	"time"
)

const MaxInt64 = int64(math.MaxInt64)

// MinU64 returns min value of a and b for uint64 types
func MinU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// MaxU64 returns max value of a and b for uint64 types
func MaxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// MinI64 returns min value of a and b for int64 types
func MinI64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// MaxI64 returns max value of a and b for int64 types
func MaxI64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// MinI32 returns min value of a and b for int32 types
func MinI32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// MaxI32 returns max value of a and b for int32 types
func MaxI32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

// MinDuration returns min value of a and b time.Duration types
func MinDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}

// MaxDuration returns min value of a and b time.Duration types
func MaxDuration(a, b time.Duration) time.Duration {
	if a >= b {
		return a
	}

	return b
}

// Min returns min value of a and b for int types
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Max returns max value of a and b for int types
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinF64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func MaxF64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func MinF32(a, b float32) float32 {
	if a < b {
		return a
	}
	return b
}

func MaxF32(a, b float32) float32 {
	if a > b {
		return a
	}
	return b
}

func MinDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func DivCeil(a, b int64) int64 {
	d, r := a/b, a%b
	if r > 0 {
		return d + 1
	}
	return d
}

// FastLog2 returns floor(log2(c))
func FastLog2(c uint64) uint {
	for i := uint(0); ; {
		if c >>= 1; c == 0 {
			return i
		}
		i++
	}
}

func FastLog2Ceil(c uint64) uint {
	if c == 0 {
		return 0
	}
	return FastLog2(c-1) + 1
}

// CailAlign returns smallest number bigger or equal to val, which is divisible by align
func CeilAlign(val, align uint) uint {
	mod := val % align
	if mod != 0 {
		val += align - mod
	}
	return val
}
