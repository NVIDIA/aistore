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

func DivCeil(a, b int64) int64 {
	d, r := a/b, a%b
	if r > 0 {
		return d + 1
	}
	return d
}
