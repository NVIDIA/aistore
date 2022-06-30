// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
)

type Bits uint8

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

func MinTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// Min returns min value from given ints.
func Min(xs ...int) int {
	debug.Assert(len(xs) > 0)
	if len(xs) == 1 {
		return xs[0]
	}
	if len(xs) == 2 {
		if xs[0] < xs[1] {
			return xs[0]
		}
		return xs[1]
	}
	return Min(xs[0], Min(xs[1:]...))
}

func MinUint(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

// Max returns max value from given ints.
func Max(xs ...int) int {
	debug.Assert(len(xs) > 0)
	if len(xs) == 1 {
		return xs[0]
	}
	if len(xs) == 2 {
		if xs[0] > xs[1] {
			return xs[0]
		}
		return xs[1]
	}
	return Max(xs[0], Max(xs[1:]...))
}

func MaxUint(a, b uint) uint {
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

func Abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

func AbsI64(a int64) int64 {
	if a < 0 {
		return -a
	}
	return a
}

func DivCeil(a, b int64) int64 {
	d, r := a/b, a%b
	if r > 0 {
		return d + 1
	}
	return d
}

func DivRound(a, b int64) int64      { return (a + b/2) / b }
func DivRoundU64(a, b uint64) uint64 { return (a + b/2) / b }

// CeilAlign returns smallest number bigger or equal to val, which is divisible by align
func CeilAlign(val, align uint) uint {
	mod := val % align
	if mod != 0 {
		val += align - mod
	}
	return val
}

func CeilAlignInt64(val, align int64) int64 {
	mod := val % align
	if mod != 0 {
		val += align - mod
	}
	return val
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

func (b *Bits) Set(flag Bits)      { x := *b; x |= flag; *b = x }
func (b *Bits) Clear(flag Bits)    { x := *b; x &^= flag; *b = x }
func (b *Bits) Toggle(flag Bits)   { x := *b; x ^= flag; *b = x }
func (b *Bits) Has(flag Bits) bool { return *b&flag != 0 }

func Ratio(high, low, curr int64) float32 {
	Assert(high > low && high <= 100 && low > 0)
	if curr <= low {
		return 0
	}
	if curr >= high {
		return 1
	}
	return float32(curr-low) / float32(high-low)
}

func RatioPct(high, low, curr int64) int64 {
	Assert(high > low && high <= 100 && low > 0)
	if curr <= low {
		return 0
	}
	if curr >= high {
		return 100
	}
	return (curr - low) * 100 / (high - low)
}
