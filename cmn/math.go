// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"math/rand"
	"time"

	"github.com/NVIDIA/aistore/cmn/mono"
)

type Bits uint8

func MaxInArray(xs ...int) int {
	Assert(len(xs) > 0)
	curMax := xs[0]
	for _, x := range xs[1:] {
		if x > curMax {
			curMax = x
		}
	}
	return curMax
}

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

// Min returns min value from given ints
func Min(ints ...int) int {
	Assert(len(ints) >= 2)
	if len(ints) == 2 {
		if ints[0] < ints[1] {
			return ints[0]
		}
		return ints[1]
	}

	return Min(ints[0], Min(ints[1:]...))
}

// Max returns max value of a and b for int types
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
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

func DivCeil(a, b int64) int64 {
	d, r := a/b, a%b
	if r > 0 {
		return d + 1
	}
	return d
}

func DivRound(a, b int64) int64 { return (a + b/2) / b }

// CailAlign returns smallest number bigger or equal to val, which is divisible by align
func CeilAlign(val, align uint) uint {
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

func NowRand() *rand.Rand {
	return rand.New(rand.NewSource(mono.NanoTime()))
}

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
