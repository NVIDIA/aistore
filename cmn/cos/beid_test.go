// Package cos_test: unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
)

// copy-paste xreg.beidSeed (unexported)
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

// -------- benchmarks --------

func BenchmarkGenBEID(b *testing.B) {
	seed := beidSeed(999, 7, 0xcafebabe, 0)
	val := xoshiro256.Hash(seed)
	for b.Loop() {
		cos.GenBEID(val, cos.LenBEID)
	}
}

func BenchmarkBeidSeed(b *testing.B) {
	for i := range b.N {
		attempt := uint64(i) % 10
		beidSeed(42, 5, 0xdeadbeef, attempt)
	}
}

func BenchmarkBeidSeedPlusHash(b *testing.B) {
	for b.Loop() {
		seed := beidSeed(42, 5, 0xdeadbeef, 0)
		xoshiro256.Hash(seed)
	}
}

func BenchmarkEndToEnd(b *testing.B) {
	for i := range b.N {
		attempt := uint64(i) % 10
		seed := beidSeed(42, 5, 0xdeadbeef, attempt)
		val := xoshiro256.Hash(seed)
		cos.GenBEID(val, cos.LenBEID)
	}
}
