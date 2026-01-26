// Package uuid_test (gen-uuid vs gen-tie)
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package uuid_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func BenchmarkGenID(b *testing.B) {
	cos.InitShortID(uint64(time.Now().UnixNano()))

	b.Run("GenUUID", func(b *testing.B) {
		for b.Loop() {
			cos.GenUUID()
		}
	})
	b.Run("GenTie", func(b *testing.B) {
		for b.Loop() {
			cos.GenTie()
		}
	})
}
