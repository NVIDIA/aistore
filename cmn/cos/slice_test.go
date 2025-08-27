// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestResetSliceCap(t *testing.T) {
	mk := func(n, c int) []int { s := make([]int, n, c); return s }

	s := mk(5, 16)
	s2 := cos.ResetSliceCap(s, 32)
	if len(s2) != 5 || cap(s2) != 16 {
		t.Fatal("keep within limit")
	}

	s = mk(5, 64)
	s2 = cos.ResetSliceCap(s, 16)
	if len(s2) != 5 || cap(s2) != 16 {
		t.Fatal("shrink cap only")
	}

	s = mk(32, 64)
	s2 = cos.ResetSliceCap(s, 16)
	if len(s2) != 16 || cap(s2) != 16 {
		t.Fatal("shrink len+cap")
	}

	s = mk(10, 64)
	s2 = cos.ResetSliceCap(s, 0)
	if len(s2) != 0 || cap(s2) != 0 {
		t.Fatal("drop backing array")
	}
}
