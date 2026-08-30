// Package cos_test: unit tests
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestResetSliceCap(t *testing.T) {
	mk := func(n, c int) []int {
		s := make([]int, n, c)
		for i := range s {
			s[i] = i + 1
		}
		return s
	}

	s := mk(5, 16)
	s2 := cos.ResetSliceCap(s, 32)
	if len(s2) != 5 || cap(s2) != 16 || &s2[0] != &s[0] {
		t.Fatal("keep within limit")
	}

	s = mk(5, 64)
	s2 = cos.ResetSliceCap(s, 16)
	if len(s2) != 5 || cap(s2) != 16 || &s2[0] == &s[0] {
		t.Fatal("replace oversized backing array")
	}
	for i := range s2 {
		if s2[i] != s[i] {
			t.Fatal("preserve contents")
		}
	}

	s = mk(32, 64)
	s2 = cos.ResetSliceCap(s, 16)
	if len(s2) != 16 || cap(s2) != 16 || &s2[0] == &s[0] {
		t.Fatal("shrink len+cap")
	}
	for i := range s2 {
		if s2[i] != s[i] {
			t.Fatal("preserve truncated contents")
		}
	}

	// Callers commonly reset length before clipping. Appending to the replacement
	// must not write into the oversized array.
	s = mk(0, 64)
	s2 = cos.ResetSliceCap(s, 16)
	if len(s2) != 0 || cap(s2) != 16 {
		t.Fatal("replace empty oversized backing array")
	}
	s2 = append(s2, 1)
	if s2[0] != 1 || s[:1][0] != 0 {
		t.Fatal("replacement still aliases oversized backing array")
	}

	s = mk(10, 64)
	s2 = cos.ResetSliceCap(s, 0)
	if s2 != nil {
		t.Fatal("drop backing array")
	}
}
