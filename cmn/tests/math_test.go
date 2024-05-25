// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestMinDuration(t *testing.T) {
	baseTime := time.Minute

	tassert.Fatalf(t, min(baseTime, baseTime+time.Second) == baseTime, "expected %s to be smaller than %s", baseTime, baseTime+time.Second)
	tassert.Fatalf(t, min(baseTime, baseTime-time.Second) == baseTime-time.Second, "expected %s to be smaller than %s", baseTime-time.Second, baseTime)
	tassert.Fatalf(t, min(baseTime, baseTime) == baseTime, "expected %s to be the same as %s", baseTime, baseTime)
}

func TestCeilAlign(t *testing.T) {
	tassert.Fatalf(t, cos.CeilAlign(12, 3) == 12, "got %d, expected 12", cos.CeilAlign(12, 3))
	tassert.Fatalf(t, cos.CeilAlign(10, 3) == 12, "got %d, expected 12", cos.CeilAlign(10, 3))
	tassert.Fatalf(t, cos.CeilAlign(10, 1) == 10, "got %d, expected 10", cos.CeilAlign(10, 1))
}

func TestMin(t *testing.T) {
	tassert.Errorf(t, min(0, 1, 2, 3, 4, 5, 1) == 0, "expected 0 to be the smallest, got %d", min(0, 1, 2, 3, 4, 5, 1))
	tassert.Errorf(t, min(10, 100, -2) == -2, "expected -2 to be the smallest, got %d", min(10, 100, -2))
	tassert.Errorf(t, min(1, 0) == 0, "expected 0 to be the smallest, got %d", min(1, 0))
}
