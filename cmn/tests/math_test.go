// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
)

func TestFastLog2(t *testing.T) {
	log22 := cos.FastLog2(2)
	log42 := cos.FastLog2(4)
	log82 := cos.FastLog2(8)
	log10242 := cos.FastLog2(1024)
	tassert.Fatalf(t, log22 == 1, "wrong power of 2 log2 result; got %d, expected %d", log22, 1)
	tassert.Fatalf(t, log42 == 2, "wrong power of 2 log2 result; got %d, expected %d", log42, 2)
	tassert.Fatalf(t, log82 == 3, "wrong power of 2 log2 result; got %d, expected %d", log82, 3)
	tassert.Fatalf(t, log10242 == 10, "wrong power of 2 log2 result; got %d, expected %d", log10242, 10)

	log32 := cos.FastLog2(3)
	log52 := cos.FastLog2(5)
	log152 := cos.FastLog2(15)
	log10232 := cos.FastLog2(1023)
	log10252 := cos.FastLog2(1025)
	tassert.Fatalf(t, log32 == 1, "wrong log2 result; got %d, expected %d", log32, 1)
	tassert.Fatalf(t, log52 == 2, "wrong log2 result; got %d, expected %d", log52, 2)
	tassert.Fatalf(t, log152 == 3, "wrong log2 result; got %d, expected %d", log152, 3)
	tassert.Fatalf(t, log10232 == 9, "wrong log2 result; got %d, expected %d", log10232, 9)
	tassert.Fatalf(t, log10252 == 10, "wrong log2 result; got %d, expected %d", log10252, 10)
}

func TestFastLog2Ceil(t *testing.T) {
	log22 := cos.FastLog2(2)
	log42 := cos.FastLog2(4)
	log82 := cos.FastLog2(8)
	log10242 := cos.FastLog2(1024)
	tassert.Fatalf(t, log22 == 1, "wrong power of 2 ceil(log2) result; got %d, expected %d", log22, 1)
	tassert.Fatalf(t, log42 == 2, "wrong power of 2 ceil(log2) result; got %d, expected %d", log42, 2)
	tassert.Fatalf(t, log82 == 3, "wrong power of 2 ceil(log2) result; got %d, expected %d", log82, 3)
	tassert.Fatalf(t, log10242 == 10, "wrong power of 2 ceil(log2) result; got %d, expected %d", log10242, 10)

	log32 := cos.FastLog2Ceil(3)
	log52 := cos.FastLog2Ceil(5)
	log152 := cos.FastLog2Ceil(15)
	log10232 := cos.FastLog2Ceil(1023)
	log10252 := cos.FastLog2Ceil(1025)
	tassert.Fatalf(t, log32 == 2, "wrong ceil(log2) result; got %d, expected %d", log32, 2)
	tassert.Fatalf(t, log52 == 3, "wrong ceil(log2) result; got %d, expected %d", log52, 3)
	tassert.Fatalf(t, log152 == 4, "wrong ceil(log2) result; got %d, expected %d", log152, 4)
	tassert.Fatalf(t, log10232 == 10, "wrong ceil(log2) result; got %d, expected %d", log10232, 10)
	tassert.Fatalf(t, log10252 == 11, "wrong ceil(log2) result; got %d, expected %d", log10252, 11)
}

func TestMinDuration(t *testing.T) {
	baseTime := time.Minute

	tassert.Fatalf(t, cos.MinDuration(baseTime, baseTime+time.Second) == baseTime, "expected %s to be smaller than %s", baseTime, baseTime+time.Second)
	tassert.Fatalf(t, cos.MinDuration(baseTime, baseTime-time.Second) == baseTime-time.Second, "expected %s to be smaller than %s", baseTime-time.Second, baseTime)
	tassert.Fatalf(t, cos.MinDuration(baseTime, baseTime) == baseTime, "expected %s to be the same as %s", baseTime, baseTime)
}

func TestCeilAlign(t *testing.T) {
	tassert.Fatalf(t, cos.CeilAlign(12, 3) == 12, "got %d, expected 12", cos.CeilAlign(12, 3))
	tassert.Fatalf(t, cos.CeilAlign(10, 3) == 12, "got %d, expected 12", cos.CeilAlign(10, 3))
	tassert.Fatalf(t, cos.CeilAlign(10, 1) == 10, "got %d, expected 10", cos.CeilAlign(10, 1))
}

func TestMin(t *testing.T) {
	tassert.Errorf(t, cos.Min(0, 1, 2, 3, 4, 5, 1) == 0, "expected 0 to be the smallest, got %d", cos.Min(0, 1, 2, 3, 4, 5, 1))
	tassert.Errorf(t, cos.Min(10, 100, -2) == -2, "expected -2 to be the smallest, got %d", cos.Min(10, 100, -2))
	tassert.Errorf(t, cos.Min(1, 0) == 0, "expected 0 to be the smallest, got %d", cos.Min(1, 0))
}
