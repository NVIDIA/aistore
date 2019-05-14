/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestFastLog2(t *testing.T) {
	log22 := cmn.FastLog2(2)
	log42 := cmn.FastLog2(4)
	log82 := cmn.FastLog2(8)
	log10242 := cmn.FastLog2(1024)
	tassert.Fatalf(t, log22 == 1, "wrong power of 2 log2 result; got %d, expected %d", log22, 1)
	tassert.Fatalf(t, log42 == 2, "wrong power of 2 log2 result; got %d, expected %d", log42, 2)
	tassert.Fatalf(t, log82 == 3, "wrong power of 2 log2 result; got %d, expected %d", log82, 3)
	tassert.Fatalf(t, log10242 == 10, "wrong power of 2 log2 result; got %d, expected %d", log10242, 10)

	log32 := cmn.FastLog2(3)
	log52 := cmn.FastLog2(5)
	log152 := cmn.FastLog2(15)
	log10232 := cmn.FastLog2(1023)
	log10252 := cmn.FastLog2(1025)
	tassert.Fatalf(t, log32 == 1, "wrong log2 result; got %d, expected %d", log32, 1)
	tassert.Fatalf(t, log52 == 2, "wrong log2 result; got %d, expected %d", log52, 2)
	tassert.Fatalf(t, log152 == 3, "wrong log2 result; got %d, expected %d", log152, 3)
	tassert.Fatalf(t, log10232 == 9, "wrong log2 result; got %d, expected %d", log10232, 9)
	tassert.Fatalf(t, log10252 == 10, "wrong log2 result; got %d, expected %d", log10252, 10)
}

func TestFastLog2Ceil(t *testing.T) {
	log22 := cmn.FastLog2(2)
	log42 := cmn.FastLog2(4)
	log82 := cmn.FastLog2(8)
	log10242 := cmn.FastLog2(1024)
	tassert.Fatalf(t, log22 == 1, "wrong power of 2 ceil(log2) result; got %d, expected %d", log22, 1)
	tassert.Fatalf(t, log42 == 2, "wrong power of 2 ceil(log2) result; got %d, expected %d", log42, 2)
	tassert.Fatalf(t, log82 == 3, "wrong power of 2 ceil(log2) result; got %d, expected %d", log82, 3)
	tassert.Fatalf(t, log10242 == 10, "wrong power of 2 ceil(log2) result; got %d, expected %d", log10242, 10)

	log32 := cmn.FastLog2Ceil(3)
	log52 := cmn.FastLog2Ceil(5)
	log152 := cmn.FastLog2Ceil(15)
	log10232 := cmn.FastLog2Ceil(1023)
	log10252 := cmn.FastLog2Ceil(1025)
	tassert.Fatalf(t, log32 == 2, "wrong ceil(log2) result; got %d, expected %d", log32, 2)
	tassert.Fatalf(t, log52 == 3, "wrong ceil(log2) result; got %d, expected %d", log52, 3)
	tassert.Fatalf(t, log152 == 4, "wrong ceil(log2) result; got %d, expected %d", log152, 4)
	tassert.Fatalf(t, log10232 == 10, "wrong ceil(log2) result; got %d, expected %d", log10232, 10)
	tassert.Fatalf(t, log10252 == 11, "wrong ceil(log2) result; got %d, expected %d", log10252, 11)
}

func TestMinDur(t *testing.T) {
	baseTime := time.Minute

	tassert.Fatalf(t, cmn.MinDur(baseTime, baseTime+time.Second) == baseTime, "expected %s to be smaller than %s", baseTime, baseTime+time.Second)
	tassert.Fatalf(t, cmn.MinDur(baseTime, baseTime-time.Second) == baseTime-time.Second, "expected %s to be smaller than %s", baseTime-time.Second, baseTime)
	tassert.Fatalf(t, cmn.MinDur(baseTime, baseTime) == baseTime, "expected %s to be the same as %s", baseTime, baseTime)
}

func TestCeilAlign(t *testing.T) {
	tassert.Fatalf(t, cmn.CeilAlign(12, 3) == 12, "got %d, expected 12", cmn.CeilAlign(12, 3))
	tassert.Fatalf(t, cmn.CeilAlign(10, 3) == 12, "got %d, expected 12", cmn.CeilAlign(10, 3))
	tassert.Fatalf(t, cmn.CeilAlign(10, 1) == 10, "got %d, expected 10", cmn.CeilAlign(10, 1))
}
