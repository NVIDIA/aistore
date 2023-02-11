// Package ais_test provides tests of ais package.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestBytesToStr(t *testing.T) {
	type tstruct struct {
		val int64
		num int
		str string
	}
	tests := []tstruct{
		{0, 0, "0B"},
		{0, 1, "0B"},
		{10, 0, "10B"},
		{1000, 0, "1000B"},
		{1100, 0, "1KiB"},
		{1100, 2, "1.07KiB"},
		{1024 * 1000, 0, "1000KiB"},
		{1024 * 1025, 0, "1MiB"},
		{1024 * 1024 * 1024 * 3, 3, "3.000GiB"},
		{1024 * 1024 * 1024 * 1024 * 17, 0, "17TiB"},
		{1024 * 1024 * 1024 * 1024 * 1024 * 2, 0, "2048TiB"},
	}

	for _, tst := range tests {
		s := cos.ToSizeIEC(tst.val, tst.num)
		if s != tst.str {
			t.Errorf("Expected %s got %s", tst.str, s)
		}
	}
}

func TestStrToBytes(t *testing.T) {
	type tstruct struct {
		val int64
		str string
	}
	tests := []tstruct{
		{0, "0"},
		{0, "0B"},
		{10, "10B"},
		{512, "0.5K"},
		{1000, "1000B"},
		{1024 * 1000, "1000KiB"},
		{1024 * 1024, "1MiB"},
		{1024 * 1024 * 2, "2m"},
		{1024 * 1024 * 1024 * 3, "3GiB"},
		{1024 * 1024 * 1024 * 1024 * 17, "17TiB"},
		{1024 * 1024 * 1024 * 1024 * 1024 * 2, "2048TiB"},
	}

	for _, tst := range tests {
		n, e := cos.ParseSizeIEC(tst.str)
		if e != nil {
			t.Errorf("Failed to convert %s: %v", tst.str, e)
		}
		if n != tst.val {
			t.Errorf("Expected %d got %d", tst.val, n)
		}
	}
}
