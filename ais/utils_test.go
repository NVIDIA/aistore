// Package ais_test provides tests of ais package.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestSizeToStr(t *testing.T) {
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

func TestStrToSize(t *testing.T) {
	type tstruct struct {
		val   int64
		str   string
		units string
	}
	tests := []tstruct{
		{0, "0", cos.UnitsIEC},
		{0, "0B", cos.UnitsIEC},
		{10, "10B", cos.UnitsIEC},
		{512, "0.5K", cos.UnitsIEC},
		{1000, "1000B", cos.UnitsIEC},
		{1024 * 1000, "1000KiB", cos.UnitsIEC},
		{1024 * 1024, "1MiB", cos.UnitsIEC},
		{1024 * 1024 * 2, "2m", cos.UnitsIEC},
		{1024 * 1024 * 1024 * 3, "3GiB", cos.UnitsIEC},
		{1024 * 1024 * 1024 * 1024 * 17, "17TiB", cos.UnitsIEC},
		{1024 * 1024 * 1024 * 1024 * 1024 * 2, "2048TiB", cos.UnitsIEC},

		{0, "0", ""},
		{0, "0B", ""},
		{10, "10B", ""},
		{500, "0.5K", ""},
		{1000, "1000B", ""}, // suffix takes precedence
		{1024 * 1000, "1000KiB", ""},
		{1024 * 1024, "1MiB", ""},
		{1000 * 1000 * 2, "2m", ""}, // ditto
		{1024 * 1024 * 1024 * 3, "3GiB", ""},
		{1024 * 1024 * 1024 * 1024 * 17, "17TiB", ""},
		{1024 * 1024 * 1024 * 1024 * 1024 * 2, "2048TiB", ""},
	}

	for _, tst := range tests {
		n, e := cos.ParseSize(tst.str, tst.units)
		if e != nil {
			t.Errorf("Failed to convert %s: %v", tst.str, e)
		}
		if n != tst.val {
			t.Errorf("Expected %d got %d", tst.val, n)
		}
	}
}
