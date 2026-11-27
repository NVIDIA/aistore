// Package cos_test: unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"math"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		size, units string
		want        int64
	}{
		{"10GiB", "", 10 * cos.GiB},
		{"1.5KiB", "", 1536},
		{"0.1MB", cos.UnitsSI, 100 * cos.KB},
		{"10KB", cos.UnitsIEC, 10 * cos.KiB},
		{"8388607TiB", "", 8388607 * cos.TiB},
		{"-8388608TiB", "", math.MinInt64},
		{"9223372036854775807", "", math.MaxInt64},
	}
	for _, tc := range tests {
		got, err := cos.ParseSize(tc.size, tc.units)
		if err != nil || got != tc.want {
			t.Errorf("ParseSize(%q, %q) = %d, %v; want %d", tc.size, tc.units, got, err, tc.want)
		}
	}
}

func TestParseSizeOutOfRange(t *testing.T) {
	// Values that do not fit in int64 must fail rather than wrap around
	for _, size := range []string{
		"8388608TiB", "9000000TiB", "-8388609TiB", "9223372036855GB",
		"8388608.0TiB", "1.0e30", "9223372036854775808.0",
	} {
		if got, err := cos.ParseSize(size, ""); err == nil {
			t.Errorf("ParseSize(%q) = %d, want an error", size, got)
		}
	}
}
