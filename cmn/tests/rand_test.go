// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tassert"
)

func TestRandStringStrongSmoke(t *testing.T) {
	var (
		ss           = cmn.NewStringSet()
		iterations   = 1000
		stringLength = 20
	)

	for i := 0; i < iterations; i++ {
		ss.Add(cmn.RandStringStrong(stringLength))
	}
	tassert.Fatalf(t, len(ss) == iterations, "expected to generate %d unique strings, got %d", iterations, len(ss))
}
