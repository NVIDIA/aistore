// Package cmn provides common types and utilities for AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"slices"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// CheckEqKeys has the custom keys that CheckEq reads
func TestCheckEqKeys(t *testing.T) {
	for _, key := range append(supportedRemAttrs[:], SourceObjMD) {
		var a, b ObjAttrs
		for _, oa := range []*ObjAttrs{&a, &b} { // the same size and checksum
			oa.Size = 1
			oa.SetCksum(cos.ChecksumOneXxh, "0123456789abcdef")
		}
		a.SetCustomKey(key, "a")
		b.SetCustomKey(key, "b")
		reads, listed := a.CheckEq(&b) != nil, slices.Contains(CheckEqKeys[:], key)
		if reads != listed {
			t.Errorf("%q: CheckEq reads it: %t, CheckEqKeys has it: %t", key, reads, listed)
		}
	}
}
