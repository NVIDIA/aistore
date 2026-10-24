// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact"
)

func TestRandStringStrongSmoke(t *testing.T) {
	var (
		ss           = cos.NewStrSet()
		iterations   = 1000
		stringLength = 20
	)

	for range iterations {
		ss.Add(cos.CryptoRandS(stringLength))
	}
	tassert.Fatalf(t, len(ss) == iterations, "expected to generate %d unique strings, got %d", iterations, len(ss))
}

func TestUUIDUniqueness(t *testing.T) {
	const (
		active     = 128
		iterations = 2_000_000
	)
	var slots [active]string
	ids := make(cos.StrSet, active)
	cos.InitShortID(3482)
	for i := range iterations {
		idx := i % active
		if slots[idx] != "" {
			ids.Delete(slots[idx])
		}
		id := cos.GenUUID()
		tassert.Fatalf(t, cos.IsValidUUID(id), "invalid UUID %q", id)
		tassert.Fatalf(t, strings.IndexByte(cos.LetterRunes, id[0]) >= 0, "UUID %q must start with a letter", id)
		last := id[len(id)-1]
		tassert.Fatalf(t, last != '-' && last != '_', "UUID %q has an unsafe suffix", id)
		tassert.Fatalf(t, !xact.IsValidRebID(id), "UUID %q parses as a global rebalance ID", id)
		tassert.Fatalf(t, !ids.Contains(id), "duplicated active UUID %q after %d generations", id, i+1)
		ids.Add(id)
		slots[idx] = id
	}
}
