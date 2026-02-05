// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"strconv"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
)

type discardEntriesTestCase struct {
	entries cmn.LsoEntries
	size    int
}

func generateEntries(size int) cmn.LsoEntries {
	result := make(cmn.LsoEntries, 0, size)
	for i := range size {
		result = append(result, &cmn.LsoEnt{Name: strconv.Itoa(i)})
	}
	return result
}

func TestDiscardFirstEntries(t *testing.T) {
	testCases := []discardEntriesTestCase{
		{generateEntries(100), 1},
		{generateEntries(1), 1},
		{generateEntries(100), 0},
		{generateEntries(1), 0},
		{generateEntries(100), 50},
		{generateEntries(1), 50},
		{generateEntries(100), 100},
		{generateEntries(100), 150},
	}

	for _, tc := range testCases {
		t.Logf("testcase %d/%d", len(tc.entries), tc.size)
		original := append(cmn.LsoEntries(nil), tc.entries...)
		entries := discardFirstEntries(tc.entries, tc.size)
		expSize := max(0, len(original)-tc.size)
		tassert.Errorf(t, len(entries) == expSize, "incorrect size. expected %d; got %d", expSize, len(entries))
		if len(entries) > 0 {
			tassert.Errorf(t, entries[0] == original[tc.size],
				"incorrect elements. expected %s, got %s", entries[0].Name, original[tc.size].Name)
		}
	}
}

func discardFirstEntries(entries cmn.LsoEntries, n int) cmn.LsoEntries {
	if n == 0 {
		return entries
	}
	if n >= len(entries) {
		return entries[:0]
	}

	toDiscard := min(len(entries), n)

	copy(entries, entries[toDiscard:])
	for i := len(entries) - toDiscard; i < len(entries); i++ {
		entries[i] = nil
	}

	return entries[:len(entries)-toDiscard]
}
