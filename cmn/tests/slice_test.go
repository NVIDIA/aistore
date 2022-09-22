// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
)

type discardEntriesTestCase struct {
	entries []*cmn.ObjEntry
	size    int
}

func generateEntries(size int) []*cmn.ObjEntry {
	result := make([]*cmn.ObjEntry, 0, size)
	for i := 0; i < size; i++ {
		result = append(result, &cmn.ObjEntry{Name: fmt.Sprintf("%d", i)})
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
		original := append([]*cmn.ObjEntry(nil), tc.entries...)
		entries := discardFirstEntries(tc.entries, tc.size)
		expSize := cos.Max(0, len(original)-tc.size)
		tassert.Errorf(t, len(entries) == expSize, "incorrect size. expected %d; got %d", expSize, len(entries))
		if len(entries) > 0 {
			tassert.Errorf(t, entries[0] == original[tc.size],
				"incorrect elements. expected %s, got %s", entries[0].Name, original[tc.size].Name)
		}
	}
}

func discardFirstEntries(entries []*cmn.ObjEntry, n int) []*cmn.ObjEntry {
	if n == 0 {
		return entries
	}
	if n >= len(entries) {
		return entries[:0]
	}

	toDiscard := cos.Min(len(entries), n)

	copy(entries, entries[toDiscard:])
	for i := len(entries) - toDiscard; i < len(entries); i++ {
		entries[i] = nil
	}

	return entries[:len(entries)-toDiscard]
}
