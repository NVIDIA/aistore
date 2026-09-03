// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func newTestLsoEntries(n int) cmn.LsoEntries {
	entries := make(cmn.LsoEntries, 0, n)
	for i := range n {
		entries = append(entries, &cmn.LsoEnt{Name: fmt.Sprintf("obj-%05d", i)})
	}
	return entries
}

func newTestDupLsoEntries(n, ntargets int) (cmn.LsoEntries, cmn.LsoEntries) {
	entries := newTestLsoEntries(n)
	dup := make(cmn.LsoEntries, 0, n*ntargets)
	for _, en := range entries {
		for range ntargets {
			dup = append(dup, en)
		}
	}
	return entries, dup
}

// apc.LsNoRecursion: de-duplication must not decide truncation on its own.
func TestFinLsoADedup(t *testing.T) {
	const (
		maxSize  = 10
		ntargets = 3
	)

	tests := []struct {
		name      string
		numEnt    int
		hasMore   bool
		wantCnt   int
		wantToken bool
	}{
		{name: "dedup-over-page", numEnt: maxSize + 2, hasMore: false, wantCnt: maxSize, wantToken: true},
		{name: "dedup-exact-page-done", numEnt: maxSize, hasMore: false, wantCnt: maxSize, wantToken: false},
		{name: "dedup-exact-page-more", numEnt: maxSize, hasMore: true, wantCnt: maxSize, wantToken: true},
		{name: "dedup-short-page-done", numEnt: maxSize - 3, hasMore: false, wantCnt: maxSize - 3, wantToken: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lsmsg := &apc.LsoMsg{PageSize: maxSize, Flags: apc.LsNoRecursion}
			entries, dup := newTestDupLsoEntries(test.numEnt, ntargets)
			objs := &cmn.LsoRes{Entries: dup}

			finLsoA(objs, lsmsg, test.hasMore)

			tassert.Fatalf(t, len(objs.Entries) == test.wantCnt,
				"expected %d entries, got %d", test.wantCnt, len(objs.Entries))
			for i, en := range objs.Entries {
				tassert.Fatalf(t, en.Name == entries[i].Name,
					"entry %d: expected %q, got %q", i, entries[i].Name, en.Name)
			}
			if !test.wantToken {
				tassert.Fatalf(t, objs.ContinuationToken == "",
					"expected no continuation token, got %q", objs.ContinuationToken)
				return
			}
			want := entries[test.wantCnt-1].Name
			tassert.Fatalf(t, objs.ContinuationToken == want,
				"expected continuation token %q, got %q", want, objs.ContinuationToken)
		})
	}
}

func TestFinLsoA(t *testing.T) {
	const maxSize = 10

	tests := []struct {
		name      string
		numEnt    int
		hasMore   bool
		wantCnt   int
		wantToken bool
	}{
		{name: "over-page", numEnt: maxSize + 1, hasMore: true, wantCnt: maxSize, wantToken: true},
		{name: "exact-page-done", numEnt: maxSize, hasMore: false, wantCnt: maxSize, wantToken: false},
		{name: "exact-page-more", numEnt: maxSize, hasMore: true, wantCnt: maxSize, wantToken: true},
		{name: "short-page-done", numEnt: maxSize - 1, hasMore: false, wantCnt: maxSize - 1, wantToken: false},
		{name: "short-page-more", numEnt: maxSize - 1, hasMore: true, wantCnt: maxSize - 1, wantToken: true},
		{name: "empty-page", numEnt: 0, hasMore: false, wantCnt: 0, wantToken: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lsmsg := &apc.LsoMsg{PageSize: maxSize}
			objs := &cmn.LsoRes{Entries: newTestLsoEntries(test.numEnt)}

			finLsoA(objs, lsmsg, test.hasMore)

			tassert.Fatalf(t, len(objs.Entries) == test.wantCnt,
				"expected %d entries, got %d", test.wantCnt, len(objs.Entries))
			if !test.wantToken {
				tassert.Fatalf(t, objs.ContinuationToken == "",
					"expected no continuation token, got %q", objs.ContinuationToken)
				return
			}
			want := objs.Entries[test.wantCnt-1].Name
			tassert.Fatalf(t, objs.ContinuationToken == want,
				"expected continuation token %q, got %q", want, objs.ContinuationToken)
		})
	}
}
