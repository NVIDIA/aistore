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

// one more entry than the page size: finLsoA must trim to `maxSize` and set the continuation token
func TestFinLsoATruncated(t *testing.T) {
	const maxSize = 10

	lsmsg := &apc.LsoMsg{PageSize: maxSize}
	entries := newTestLsoEntries(maxSize + 1)
	objs := &cmn.LsoRes{Entries: entries}

	finLsoA(objs, lsmsg)

	tassert.Fatalf(t, len(objs.Entries) == maxSize, "expected %d entries, got %d", maxSize, len(objs.Entries))
	want := entries[maxSize-1].Name
	tassert.Fatalf(t, objs.ContinuationToken == want,
		"expected continuation token %q, got %q", want, objs.ContinuationToken)
}
