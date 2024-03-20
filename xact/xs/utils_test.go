// Package xs_test - basic list-concatenation unit tests.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

func TestConcatObjLists(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping %s in short mode", t.Name())
	}
	tests := []struct {
		name      string
		objCounts []int
		maxSize   int
		token     bool
	}{
		// * `st` stands for "single target"
		{name: "st/all", objCounts: []int{10}, maxSize: 0, token: false},
		{name: "st/half", objCounts: []int{10}, maxSize: 5, token: true},
		{name: "st/all_with_marker", objCounts: []int{10}, maxSize: 10, token: true},
		{name: "st/more_than_all", objCounts: []int{10}, maxSize: 11, token: false},

		// * `mt` stands for "multiple targets"
		// * `one` stands for "one target has objects"
		{name: "mt/one/all", objCounts: []int{0, 0, 10}, maxSize: 0, token: false},
		{name: "mt/one/half", objCounts: []int{0, 0, 10}, maxSize: 5, token: true},
		{name: "mt/one/all_with_marker", objCounts: []int{0, 0, 10}, maxSize: 10, token: true},
		{name: "mt/one/more_than_all", objCounts: []int{0, 0, 10}, maxSize: 11, token: false},

		// * `mt` stands for "multiple targets"
		// * `more` stands for "more than one target has objects"
		{name: "mt/more/all", objCounts: []int{5, 1, 4, 10}, maxSize: 0, token: false},
		{name: "mt/more/half", objCounts: []int{5, 1, 4, 10}, maxSize: 10, token: true},
		{name: "mt/more/all_with_marker", objCounts: []int{5, 1, 4, 10}, maxSize: 20, token: true},
		{name: "mt/more/more_than_all", objCounts: []int{5, 1, 4, 10}, maxSize: 21, token: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				lists          = make([]*cmn.LsoRes, 0, len(test.objCounts))
				expectedObjCnt = 0
			)
			for _, objCount := range test.objCounts {
				list := &cmn.LsoRes{}
				for range objCount {
					list.Entries = append(list.Entries, &cmn.LsoEnt{
						Name: trand.String(5),
					})
				}
				lists = append(lists, list)
				expectedObjCnt += len(list.Entries)
			}
			expectedObjCnt = min(expectedObjCnt, test.maxSize)

			objs := concatLso(lists, test.maxSize)
			tassert.Errorf(
				t, test.maxSize == 0 || len(objs.Entries) == expectedObjCnt,
				"number of objects (%d) is different from expected (%d)", len(objs.Entries), expectedObjCnt,
			)
			tassert.Errorf(
				t, (objs.ContinuationToken != "") == test.token,
				"continuation token expected to be set=%t", test.token,
			)
		})
	}
}

// concatLso takes a slice of object lists and concatenates them: all lists
// are appended to the first one.
// If maxSize is greater than 0, the resulting list is sorted and truncated. Zero
// or negative maxSize means returning all objects.
func concatLso(lists []*cmn.LsoRes, maxSize int) (objs *cmn.LsoRes) {
	if len(lists) == 0 {
		return &cmn.LsoRes{}
	}

	objs = &cmn.LsoRes{}
	objs.Entries = make(cmn.LsoEntries, 0)

	for _, l := range lists {
		objs.Flags |= l.Flags
		objs.Entries = append(objs.Entries, l.Entries...)
	}

	if len(objs.Entries) == 0 {
		return objs
	}

	// For corner case: we have objects with replicas on page threshold
	// we have to sort taking status into account. Otherwise wrong
	// one(Status=moved) may get into the response
	cmn.SortLso(objs.Entries)

	// Remove duplicates
	objs.Entries = cmn.DedupLso(objs.Entries, maxSize)
	l := len(objs.Entries)
	if maxSize > 0 && l >= maxSize {
		objs.ContinuationToken = objs.Entries[l-1].Name
	}
	return
}
