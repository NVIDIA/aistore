// Package objwalk provides core functionality for reading the list of a bucket objects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestConcatObjLists(t *testing.T) {
	tests := []struct {
		name       string
		objCounts  []int
		maxSize    uint
		pageMarker bool
	}{
		// * `st` stands for "single target"
		{name: "st/all", objCounts: []int{10}, maxSize: 0, pageMarker: false},
		{name: "st/half", objCounts: []int{10}, maxSize: 5, pageMarker: true},
		{name: "st/all_with_marker", objCounts: []int{10}, maxSize: 10, pageMarker: true},
		{name: "st/more_than_all", objCounts: []int{10}, maxSize: 11, pageMarker: false},

		// * `mt` stands for "multiple targets"
		// * `one` stands for "one target has objects"
		{name: "mt/one/all", objCounts: []int{0, 0, 10}, maxSize: 0, pageMarker: false},
		{name: "mt/one/half", objCounts: []int{0, 0, 10}, maxSize: 5, pageMarker: true},
		{name: "mt/one/all_with_marker", objCounts: []int{0, 0, 10}, maxSize: 10, pageMarker: true},
		{name: "mt/one/more_than_all", objCounts: []int{0, 0, 10}, maxSize: 11, pageMarker: false},

		// * `mt` stands for "multiple targets"
		// * `more` stands for "more than one target has objects"
		{name: "mt/more/all", objCounts: []int{5, 1, 4, 10}, maxSize: 0, pageMarker: false},
		{name: "mt/more/half", objCounts: []int{5, 1, 4, 10}, maxSize: 10, pageMarker: true},
		{name: "mt/more/all_with_marker", objCounts: []int{5, 1, 4, 10}, maxSize: 20, pageMarker: true},
		{name: "mt/more/more_than_all", objCounts: []int{5, 1, 4, 10}, maxSize: 21, pageMarker: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				lists          = make([]*cmn.BucketList, 0, len(test.objCounts))
				expectedObjCnt = 0
			)
			for _, objCount := range test.objCounts {
				list := &cmn.BucketList{}
				for i := 0; i < objCount; i++ {
					list.Entries = append(list.Entries, &cmn.BucketEntry{
						Name: cmn.RandString(5),
					})
				}
				lists = append(lists, list)
				expectedObjCnt += len(list.Entries)
			}
			expectedObjCnt = cmn.Min(expectedObjCnt, int(test.maxSize))

			objs := ConcatObjLists(lists, test.maxSize)
			tassert.Errorf(
				t, test.maxSize == 0 || len(objs.Entries) == expectedObjCnt,
				"number of objects (%d) is different than expected (%d)", len(objs.Entries), expectedObjCnt,
			)
			tassert.Errorf(
				t, (objs.PageMarker != "") == test.pageMarker,
				"page marker expected to be set=%t", test.pageMarker,
			)
		})
	}
}
