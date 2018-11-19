/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package lru

import (
	"container/heap"
	"reflect"
	"sort"
	"testing"
	"time"
)

type fileInfos []fileInfo

func (fis fileInfos) Len() int {
	return len(fis)
}

func (fis fileInfos) Less(i, j int) bool {
	return fis[i].usetime.Before(fis[j].usetime)
}

func (fis fileInfos) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func Test_HeapEqual(t *testing.T) {
	tcs := []fileInfos{
		{
			{
				"o1",
				time.Date(2018, time.June, 26, 1, 2, 3, 0, time.UTC),
				1024,
			},
			{
				"o2",
				time.Date(2018, time.June, 26, 1, 3, 3, 0, time.UTC),
				1025,
			},
		},
		{
			{
				"o3",
				time.Date(2018, time.June, 26, 1, 5, 3, 0, time.UTC),
				1024,
			},
			{
				"o4",
				time.Date(2018, time.June, 26, 1, 4, 3, 0, time.UTC),
				1025,
			},
		},
		{
			{
				"o5",
				time.Date(2018, time.June, 26, 1, 5, 3, 0, time.UTC),
				1024,
			},
		},
		{
			{
				"o6",
				time.Date(2018, time.June, 26, 1, 5, 3, 0, time.UTC),
				10240,
			},
			{
				"o7",
				time.Date(2018, time.June, 28, 1, 4, 3, 0, time.UTC),
				102500,
			},
			{
				"o8",
				time.Date(2018, time.June, 30, 1, 5, 3, 0, time.UTC),
				1024,
			},
			{
				"o9",
				time.Date(2018, time.June, 20, 1, 4, 3, 0, time.UTC),
				10250,
			},
		},
	}

	h := &fileInfoMinHeap{}
	heap.Init(h)

	for tcNum, tc := range tcs {
		for i := range tc {
			heap.Push(h, &tc[i])
		}

		act := make(fileInfos, len(tc))
		for i := 0; i < len(tc); i++ {
			act[i] = *heap.Pop(h).(*fileInfo)
		}

		sort.Sort(tc)
		if !reflect.DeepEqual(act, tc) {
			t.Fatalf("Test case %d failed", tcNum+1)
		}
	}
}
