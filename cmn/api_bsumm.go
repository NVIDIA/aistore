// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "sort"

type (
	// bucket summary (result) for a given bucket
	BckSumm struct {
		Bck
		ObjCount       uint64  `json:"count,string"`
		Size           uint64  `json:"size,string"`
		TotalDisksSize uint64  `json:"disks_size,string"`
		UsedPct        float64 `json:"used_pct"`
	}
	BckSummaries []BckSumm
)

func (bs *BckSumm) aggregate(bckSummary BckSumm) {
	bs.ObjCount += bckSummary.ObjCount
	bs.Size += bckSummary.Size
	bs.TotalDisksSize += bckSummary.TotalDisksSize
	bs.UsedPct = float64(bs.Size) * 100 / float64(bs.TotalDisksSize)
}

//////////////////////
// BucketsSummaries //
//////////////////////

// interface guard
var _ sort.Interface = (*BckSummaries)(nil)

func (s BckSummaries) Len() int           { return len(s) }
func (s BckSummaries) Less(i, j int) bool { return s[i].Bck.Less(&s[j].Bck) }
func (s BckSummaries) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s BckSummaries) Aggregate(summary BckSumm) BckSummaries {
	for idx, bckSummary := range s {
		if bckSummary.Bck.Equal(&summary.Bck) {
			bckSummary.aggregate(summary)
			s[idx] = bckSummary
			return s
		}
	}
	s = append(s, summary)
	return s
}

func (s BckSummaries) Get(bck Bck) (BckSumm, bool) {
	for _, bckSummary := range s {
		if bckSummary.Bck.Equal(&bck) {
			return bckSummary, true
		}
	}
	return BckSumm{}, false
}
