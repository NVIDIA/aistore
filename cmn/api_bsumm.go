// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"math"
	"sort"

	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	// bucket summary (result) for a given bucket
	BckSumm struct {
		Bck
		ObjCount uint64 `json:"obj_count,string"`
		ObjSize  struct {
			Min int64 `json:"obj_min_size"`
			Avg int64 `json:"obj_avg_size"`
			Max int64 `json:"obj_max_size"`
		}
		Size           uint64 `json:"size,string"`
		TotalDisksSize uint64 `json:"total_disks_size,string"`
		UsedPct        uint64 `json:"used_pct"`
	}
	BckSummaries []*BckSumm
)

func NewBckSumm(bck *Bck, totalDisksSize uint64) (bs *BckSumm) {
	bs = &BckSumm{Bck: *bck, TotalDisksSize: totalDisksSize}
	bs.ObjSize.Min = math.MaxInt64
	return
}

//////////////////////
// BucketsSummaries //
//////////////////////

// interface guard
var _ sort.Interface = (*BckSummaries)(nil)

func (s BckSummaries) Len() int           { return len(s) }
func (s BckSummaries) Less(i, j int) bool { return s[i].Bck.Less(&s[j].Bck) }
func (s BckSummaries) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s BckSummaries) Aggregate(from *BckSumm) BckSummaries {
	for _, to := range s {
		if to.Bck.Equal(&from.Bck) {
			aggr(from, to)
			return s
		}
	}
	if from.ObjCount > 0 {
		from.ObjSize.Avg = int64(cos.DivRoundU64(from.Size, from.ObjCount))
	}
	s = append(s, from)
	return s
}

func aggr(from, to *BckSumm) {
	if from.ObjSize.Min < to.ObjSize.Min {
		to.ObjSize.Min = from.ObjSize.Min
	}
	if from.ObjSize.Max > to.ObjSize.Max {
		to.ObjSize.Max = from.ObjSize.Max
	}
	to.ObjCount += from.ObjCount
	to.Size += from.Size
}

func (s BckSummaries) Finalize(dsize map[string]uint64, testingEnv bool) {
	var totalDisksSize uint64
	for _, tsiz := range dsize {
		totalDisksSize += tsiz
		if testingEnv {
			break
		}
	}
	for _, summ := range s {
		if summ.ObjSize.Min == math.MaxInt64 {
			summ.ObjSize.Min = 0 // alternatively, CLI to show `-`
		}
		if summ.ObjCount > 0 {
			summ.ObjSize.Avg = int64(cos.DivRoundU64(summ.Size, summ.ObjCount))
		}
		summ.UsedPct = cos.DivRoundU64(summ.Size*100, totalDisksSize)
	}
}
