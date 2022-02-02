// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// copy & (offline) transform bucket to bucket
type (
	CopyBckMsg struct {
		Prefix string `json:"prefix"`  // Prefix added to each resulting object.
		DryRun bool   `json:"dry_run"` // Don't perform any PUT
	}
	TCBMsg struct {
		// Resulting objects names will have this extension. Warning: if in a source bucket exist two objects with the
		// same base name, but different extension, specifying this field might cause object overriding. This is because
		// of resulting name conflict.
		// TODO: this field might not be required when transformation on subset (template) of bucket is supported.
		Ext cos.SimpleKVs `json:"ext"`

		ID             string       `json:"id,omitempty"`              // optional, ETL only
		RequestTimeout cos.Duration `json:"request_timeout,omitempty"` // optional, ETL only

		CopyBckMsg
	}
)

// control message to generate bucket summary or summaries
type (
	BckSummMsg struct {
		UUID   string `json:"uuid"`
		Fast   bool   `json:"fast"`
		Cached bool   `json:"cached"`
	}
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

////////////
// TCBMsg //
////////////

func (msg *TCBMsg) Validate() error {
	if msg.ID == "" {
		return ErrETLMissingUUID
	}
	return nil
}

// Replace extension and add suffix if provided.
func (msg *TCBMsg) ToName(name string) string {
	if msg.Ext != nil {
		if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
			ext := name[:idx]
			if replacement, exists := msg.Ext[ext]; exists {
				name = name[:idx+1] + strings.TrimLeft(replacement, ".")
			}
		}
	}
	if msg.Prefix != "" {
		name = msg.Prefix + name
	}
	return name
}

//////////////////////
// BucketsSummary(ies)
//////////////////////

func (bs *BckSumm) Aggregate(bckSummary BckSumm) {
	bs.ObjCount += bckSummary.ObjCount
	bs.Size += bckSummary.Size
	bs.TotalDisksSize += bckSummary.TotalDisksSize
	bs.UsedPct = float64(bs.Size) * 100 / float64(bs.TotalDisksSize)
}

func (s BckSummaries) Len() int           { return len(s) }
func (s BckSummaries) Less(i, j int) bool { return s[i].Bck.Less(s[j].Bck) }
func (s BckSummaries) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s BckSummaries) Aggregate(summary BckSumm) BckSummaries {
	for idx, bckSummary := range s {
		if bckSummary.Bck.Equal(summary.Bck) {
			bckSummary.Aggregate(summary)
			s[idx] = bckSummary
			return s
		}
	}
	s = append(s, summary)
	return s
}

func (s BckSummaries) Get(bck Bck) (BckSumm, bool) {
	for _, bckSummary := range s {
		if bckSummary.Bck.Equal(bck) {
			return bckSummary, true
		}
	}
	return BckSumm{}, false
}
