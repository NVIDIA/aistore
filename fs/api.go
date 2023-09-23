// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package fs

type (
	Capacity struct {
		Used    uint64 `json:"used,string"`  // bytes
		Avail   uint64 `json:"avail,string"` // ditto
		PctUsed int32  `json:"pct_used"`     // %% used (redundant ok)
	}
	// Capacity, Disks, Filesystem (CDF)
	// (not to be confused with Cumulative Distribution Function)
	CDF struct {
		Capacity
		Disks []string `json:"disks"` // owned disks (ios.FsDisks map => slice)
		FS    string   `json:"fs"`    // cos.Fs + cos.FsID
	}
	// Target (cumulative) CDF
	TargetCDF struct {
		Mountpaths map[string]*CDF // mpath => [Capacity, Disks, FS (CDF)]
		PctMax     int32           `json:"pct_max"` // max used (%)
		PctAvg     int32           `json:"pct_avg"` // avg used (%)
		PctMin     int32           `json:"pct_min"` // min used (%)
		CsErr      string          `json:"cs_err"`  // OOS or high-wm error message
	}
)
