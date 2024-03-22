// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ios"
)

type (
	Capacity struct {
		Used    uint64 `json:"used,string"`  // bytes
		Avail   uint64 `json:"avail,string"` // ditto
		PctUsed int32  `json:"pct_used"`     // %% used (redundant ok)
	}
	// Capacity, Disks, Filesystem (CDF)
	CDF struct {
		Capacity
		Disks []string  `json:"disks"` // owned or shared disks (ios.FsDisks map => slice)
		Label ios.Label `json:"mountpath_label"`
		FS    cos.FS    `json:"fs"`
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

func InitCDF(tcdf *TargetCDF) {
	avail := GetAvail()
	tcdf.Mountpaths = make(map[string]*CDF, len(avail))
	for mpath := range avail {
		tcdf.Mountpaths[mpath] = &CDF{}
	}
}
