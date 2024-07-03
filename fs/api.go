// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ios"
)

// disk name suffix (see HasAlert below)
const (
	DiskFaulted = "(faulted)"      // disabled by FSHC
	DiskOOS     = "(out-of-space)" // (capacity)
	DiskDisable = "(->disabled)"   // FlagBeingDisabled (in transition)
	DiskDetach  = "(->detach)"     // FlagBeingDetached (ditto)
	DiskHighWM  = "(low-on-space)" // (capacity)

	// NOTE: when adding/updating see "must contain" below
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
		Disks []string  `json:"disks"` // owned or shared disks (ios.FsDisks map => slice); "name[.faulted | degraded]"
		Label ios.Label `json:"mountpath_label"`
		FS    cos.FS    `json:"fs"`
	}
	// Target (cumulative) CDF
	TargetCDF struct {
		Mountpaths map[string]*CDF // mpath => [Capacity, Disks, FS (CDF)]
		TotalUsed  uint64          `json:"total_used,string"`  // bytes
		TotalAvail uint64          `json:"total_avail,string"` // bytes
		PctMax     int32           `json:"pct_max"`            // max used (%)
		PctAvg     int32           `json:"pct_avg"`            // avg used (%)
		PctMin     int32           `json:"pct_min"`            // min used (%)
		CsErr      string          `json:"cs_err"`             // OOS or high-wm error message; disk fault
	}
)

// [backward compatibility]: v3.22 cdf* structures
type (
	CDFv322 struct {
		Capacity
		Disks []string `json:"disks"`
		FS    string   `json:"fs"`
	}
	TargetCDFv322 struct {
		Mountpaths map[string]*CDFv322
		PctMax     int32  `json:"pct_max"`
		PctAvg     int32  `json:"pct_avg"`
		PctMin     int32  `json:"pct_min"`
		CsErr      string `json:"cs_err"`
	}
)

func InitCDF(tcdf *TargetCDF) {
	avail := GetAvail()
	tcdf.Mountpaths = make(map[string]*CDF, len(avail))
	for mpath := range avail {
		tcdf.Mountpaths[mpath] = &CDF{}
	}
}

func (tcdf *TargetCDF) HasAlerts() bool {
	for _, cdf := range tcdf.Mountpaths {
		if alert, _ := cdf.HasAlert(); alert != "" {
			return true
		}
	}
	return false
}

// Returns "" and (-1) when no alerts found;
// otherwise, returns alert name and its index in the string, which is formatted as follows:
// <DISK-NAME>[<ALERT-NAME>]
func (cdf *CDF) HasAlert() (alert string, idx int) {
	var alerts = []string{DiskFaulted, DiskOOS, DiskDisable, DiskDetach, DiskHighWM} // NOTE: must contain all flags
	for _, disk := range cdf.Disks {
		for _, a := range alerts {
			if idx = strings.Index(disk, a); idx > 0 {
				return disk[idx:], idx
			}
		}
	}
	return "", -1
}

func (cdf *CDF) _alert(a string) {
	for i, d := range cdf.Disks {
		if !strings.Contains(d, a) {
			cdf.Disks[i] = d + a
		}
	}
}
