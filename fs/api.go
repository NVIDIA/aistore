// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ios"
)

// available mountpaths: disk name suffix
// NOTE: when adding/updating, check HasAlert below
const (
	DiskFault    = "(faulted)"        // disabled by FSHC
	DiskOOS      = "(out-of-space)"   // (capacity)
	Disk2Disable = "(->disabled)"     // FlagBeingDisabled (in transition)
	Disk2Detach  = "(->detach)"       // FlagBeingDetached (ditto)
	DiskHighWM   = "(low-free-space)" // (capacity)
)

// !available mountpath // TODO: not yet used; readability
const (
	DiskDisabled = "(mp-disabled)"
	DiskDetached = "(mp-detached)"
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
		if alert, _ := HasAlert(cdf.Disks); alert != "" {
			return true
		}
	}
	return false
}

// [convention] <DISK-NAME>[(<alert>)]
// Returns "" and (-1) when no alerts found otherwise, returns alert name and its index in the DISK-NAME string
func HasAlert(disks []string) (alert string, idx int) {
	var alerts = []string{DiskFault, DiskOOS, Disk2Disable, Disk2Detach, DiskHighWM}
	for _, disk := range disks {
		for _, a := range alerts {
			if idx = strings.Index(disk, a); idx > 0 {
				return disk[idx:], idx
			}
			// expecting TargetCDF to contain only _available_ mountpaths
			debug.Assert(!strings.Contains(disk, DiskDisabled), disk)
			debug.Assert(!strings.Contains(disk, DiskDetached), disk)
		}
	}
	return "", -1
}

func (cdf *CDF) _alert(a string) {
	disks := cdf.Disks
	if _, idx := HasAlert(disks); idx < 0 {
		// clone just once upon the first alert
		disks = make([]string, len(cdf.Disks))
	}
	for i, d := range cdf.Disks {
		disks[i] = d
		if !strings.Contains(d, a) {
			disks[i] = d + a
		}
	}
	cdf.Disks = disks
}
