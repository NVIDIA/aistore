// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

const (
	NumDiskMetrics = 5 // RBps, Ravg, WBps, Wavg, Util (see DiskStats)
)

type (
	DiskStats struct {
		RBps, Ravg, WBps, Wavg, Util int64
	}
	AllDiskStats map[string]DiskStats
)

// Introduced in v3.23, labels are used to deliver extended (mountpath usage related)
// functionality that includes:
//   - mapping of the mountpath to its underlying disk(s), if any
//     (potentially useful in highly virtualized/containerized environments);
//   - disk sharing between multiple mountpaths;
//   - storage classes (as in: "different storages - for different buckets");
//   - user-defined grouping of the mountpaths
//     (e.g., to reflect different non-overlapping storage capacities and/or storage classes)
//
// In v3.23, user-assigned `DiskLabel` simply implies that user takes a full responsibility
// for filesystem sharing (or not sharing) across mountpaths
type MountpathLabel string

const TestMpathLabel = MountpathLabel("test-label")

func (label MountpathLabel) IsNil() bool { return label == "" }

func (label MountpathLabel) ToLog() string {
	if label == "" {
		return ""
	}
	return ", \"" + string(label) + "\""
}
