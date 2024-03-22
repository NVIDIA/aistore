// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package ios

// Introduced in v3.23, mountpath labels are used to deliver extended (mountpath usage related)
// functionality that includes:
//   - mapping of the mountpath to its underlying disk(s), if any
//     (potentially useful in highly virtualized/containerized environments);
//   - disk sharing between multiple mountpaths;
//   - storage classes (as in: "different storages - for different buckets");
//   - user-defined grouping of the mountpaths
//     (e.g., to reflect different non-overlapping storage capacities and/or storage classes)
//
// In v3.23, user-assigned `ios.Label` simply implies that user takes a full responsibility
// for filesystem sharing (or not sharing) across mountpaths
type Label string

const TestLabel = Label("test-label")

func (label Label) IsNil() bool { return label == "" }

func (label Label) ToLog() string {
	if label == "" {
		return ""
	}
	return ", \"" + string(label) + "\""
}
