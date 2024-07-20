// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ios

type (
	DiskStats struct {
		RBps, Ravg, WBps, Wavg, Util int64
	}
	AllDiskStats map[string]DiskStats
)
