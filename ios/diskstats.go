// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ios

type (
	diskBlockStat interface {
		Reads() int64
		ReadBytes() int64
		Writes() int64
		WriteBytes() int64

		IOMs() int64
		WriteMs() int64
		ReadMs() int64
	}

	diskBlockStats map[string]diskBlockStat
)
