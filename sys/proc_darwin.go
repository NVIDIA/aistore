// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package sys

// TODO: implement

func procMem(_ int) (ProcMemStats, error) {
	return ProcMemStats{}, nil
}

func procCPU(_ int) (ProcCPUStats, error) {
	return ProcCPUStats{}, nil
}

func getFDSize() (int, error) {
	return 0, nil
}
