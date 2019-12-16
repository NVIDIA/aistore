// Package sys provides methods to read system information
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package sys

// TODO: remove hardcoded constants
func procMem(pid int) (ProcMemStats, error) {
	return ProcMemStats{}, nil
}

// TODO: remove hardcoded constants
func procCPU(pid int) (ProcCPUStats, error) {
	return ProcCPUStats{}, nil
}
