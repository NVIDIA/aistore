// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package sys

// TODO: remove hardcoded constants
func procMem(_ int) (ProcMemStats, error) {
	return ProcMemStats{}, nil
}

// TODO: remove hardcoded constants
func procCPU(_ int) (ProcCPUStats, error) {
	return ProcCPUStats{}, nil
}
