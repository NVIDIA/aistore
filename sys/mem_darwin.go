// Package sys provides methods to read system information
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"math"
)

// HostMem returns memory and swap stats for a host OS
// TODO: remove hardcoded constants
func HostMem() (MemStat, error) {
	return MemStat{
		Total:      uint64(math.MaxUint32),
		Free:       uint64(math.MaxUint32),
		ActualFree: uint64(math.MaxUint32),

		SwapTotal: uint64(math.MaxUint32),
		SwapFree:  uint64(math.MaxUint32),
	}, nil
}

// ContainerMem returns memory stats for container and swap stats for a host OS.
// If memory is not restricted for a container, the function returns host OS stats.
// TODO: remove hardcoded constants
func ContainerMem() (MemStat, error) {
	return MemStat{
		Total:      uint64(math.MaxUint32),
		Free:       uint64(math.MaxUint32),
		ActualFree: uint64(math.MaxUint32),

		SwapTotal: uint64(math.MaxUint32),
		SwapFree:  uint64(math.MaxUint32),
	}, nil
}
