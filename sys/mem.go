// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package sys

type MemStat struct {
	Total      uint64
	Used       uint64
	Free       uint64
	BuffCache  uint64
	ActualFree uint64
	ActualUsed uint64
	SwapTotal  uint64
	SwapFree   uint64
	SwapUsed   uint64
}

// Mem returns memory and swap stats for host OS or for container depending on
// where the app is running: on hardware or inside any container.
// Returns and error if memory stats cannot be read. If the function fails to
// read container's stats, it returns host OS stats
// NOTE: Swap stats is always host OS ones.
func Mem() (MemStat, error) {
	if !containerized {
		return HostMem()
	}
	return ContainerMem()
}
