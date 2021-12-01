// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// HostMem returns memory and swap stats for a host OS
func HostMem() (MemStat, error) {
	var caches uint64
	mem := MemStat{ActualFree: math.MaxUint64}

	setValue := func(name, valStr string) {
		val, err := strconv.ParseUint(valStr, 10, 64)
		if err != nil {
			return
		}
		val *= cos.KiB
		switch name {
		case "MemTotal":
			mem.Total = val
		case "MemFree":
			mem.Free = val
		case "MemAvailable":
			mem.ActualFree = val
		case "Cached", "Buffers":
			caches += val
		case "SwapTotal":
			mem.SwapTotal = val
		case "SwapFree":
			mem.SwapFree = val
		}
	}

	err := cos.ReadLines(hostMemPath, func(line string) error {
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			return nil
		}
		name := fields[0]
		valStr := strings.Fields(strings.TrimLeft(fields[1], " "))[0]
		setValue(name, valStr)
		return nil
	})
	if err != nil {
		return mem, err
	}

	if mem.ActualFree == math.MaxUint64 {
		mem.ActualFree = mem.Free + caches
	}
	mem.Used = mem.Total - mem.Free
	mem.ActualUsed = mem.Total - mem.ActualFree
	mem.SwapUsed = mem.SwapTotal - mem.SwapFree
	return mem, nil
}

// ContainerMem returns memory stats for container and swap stats for a host OS.
// If memory is not restricted for a container, the function returns host OS stats.
func ContainerMem() (MemStat, error) {
	mem, err := HostMem()
	if err != nil {
		return mem, err
	}
	memLimit, err := cos.ReadOneUint64(contMemLimitPath)
	if err != nil {
		return mem, nil
	}
	// if memory limit for a container is not set, the variable value is a bit less
	// than MaxInt64 (rounded down to kernel memory page size - 4k).
	// It is safe to assume that the value is greater than half of MaxInt64
	// https://unix.stackexchange.com/questions/420906/what-is-the-value-for-the-cgroups-limit-in-bytes-if-the-memory-is-not-restricte
	if memLimit > math.MaxInt64/2 {
		return mem, nil
	}

	// this one is approximate value that includes caches
	memUsed, err := cos.ReadOneUint64(contMemUsedPath)
	if err != nil {
		return mem, nil
	}
	mem.Total = memLimit
	mem.Used = memUsed
	mem.Free = mem.Total - mem.Used

	// calculate memory used for caches
	err = cos.ReadLines(contMemStatPath, func(line string) error {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil
		}
		if fields[0] == "total_cache" {
			val, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				memUsed -= val
			}
			return io.EOF
		}
		return nil
	})
	if err != nil {
		return mem, nil
	}

	mem.ActualUsed = memUsed
	mem.ActualFree = mem.Total - mem.ActualUsed
	return mem, nil
}
