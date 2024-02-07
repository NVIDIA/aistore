// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"math"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

var mem0 = MemStat{ActualFree: math.MaxUint64}

func (mem *MemStat) setValue(name, valStr string) error {
	val, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		debug.AssertNoErr(err)
		return err
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
		mem.BuffCache += val
	case "SwapTotal":
		mem.SwapTotal = val
	case "SwapFree":
		mem.SwapFree = val
	}
	return nil
}

// parse `/proc/meminfo` lines
func (mem *MemStat) parse(line string) error {
	fields := strings.Split(line, ":")
	if len(fields) != 2 {
		return nil
	}
	name := fields[0]
	valStr := strings.Fields(strings.TrimLeft(fields[1], " "))[0]
	return mem.setValue(name, valStr)
}

func (mem *MemStat) host() (err error) {
	*mem = mem0 // ActualFree = MaxUint64, zeros all the rest
	if err = cos.ReadLines(hostMemPath, mem.parse); err != nil {
		return
	}

	if mem.ActualFree == math.MaxUint64 { // remains unassigned (see above)
		mem.ActualFree = mem.Free + mem.BuffCache
	}
	mem.Used = mem.Total - mem.Free
	mem.ActualUsed = mem.Total - mem.ActualFree
	mem.SwapUsed = mem.SwapTotal - mem.SwapFree
	return
}

// parse `/sys/fs/cgroup/memory/memory.stat` lines
func (mem *MemStat) cgroupParse(line string) error {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return nil
	}
	if fields[0] != "total_cache" {
		return nil
	}
	val, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return err
	}
	mem.BuffCache += val
	return nil
}

// Returns host stats if memory is not limited via cgroups "memory.limit_in_bytes".
func (mem *MemStat) container() error {
	if err := mem.host(); err != nil {
		return err
	}
	memLimit, err := cos.ReadOneUint64(contMemLimitPath)
	if err != nil {
		return nil
	}
	// It is safe to assume that the value greater than MaxInt64/2 indicates "no limit"
	// https://unix.stackexchange.com/questions/420906/what-is-the-value-for-the-cgroups-limit-in-bytes-if-the-memory-is-not-restricte
	if memLimit > math.MaxInt64/2 {
		return nil
	}

	// this one is an approximate value that includes buff/cache
	// (ie., kernel buffers and page caches that can be reclaimed)
	memUsed, err := cos.ReadOneUint64(contMemUsedPath)
	if err != nil {
		return nil
	}
	mem.Total = memLimit
	mem.Used = memUsed
	mem.Free = mem.Total - mem.Used

	// calculate memory used for buffcache
	err = cos.ReadLines(contMemStatPath, mem.cgroupParse)
	if err != nil {
		debug.AssertNoErr(err)
		// NOTE: returning host memory
		return nil
	}

	mem.ActualUsed = memUsed - mem.BuffCache
	mem.ActualFree = mem.Total - mem.ActualUsed
	return nil
}
