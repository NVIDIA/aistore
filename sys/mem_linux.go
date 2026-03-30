// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

//
// bare metal: /proc/meminfo
//

func (mem *MemStat) parse(line string) error {
	name, rest, ok := strings.Cut(line, ":")
	if !ok {
		return nil
	}

	rest = strings.TrimLeft(rest, " ")

	var n int
	for n < len(rest) && rest[n] >= '0' && rest[n] <= '9' {
		n++
	}
	if n == 0 {
		return nil
	}

	val, err := strconv.ParseUint(rest[:n], 10, 64)
	if err != nil {
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

func readMemHost() (mem MemStat, err error) {
	mem.ActualFree = math.MaxUint64

	if err = cos.ReadLines(hostMemPath, mem.parse); err != nil {
		return mem, err
	}

	// if "MemAvailable" was not present (older kernels)
	if mem.ActualFree == math.MaxUint64 {
		mem.ActualFree = mem.Free + mem.BuffCache
	}

	// NOTE: unsigned arithmetic (trusting kernels)
	mem.Used = mem.Total - mem.Free
	mem.ActualUsed = mem.Total - mem.ActualFree
	mem.SwapUsed = mem.SwapTotal - mem.SwapFree
	return mem, nil
}

// readMemCgroupV2 returns container-scoped memory stats for cgroup v2:
//
// - `memory.max` and `memory.current` define the accounting scope and are
//   treated as required. If either cannot be read, the function returns an
//   error rather than silently falling back to host memory.
// - This is intentional: once init-time detection selected cgroup v2,
//   returning host memory here could greatly overstate available memory and
//   mask container-local memory pressure / OOM risk.
// - If memory.max == "max", the container has no explicit memory limit and
//   host memory becomes the effective scope, so we fall back to readMemHost().
// - `memory.current` includes reclaimable page cache and kernel buffers;
//   therefore `Used` may overstate pressure for GC decisions.
// - `memory.stat` is treated as best-effort. When available, `inactive_file`
//   is used as reclaimable cache (`BuffCache`) to derive `ActualUsed` and `ActualFree`.
// - If `memory.stat` is unavailable, we still return the already-read
//   container-scoped totals and conservatively set:
//       ActualUsed = Used
//       ActualFree = Free
// - `Used` is capped at `Total` to guard against transient overshoot near OOM.

func readMemCgroupV2() (mem MemStat, _ error) {
	// 1. memory limit
	line, err := cos.ReadOneLine(contMemV2Max)
	if err != nil {
		return mem, err
	}
	line = strings.TrimSpace(line)
	if line == "max" {
		// no memory limit: fall back to host
		return readMemHost()
	}
	memLimit, err := strconv.ParseUint(line, 10, 64)
	if err != nil {
		return mem, err
	}

	// 2. current usage (includes kernel caches)
	memUsed, err := cos.ReadOneUint64(contMemV2Current)
	if err != nil {
		return mem, err
	}

	mem.Total = memLimit
	mem.Used = min(memUsed, memLimit)
	mem.Free = memLimit - mem.Used

	// 3. reclaimable cache from memory.stat (inactive_file)
	data, err := os.ReadFile(contMemV2Stat)
	if err != nil {
		mem.ActualUsed = mem.Used
		mem.ActualFree = mem.Free
		return mem, nil
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		key, val, ok := strings.Cut(line, " ")
		if ok && key == "inactive_file" {
			if v, e := strconv.ParseUint(val, 10, 64); e == nil {
				mem.BuffCache = v
			}
			break
		}
	}
	mem.ActualUsed = cos.Ternary(mem.BuffCache < mem.Used, mem.Used-mem.BuffCache, 0)
	mem.ActualFree = mem.Total - mem.ActualUsed // NOTE: unsigned arithmetic (trusting kernel)
	return mem, nil
}

//
// cgroup v1 (obsolete; to be removed)
//

func readMemCgroupV1() (MemStat, error) {
	// start with host stats as baseline (swap, etc.)
	mem, err := readMemHost()
	if err != nil {
		return mem, err
	}

	memLimit, err := cos.ReadOneUint64(contMemLimitPath)
	if err != nil {
		return mem, nil // no limit file: keep host stats
	}
	// value > MaxInt64/2 indicates "no limit"
	// ref: https://unix.stackexchange.com/questions/420906
	if memLimit > math.MaxInt64/2 {
		return mem, nil
	}

	memUsed, err := cos.ReadOneUint64(contMemUsedPath)
	if err != nil {
		return mem, nil
	}

	mem.Total = memLimit
	mem.Used = memUsed
	mem.Free = memLimit - memUsed

	// reclaimable cache from memory.stat (total_cache)
	mem.BuffCache = 0
	err = cos.ReadLines(contMemStatPath, func(line string) error {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "total_cache" {
			if v, e := strconv.ParseUint(fields[1], 10, 64); e == nil {
				mem.BuffCache += v
			}
		}
		return nil
	})
	if err != nil {
		return mem, nil // keep what we have
	}

	mem.ActualUsed = memUsed - mem.BuffCache
	mem.ActualFree = memLimit - mem.ActualUsed
	return mem, nil
}
