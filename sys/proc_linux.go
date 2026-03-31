// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
// with support for cgroup v2 and a moving-average CPU estimator.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// memory usage by specified PID
func procMem(pid int) (ProcMemStats, error) {
	mem := ProcMemStats{}

	procPath := fmt.Sprintf(hostProcessStatMemPath, pid)
	line, err := cos.ReadOneLine(procPath)
	if err != nil {
		return mem, err
	}

	fields := strings.Fields(line)
	val, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return mem, err
	}
	mem.Size = val << 12
	val, err = strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return mem, err
	}
	mem.Resident = val << 12
	val, err = strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return mem, err
	}
	mem.Share = val << 12

	return mem, err
}

// CPU usage by PID
func procCPU(pid int) (ProcCPUStats, error) {
	var cpu ProcCPUStats

	procPath := fmt.Sprintf(hostProcessStatCPUPath, pid)
	line, err := cos.ReadOneLine(procPath)
	if err != nil {
		return cpu, err
	}

	// TODO:
	// - generally, comm field in /proc/<pid>/stat is parenthesized and may contain spaces
	// - for AIStore the field will be "aisnode", so can wait
	fields := strings.Fields(line)
	if len(fields) < 22 {
		return cpu, fmt.Errorf("%s: unexpected format: num-fields=%d", procPath, len(fields))
	}

	user, err := strconv.ParseUint(fields[13], 10, 64) // utime
	if err != nil {
		return cpu, fmt.Errorf("%s: parse utime %q: %w", procPath, fields[13], err)
	}
	sys, err := strconv.ParseUint(fields[14], 10, 64) // stime
	if err != nil {
		return cpu, fmt.Errorf("%s: parse stime %q: %w", procPath, fields[14], err)
	}
	start, err := strconv.ParseUint(fields[21], 10, 64) // starttime since boot, in ticks
	if err != nil {
		return cpu, fmt.Errorf("%s: parse starttime %q: %w", procPath, fields[21], err)
	}

	// cumulative CPU time in milliseconds
	cpu.User = user * (1000 / userHZ)
	cpu.System = sys * (1000 / userHZ)
	cpu.Total = cpu.User + cpu.System

	// lifetime-average CPU percent:
	// - elapsed process lifetime = uptime since boot - process `starttime` since boot
	// - cpu percent = cumulative process cpu time / elapsed process lifetime
	upline, err := cos.ReadOneLine(proc + "uptime")
	if err != nil {
		return cpu, fmt.Errorf("%s: %w", proc+"uptime", err)
	}
	ufields := strings.Fields(upline)
	if len(ufields) < 1 {
		return cpu, fmt.Errorf("%s: unexpected format", proc+"uptime")
	}
	uptimeSec, err := strconv.ParseFloat(ufields[0], 64)
	if err != nil {
		return cpu, fmt.Errorf("%s: parse uptime %q: %w", proc+"uptime", ufields[0], err)
	}

	startSec := float64(start) / userHZ
	elapsedSec := uptimeSec - startSec

	if elapsedSec > 0 {
		pct := float64(user+sys) / userHZ * 100 / elapsedSec
		if gcpu.num > 1 {
			pct /= float64(gcpu.num)
		}
		cpu.Percent = pct
	}

	return cpu, nil
}

// FDSize represents currently allocated FD table size
func getFDSize() (int, error) {
	const (
		prefix = "FDSize:"
	)
	data, err := os.ReadFile(hostProcessInfo)
	if err != nil {
		return 0, err
	}
	ld := len(data)

	needle := cos.UnsafeB(prefix)
	if idx := bytes.Index(data, needle); idx >= 0 {
		off := idx + len(needle)
		if off >= ld {
			return 0, fmt.Errorf("no '%s' in %s: too short", prefix, hostProcessInfo)
		}
		b := data[off:]
		l := bytes.IndexByte(b, '\n')
		if l > 0 {
			b = b[:l]
		}
		sval := strings.TrimSpace(string(b))
		val, err := strconv.Atoi(sval)
		if err != nil {
			return 0, fmt.Errorf("failed to parse '%s': %v", prefix, err)
		}
		return val, nil
	}

	return 0, fmt.Errorf("failed to find '%s' in %s", prefix, hostProcessInfo)
}
