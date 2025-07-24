// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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

const ticks = 100 // C.sysconf(C._SC_CLK_TCK)

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

func procCPU(pid int) (ProcCPUStats, error) {
	cpu := ProcCPUStats{}

	procPath := fmt.Sprintf(hostProcessStatCPUPath, pid)
	line, err := cos.ReadOneLine(procPath)
	if err != nil {
		return cpu, err
	}

	fields := strings.Fields(line)
	user, err := strconv.ParseUint(fields[13], 10, 64)
	if err != nil {
		return cpu, err
	}
	sys, err := strconv.ParseUint(fields[14], 10, 64)
	if err != nil {
		return cpu, err
	}

	// convert to milliseconds
	cpu.User = user * (1000 / ticks)
	cpu.System = sys * (1000 / ticks)
	cpu.Total = cpu.User + cpu.System

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
