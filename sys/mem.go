// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Memory stats for the host OS or for container, depending on where the app is running.
// For the host, returns an error if memory cannot be read. If the function fails to read
// container's stats, it returns host memory. Swap stats, however, are _always_ host stats.

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

func (mem *MemStat) Get() error {
	if !containerized {
		return mem.host()
	}
	return mem.container()
}

func (mem *MemStat) Str(sb *cos.SB) {
	sb.WriteString("used ")
	sb.WriteString(cos.IEC(int64(mem.Used), 0))
	sb.WriteString(", ")
	sb.WriteString("free ")
	sb.WriteString(cos.IEC(int64(mem.Free), 0))
	sb.WriteString(", ")
	sb.WriteString("buffcache ")
	sb.WriteString(cos.IEC(int64(mem.BuffCache), 0))
	sb.WriteString(", ")
	sb.WriteString("actfree ")
	sb.WriteString(cos.IEC(int64(mem.ActualFree), 0))
}
