// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
// with support for cgroup v2 and a moving-average CPU estimator.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const errPrefixMem = "sys/mem"

type MemStat struct {
	Total      uint64 `msg:"t"`
	Used       uint64 `msg:"u"`
	Free       uint64 `msg:"f"`
	BuffCache  uint64 `msg:"b"`
	ActualFree uint64 `msg:"af"`
	ActualUsed uint64 `msg:"au"`
	SwapTotal  uint64 `msg:"st"`
	SwapFree   uint64 `msg:"sf"`
	SwapUsed   uint64 `msg:"su"`
}

func (mem *MemStat) Get() error {
	var err error
	switch cgroupVer {
	case 2:
		debug.Assert(contMemV2Max != "") // must be resolved via initCgroupV2Paths()
		*mem, err = readMemCgroupV2()
	case 1:
		*mem, err = readMemCgroupV1()
	default:
		*mem, err = readMemHost()
	}
	if err != nil {
		return fmt.Errorf("%s: %w", errPrefixMem, err)
	}
	return nil
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
