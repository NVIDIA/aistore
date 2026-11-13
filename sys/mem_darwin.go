// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

/*
#include <mach/mach.h>
*/
import "C" //nolint:gci,gocritic // super weird case

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"syscall"
	"unsafe" //nolint:gocritic // super weird case

	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	swapStats struct {
		Total uint64
		Free  uint64
		Used  uint64
	}
)

func readSysctl(name string, data any) (err error) {
	value, err := syscall.Sysctl(name)
	if err != nil {
		return err
	}

	buf := []byte(value)

	switch v := data.(type) {
	case *uint64:
		*v = *(*uint64)(unsafe.Pointer(&buf[0]))
		return
	default:
		b := bytes.NewBuffer(buf)
		return binary.Read(b, binary.LittleEndian, data)
	}
}

func readTotalMemory() (uint64, error) {
	var totalMem uint64
	if err := readSysctl("hw.memsize", &totalMem); err != nil {
		return 0, err
	}
	return totalMem, nil
}

func readVMStat(host C.host_t, vmstat *C.vm_statistics64_data_t) error {
	var count C.mach_msg_type_number_t = C.HOST_VM_INFO64_COUNT
	status := C.host_statistics64(
		host,
		C.HOST_VM_INFO64,
		C.host_info64_t(unsafe.Pointer(vmstat)),
		&count,
	)
	if status != C.KERN_SUCCESS {
		return fmt.Errorf("host_statistics64=%v", status)
	}
	return nil
}

func readPageSize(host C.host_t) (uint64, error) {
	var pageSize C.vm_size_t
	if status := C.host_page_size(host, &pageSize); status != C.KERN_SUCCESS {
		return 0, fmt.Errorf("host_page_size=%v", status)
	}
	return uint64(pageSize), nil
}

func readMemHost() (mem MemStat, err error) {
	totalMem, err := readTotalMemory()
	if err != nil {
		return mem, err
	}

	// mach_host_self() returns a send right that must be released
	host := C.mach_host_self()
	defer C.mach_port_deallocate(C.mach_task_self_, host)

	var vmstat C.vm_statistics64_data_t
	if err := readVMStat(host, &vmstat); err != nil {
		return mem, err
	}
	pageSize, err := readPageSize(host)
	if err != nil {
		return mem, err
	}

	// BuffCache: file-backed pages minus speculative ones (already counted in free_count)
	var (
		sstats      = swapStats{}
		freeMem     = uint64(vmstat.free_count) * pageSize
		external    = uint64(vmstat.external_page_count)
		speculative = uint64(vmstat.speculative_count)
		buffCache   = cos.Ternary(external > speculative, external-speculative, 0) * pageSize
	)
	if err := readSysctl("vm.swapusage", &sstats); err != nil {
		return mem, err
	}

	mem.Total = totalMem
	mem.Free = freeMem
	mem.Used = totalMem - freeMem
	mem.BuffCache = buffCache
	mem.ActualFree = min(freeMem+buffCache, totalMem)
	mem.ActualUsed = totalMem - mem.ActualFree
	mem.SwapTotal = sstats.Total
	mem.SwapFree = sstats.Free
	mem.SwapUsed = sstats.Used
	return mem, nil
}

func readMemCgroupV2() (MemStat, error) { return MemStat{}, errDarwin }
func readMemCgroupV1() (MemStat, error) { return MemStat{}, errDarwin }
