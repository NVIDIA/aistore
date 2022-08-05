// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package sys

/*
#include <mach/mach_host.h>
*/
import "C" // nolint:gci,gocritic // super weird case

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
	"unsafe" // nolint:gci,gocritic // super weird case
)

type (
	swapStats struct {
		Total uint64
		Free  uint64
		Used  uint64
	}
)

func readSysctl(name string, data interface{}) (err error) {
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

func readVMStat(vmstat *C.vm_statistics_data_t) error {
	var count C.mach_msg_type_number_t = C.HOST_VM_INFO_COUNT
	status := C.host_statistics(
		C.mach_host_self(),
		C.HOST_VM_INFO,
		C.host_info_t(unsafe.Pointer(vmstat)),
		&count,
	)
	if status != C.KERN_SUCCESS {
		return fmt.Errorf("host_statistics=%v", status)
	}
	return nil
}

func (mem *MemStat) host() error {
	totalMem, err := readTotalMemory()
	if err != nil {
		return err
	}

	var vmstat C.vm_statistics_data_t
	if err := readVMStat(&vmstat); err != nil {
		return err
	}

	var (
		sstats   = swapStats{}
		pageSize = uint64(1 << 12)
		freeMem  = uint64(vmstat.free_count) * pageSize
		kern     = uint64(vmstat.inactive_count) * pageSize
	)
	if err := readSysctl("vm.swapusage", &sstats); err != nil {
		return err
	}
	{
		mem.Total = totalMem
		mem.Free = freeMem
		mem.Used = totalMem - freeMem
		mem.ActualFree = freeMem + kern
		mem.ActualUsed = totalMem - freeMem - kern
		mem.SwapTotal = sstats.Total
		mem.SwapFree = sstats.Free
		mem.SwapUsed = sstats.Used
	}
	return nil
}

func (*MemStat) container() error { return errors.New("Darwin: cannot get container memory stats") }
