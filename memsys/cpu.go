// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"os"
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	sigar "github.com/cloudfoundry/gosigar"
)

// This file provides additional functions to monitor system statistics, such as cpu and disk usage, using the memsys library

func (r *Mem2) FetchSysInfo() cmn.SysInfo {
	sysInfo := cmn.SysInfo{}

	concSigar := sigar.ConcreteSigar{}
	concMem, _ := concSigar.GetMem()

	sysInfo.MemAvail = concMem.Total

	mem := sigar.ProcMem{}
	mem.Get(os.Getpid())
	sysInfo.MemUsed = mem.Resident
	sysInfo.PctMemUsed = float64(sysInfo.MemUsed) * 100 / float64(sysInfo.MemAvail)

	cpu := sigar.ProcCpu{}
	cpu.Get(os.Getpid())
	sysInfo.PctCPUUsed = cpu.Percent

	return sysInfo
}

func (r *Mem2) FetchFSInfo() cmn.FSInfo {
	fsInfo := cmn.FSInfo{}

	availableMountpaths, _ := fs.Mountpaths.Get()

	vistedFS := make(map[syscall.Fsid]bool)

	for mpath := range availableMountpaths {
		statfs := &syscall.Statfs_t{}

		if err := syscall.Statfs(mpath, statfs); err != nil {
			glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
			continue
		}

		if _, ok := vistedFS[statfs.Fsid]; ok {
			continue
		}

		vistedFS[statfs.Fsid] = true

		fsInfo.FSUsed += (statfs.Blocks - statfs.Bavail) * uint64(statfs.Bsize)
		fsInfo.FSCapacity += statfs.Blocks * uint64(statfs.Bsize)
	}

	if fsInfo.FSCapacity > 0 {
		//FIXME: assuming that each mountpath has the same capacity and gets distributed the same files
		fsInfo.PctFSUsed = float64(fsInfo.FSUsed*100) / float64(fsInfo.FSCapacity)
	}

	return fsInfo
}
