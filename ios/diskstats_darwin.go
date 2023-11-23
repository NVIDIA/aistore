// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/lufia/iostat"
)

func readStats(disks, _ cos.StrKVs, all allBlockStats) {
	driveStats, err := iostat.ReadDriveStats()
	if err != nil {
		return
	}
	for _, stats := range driveStats {
		ds, ok := all[stats.Name]
		if !ok {
			all[stats.Name] = &blockStats{}
			ds = all[stats.Name]
		}
		*ds = blockStats{
			readSectors:  stats.BytesRead / 512, // HACK
			readMs:       stats.TotalReadTime.Milliseconds(),
			writeSectors: stats.BytesWritten / 512, // ditto
			writeMs:      stats.TotalWriteTime.Milliseconds(),
			ioMs:         stats.TotalReadTime.Milliseconds() + stats.TotalWriteTime.Milliseconds(),
		}
	}
}

func (*blockStats) Reads() int64         { return 0 } // TODO: not implemented
func (ds *blockStats) ReadBytes() int64  { return ds.readSectors * 512 }
func (*blockStats) Writes() int64        { return 0 } // TODO: not implemented
func (ds *blockStats) WriteBytes() int64 { return ds.writeSectors * 512 }
func (ds *blockStats) IOMs() int64       { return ds.ioMs }
func (ds *blockStats) WriteMs() int64    { return ds.writeMs }
func (ds *blockStats) ReadMs() int64     { return ds.readMs }

// NVMe multipathing - Linux only
// * nvmeInN:     instance I namespace N
// * nvmeIcCnN:   instance I controller C namespace N

func icn(string, string) string           { return "" }
func icnPath(string, string, string) bool { return false }
