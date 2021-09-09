// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/lufia/iostat"
)

type dblockStat struct {
	readBytes  int64
	readMs     int64
	writeBytes int64
	writeMs    int64
	ioMs       int64
}

// interface guard
var (
	_ diskBlockStat = (*dblockStat)(nil)
)

// readDiskStats returns disk stats
func readDiskStats(disks, _ cos.SimpleKVs) diskBlockStats {
	driveStats, err := iostat.ReadDriveStats()
	if err != nil {
		return diskBlockStats{}
	}

	dblockStats := make(diskBlockStats, len(disks))
	for _, driveStat := range driveStats {
		dblockStats[driveStat.Name] = dblockStat{
			readBytes:  driveStat.BytesRead,
			readMs:     driveStat.TotalReadTime.Milliseconds(),
			writeBytes: driveStat.BytesWritten,
			writeMs:    driveStat.TotalWriteTime.Milliseconds(),
			ioMs:       driveStat.TotalReadTime.Milliseconds() + driveStat.TotalWriteTime.Milliseconds(),
		}
	}
	return dblockStats
}

func (dblockStat) Reads() int64          { return 0 } // TODO: not implemented
func (dbs dblockStat) ReadBytes() int64  { return dbs.readBytes }
func (dblockStat) Writes() int64         { return 0 } // TODO: not implemented
func (dbs dblockStat) WriteBytes() int64 { return dbs.writeBytes }
func (dbs dblockStat) IOMs() int64       { return dbs.ioMs }
func (dbs dblockStat) WriteMs() int64    { return dbs.writeMs }
func (dbs dblockStat) ReadMs() int64     { return dbs.readMs }
