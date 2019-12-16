// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import "github.com/NVIDIA/aistore/cmn"

// TODO: this struct is only there to make Darwin platform compile. Later,
//  this must become an interface that will be implemented for both Linux and
//  Darwin platform. Then the signature of `readDiskStats` needs to be changed
//  as well to accept an interface rather than a struct.
type dblockStat struct {
	ReadComplete  int64 // 1 - # of reads completed
	ReadMerged    int64 // 2 - # of reads merged
	ReadSectors   int64 // 3 - # of sectors read
	ReadMs        int64 // 4 - # ms spent reading
	WriteComplete int64 // 5 - # writes completed
	WriteMerged   int64 // 6 - # writes merged
	WriteSectors  int64 // 7 - # of sectors written
	WriteMs       int64 // 8 - # of milliseconds spent writing
	IOPending     int64 // 9 - # of I/Os currently in progress
	IOMs          int64 // 10 - # of milliseconds spent doing I/Os
	IOMsWeighted  int64 // 11 - weighted # of milliseconds spent doing I/Os
}

type diskBlockStats map[string]dblockStat

// readDiskStats returns disk stats
// TODO implementation
func readDiskStats(disks, sysfnames cmn.SimpleKVs, blockStats diskBlockStats) {}
