// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ios

// based on:
// - https://www.kernel.org/doc/Documentation/iostats.txt
// - https://www.kernel.org/doc/Documentation/block/stat.txt
type blockStats struct {
	readComplete  int64 // 1 - # of reads completed
	readMerged    int64 // 2 - # of reads merged
	readSectors   int64 // 3 - # of sectors read
	readMs        int64 // 4 - # ms spent reading
	writeComplete int64 // 5 - # writes completed
	writeMerged   int64 // 6 - # writes merged
	writeSectors  int64 // 7 - # of sectors written
	writeMs       int64 // 8 - # of milliseconds spent writing
	ioPending     int64 // 9 - # of I/Os currently in progress
	ioMs          int64 // 10 - # of milliseconds spent doing I/Os
	ioMsWeighted  int64 // 11 - weighted # of milliseconds spent doing I/Os
	// 12 - 15: discard I/Os, discard merges, discard sectors, discard ticks
	// 16, 17:  flash I/Os, flash ticks, as per https://github.com/sysstat/sysstat/blob/master/iostat.c
}

type allBlockStats map[string]*blockStats
