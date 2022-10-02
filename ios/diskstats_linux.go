// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// The "sectors" in question are the standard UNIX 512-byte sectors, not any device- or filesystem-specific block size
// (from https://www.kernel.org/doc/Documentation/block/stat.txt)
const sectorSize = int64(512)

// based on https://www.kernel.org/doc/Documentation/iostats.txt
//
//	and https://www.kernel.org/doc/Documentation/block/stat.txt
type dblockStat struct {
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
}

// interface guard
var (
	_ diskBlockStat = (*dblockStat)(nil)
)

var blockStats = make(diskBlockStats, 10)

// readDiskStats returns disk stats
func readDiskStats(disks, sysfnames cos.StrKVs) diskBlockStats {
	for d := range disks {
		stat, ok := readSingleDiskStat(sysfnames[d])
		if !ok {
			continue
		}
		blockStats[d] = stat
	}
	return blockStats
}

// https://www.kernel.org/doc/Documentation/block/stat.txt
func readSingleDiskStat(sysfn string) (dblockStat, bool) {
	file, err := os.Open(sysfn)
	if err != nil {
		glog.Errorf("%s: %v", sysfn, err)
		return dblockStat{}, false
	}
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	fields := strings.Fields(scanner.Text())

	_ = file.Close()
	if len(fields) < 11 {
		return dblockStat{}, false
	}
	return extractDiskStat(fields, 0), true
}

func extractDiskStat(fields []string, offset int) dblockStat {
	return dblockStat{
		extractI64(fields[offset]),
		extractI64(fields[offset+1]),
		extractI64(fields[offset+2]),
		extractI64(fields[offset+3]),
		extractI64(fields[offset+4]),
		extractI64(fields[offset+5]),
		extractI64(fields[offset+6]),
		extractI64(fields[offset+7]),
		extractI64(fields[offset+8]),
		extractI64(fields[offset+9]),
		extractI64(fields[offset+10]),
	}
}

func extractI64(field string) int64 {
	val, err := strconv.ParseInt(field, 10, 64)
	if err != nil {
		cos.Assertf(false, "failed to parse %q field to integer", field)
	}
	return val
}

func (dbs dblockStat) Reads() int64      { return dbs.readComplete }
func (dbs dblockStat) ReadBytes() int64  { return dbs.readSectors * sectorSize }
func (dbs dblockStat) Writes() int64     { return dbs.writeComplete }
func (dbs dblockStat) WriteBytes() int64 { return dbs.writeSectors * sectorSize }
func (dbs dblockStat) IOMs() int64       { return dbs.ioMs }
func (dbs dblockStat) WriteMs() int64    { return dbs.writeMs }
func (dbs dblockStat) ReadMs() int64     { return dbs.readMs }
