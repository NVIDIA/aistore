// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

// based on https://www.kernel.org/doc/Documentation/iostats.txt
//   and https://www.kernel.org/doc/Documentation/block/stat.txt
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

// readDiskStats returns disk stats FIXME: optimize
func readDiskStats(disks, sysfnames cmn.SimpleKVs, blockStats diskBlockStats) {
	for d := range disks {
		stat, ok := readSingleDiskStat(sysfnames[d])
		if !ok {
			continue
		}
		blockStats[d] = stat
	}
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
		glog.Fatalf("Failed to convert field value '%s' to int: %v \n",
			field, err)
	}
	return val
}
