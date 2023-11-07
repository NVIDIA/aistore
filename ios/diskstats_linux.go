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

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
	// 12 - 15: discard I/Os, discard merges, discard sectors, discard ticks
	// 16, 17:  flash I/Os, flash ticks, as per https://github.com/sysstat/sysstat/blob/master/iostat.c
}

// interface guard
var (
	_ diskBlockStat = (*dblockStat)(nil)
)

var blockStats = make(diskBlockStats, 10)

// readStats returns disk stats
func readStats(disks, sysfnames cos.StrKVs) diskBlockStats {
	for d := range disks {
		stat, ok := _read(sysfnames[d])
		if !ok {
			continue
		}
		blockStats[d] = stat
	}
	return blockStats
}

// https://www.kernel.org/doc/Documentation/block/stat.txt
func _read(sysfn string) (dblockStat, bool) {
	file, err := os.Open(sysfn)
	if err != nil {
		nlog.Errorf("%s: %v", sysfn, err)
		return dblockStat{}, false
	}
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	fields := strings.Fields(scanner.Text())

	_ = file.Close()
	if len(fields) < 11 {
		return dblockStat{}, false
	}
	return _extact(fields, 0), true
}

func _extact(fields []string, offset int) dblockStat {
	return dblockStat{
		_exI64(fields[offset]),
		_exI64(fields[offset+1]),
		_exI64(fields[offset+2]),
		_exI64(fields[offset+3]),
		_exI64(fields[offset+4]),
		_exI64(fields[offset+5]),
		_exI64(fields[offset+6]),
		_exI64(fields[offset+7]),
		_exI64(fields[offset+8]),
		_exI64(fields[offset+9]),
		_exI64(fields[offset+10]),
	}
}

func _exI64(field string) int64 {
	val, err := strconv.ParseInt(field, 10, 64)
	debug.AssertNoErr(err)
	return val
}

func (dbs dblockStat) Reads() int64      { return dbs.readComplete }
func (dbs dblockStat) ReadBytes() int64  { return dbs.readSectors * sectorSize }
func (dbs dblockStat) Writes() int64     { return dbs.writeComplete }
func (dbs dblockStat) WriteBytes() int64 { return dbs.writeSectors * sectorSize }
func (dbs dblockStat) IOMs() int64       { return dbs.ioMs }
func (dbs dblockStat) WriteMs() int64    { return dbs.writeMs }
func (dbs dblockStat) ReadMs() int64     { return dbs.readMs }

// NVMe multipathing
// * nvmeInN:     instance I namespace N
// * nvmeIcCnN:   instance I controller C namespace N

// instance-controller-namespace (icn):
// given "nvmeInN" return the corresponding multipath "nvmeIcCnN" (same instance, same namespace)
func icn(disk, dir string) (cdisk string) {
	if !strings.HasPrefix(disk, "nvme") {
		return
	}
	a := regex.FindStringSubmatch(disk)
	if len(a) < 3 {
		return
	}
	dentries, err := os.ReadDir(dir)
	if err != nil {
		debug.Assert(err == nil, dir, err)
		return
	}
	for _, d := range dentries {
		name := d.Name()
		if !strings.HasPrefix(name, "nvme") {
			continue
		}
		b := cregex.FindStringSubmatch(name)
		if len(b) < 4 {
			continue
		}
		if a[1] == b[1] && a[2] == b[3] {
			cdisk = name
			break
		}
	}
	return
}

func icnPath(dir, cdir, mountpath string) bool {
	stats, ok := _read(dir)
	cstats, cok := _read(cdir)
	if !cok {
		return false
	}
	if !ok {
		return true
	}
	// first, an easy check
	if stats.readComplete == 0 && stats.writeComplete == 0 && (cstats.readComplete > 0 || cstats.writeComplete > 0) {
		return true
	}

	// write at the root of the mountpath and check whether (the alternative) stats incremented
	fh, err := os.CreateTemp(mountpath, "")
	if err != nil {
		debug.Assert(err == nil, mountpath, err)
		return false
	}
	fqn := fh.Name()
	fh.Close()
	err = os.Remove(fqn)
	debug.AssertNoErr(err)

	stats2, ok := _read(dir)
	cstats2, cok := _read(cdir)
	debug.Assert(ok && cok)
	return stats.writeComplete == stats2.writeComplete && cstats.writeComplete < cstats2.writeComplete
}
