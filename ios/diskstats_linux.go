// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Based on:
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

// The "sectors" in question are the standard UNIX 512-byte sectors, not any device- or filesystem-specific block size
// (from https://www.kernel.org/doc/Documentation/block/stat.txt)
const sectorSize = int64(512)

var (
	regex  = regexp.MustCompile(`nvme(\d+)n(\d+)`)
	cregex = regexp.MustCompile(`nvme(\d+)c(\d+)n(\d+)`)
)

func readStats(disks, sysfnames cos.StrKVs, all allBlockStats) {
	for disk := range disks {
		ds, ok := all[disk]
		debug.Assert(ok, disk)
		_ = _read(sysfnames[disk], ds)
	}
}

// https://www.kernel.org/doc/Documentation/block/stat.txt
func _read(sysfn string, ds *blockStats) bool {
	file, err := os.Open(sysfn)
	if err != nil {
		return false
	}
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	fields := strings.Fields(scanner.Text())

	_ = file.Close()
	if len(fields) < 11 {
		return false
	}
	*ds = blockStats{
		_exI64(fields[0]),
		_exI64(fields[1]),
		_exI64(fields[2]),
		_exI64(fields[3]),
		_exI64(fields[4]),
		_exI64(fields[5]),
		_exI64(fields[6]),
		_exI64(fields[7]),
		_exI64(fields[8]),
		_exI64(fields[9]),
		_exI64(fields[10]),
	}
	return true
}

func _exI64(field string) int64 {
	val, err := strconv.ParseInt(field, 10, 64)
	debug.AssertNoErr(err)
	return val
}

func (ds *blockStats) Reads() int64      { return ds.readComplete }
func (ds *blockStats) ReadBytes() int64  { return ds.readSectors * sectorSize }
func (ds *blockStats) Writes() int64     { return ds.writeComplete }
func (ds *blockStats) WriteBytes() int64 { return ds.writeSectors * sectorSize }
func (ds *blockStats) IOMs() int64       { return ds.ioMs }
func (ds *blockStats) WriteMs() int64    { return ds.writeMs }
func (ds *blockStats) ReadMs() int64     { return ds.readMs }

// NVMe multipathing
// * nvmeInN:     instance I namespace N
// * nvmeIcCnN:   instance I controller C namespace N

// instance-controller-namespace (icn):
// given "nvmeInN" return the corresponding multipath "nvmeIcCnN" (same instance, same namespace)
func icn(disk, dir string) (cdisk string, err error) {
	if !strings.HasPrefix(disk, "nvme") {
		return
	}
	a := regex.FindStringSubmatch(disk)
	if len(a) < 3 {
		return
	}
	dentries, errN := os.ReadDir(dir)
	if errN != nil {
		return "", fmt.Errorf("%q does not parse as NVMe icn", dir)
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
	var (
		stats, cstats blockStats
	)
	ok := _read(dir, &stats)
	cok := _read(cdir, &cstats)
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

	var (
		stats2, cstats2 blockStats
	)
	ok = _read(dir, &stats2)
	cok = _read(cdir, &cstats2)
	debug.Assert(ok && cok)
	return stats.writeComplete == stats2.writeComplete && cstats.writeComplete < cstats2.writeComplete
}
