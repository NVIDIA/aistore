// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// The "sectors" in question are the standard UNIX 512-byte sectors, not any device- or filesystem-specific block size
// (from https://www.kernel.org/doc/Documentation/block/stat.txt)
const sectorSize = int64(512)

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
