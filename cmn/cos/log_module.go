// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// see related: duration.go, size.go

const ferl = "invalid log.level %q (%d, %08b)"

const (
	SmoduleTransport = 1 << iota
	SmoduleAIS
	SmoduleMemsys
	SmoduleCluster
	SmoduleFS
	SmoduleReb
	SmoduleEC
	SmoduleStats
	SmoduleIOS
	SmoduleXs
	SmoduleBackend
	SmoduleSpace
	SmoduleMirror
	SmoduleDsort
	SmoduleDload
	SmoduleETL
	SmoduleS3

	// NOTE: the last
	_smoduleLast
)

const maxLevel = 5

// NOTE: keep in-sync with the above
var Smodules = []string{
	"transport", "ais", "memsys", "cluster", "fs", "reb", "ec", "stats",
	"ios", "xs", "backend", "space", "mirror", "dsort", "downloader", "etl",
	"s3",
}

type LogLevel string

func (l LogLevel) Parse() (level, modules int) {
	value, err := strconv.Atoi(string(l))
	debug.AssertNoErr(err)
	level, modules = value&0x7, value>>3
	return
}

func (l *LogLevel) Set(level int, sm []string) {
	var modules int
	for i, a := range Smodules {
		for _, b := range sm {
			if a == b {
				modules |= 1 << i
			}
		}
	}
	*l = LogLevel(strconv.Itoa(level + modules<<3))
}

func (l LogLevel) Validate() (err error) {
	level, modules := l.Parse()
	if level == 0 || level > maxLevel || modules > _smoduleLast {
		err = fmt.Errorf(ferl, string(l), level, modules)
	}
	return
}

func (l LogLevel) String() (s string) {
	var (
		ms             string
		n              int
		level, modules = l.Parse()
	)
	s = strconv.Itoa(level)
	if modules == 0 {
		return
	}
	for i, sm := range Smodules {
		if modules&(1<<i) != 0 {
			ms += "," + sm
			n++
		}
	}
	debug.Assert(n > 0, fmt.Sprintf(ferl, string(l), level, modules))
	s += " (module" + Plural(n) + ": " + ms[1:] + ")"
	return
}
