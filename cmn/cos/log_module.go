// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
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
	ModTransport = 1 << iota
	ModAIS
	ModMemsys
	ModCore
	ModFS
	ModReb
	ModEC
	ModStats
	ModIOS
	ModXs
	ModBackend
	ModSpace
	ModMirror
	ModDsort
	ModDload
	ModETL
	ModS3
	ModKalive

	// NOTE: the last
	_smoduleLast
)

const maxLevel = 5

// NOTE: keep in-sync with the above; used by CLI
var Mods = [...]string{
	"transport", "ais", "memsys", "cluster", "fs", "reb", "ec", "stats",
	"ios", "xs", "backend", "space", "mirror", "dsort", "downloader", "etl",
	"s3",
	"kalive",
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
	for i, a := range Mods {
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
		n              int
		level, modules = l.Parse()
	)
	s = strconv.Itoa(level)
	if modules == 0 {
		return
	}

	var sb SB
	sb.Init(len(Mods) * 16)
	for i, sm := range Mods {
		if modules&(1<<i) != 0 {
			sb.WriteUint8(',')
			sb.WriteString(sm)
			n++
		}
	}

	debug.Assert(n > 0, fmt.Sprintf(ferl, string(l), level, modules))
	s += " (module" + Plural(n) + ": " + sb.String()[1:] + ")"
	return
}
