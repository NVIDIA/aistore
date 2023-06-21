// Package main for the AIS node executable.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"flag"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

var (
	cpuProfile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	memProfile = flag.String("memprofile", "", "write memory profile to `file`")
)

// NOTE: these variables are set by ldflags
var (
	build     string
	buildtime string
)

func main() {
	os.Exit(run())
}

func run() int {
	flag.Parse()

	if s := *cpuProfile; s != "" {
		*cpuProfile = s + "." + strconv.Itoa(syscall.Getpid())
		f, err := os.Create(*cpuProfile)
		if err != nil {
			cos.ExitLogf("Couldn't create CPU profile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			cos.ExitLogf("Couldn't start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	exitCode := ais.Run(cmn.VersionAIStore+"."+build, buildtime)
	glog.FlushExit()

	if s := *memProfile; s != "" {
		*memProfile = s + "." + strconv.Itoa(syscall.Getpid())
		f, err := os.Create(*memProfile)
		if err != nil {
			cos.ExitLogf("Couldn't create memory profile: %v", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			cos.ExitLogf("Couldn't write memory profile: %v", err)
		}
	}

	return exitCode
}
