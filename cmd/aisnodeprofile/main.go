// Package main for the AIS node executable.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
)

var cpuProfile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memProfile = flag.String("memprofile", "", "write memory profile to `file`")

// NOTE: these variables are set by ldflags
var (
	version string
	build   string
)

func main() {
	os.Exit(run())
}

func run() int {
	if s := *cpuProfile; s != "" {
		*cpuProfile = s + "." + strconv.Itoa(syscall.Getpid())
		f, err := os.Create(*cpuProfile)
		if err != nil {
			glog.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			glog.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	exitCode := ais.Run(version, build)

	if s := *memProfile; s != "" {
		*memProfile = s + "." + strconv.Itoa(syscall.Getpid())
		f, err := os.Create(*memProfile)
		if err != nil {
			glog.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			glog.Fatal("could not write memory profile: ", err)
		}
	}

	return exitCode
}
