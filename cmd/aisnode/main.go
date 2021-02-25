// Package main for the AIS node executable.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"os"

	"github.com/NVIDIA/aistore/ais"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// NOTE: (major.minor) version is updated manually at (each) release time;
//       the other two variables are populated by the `make` (via ldflags)

const version = "3.4"

var (
	build     string
	buildtime string
)

func main() {
	debug.AssertMsg(build != "", "missing build")
	debug.AssertMsg(buildtime != "", "missing build time")
	os.Exit(ais.Run(version+"."+build, buildtime))
}
