// Package main for the AIS node executable.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"os"

	"github.com/NVIDIA/aistore/ais"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

var (
	build     string
	buildtime string
)

func main() {
	debug.Assert(build != "", "missing build")
	ecode := ais.Run(cmn.VersionAIStore+"."+build, buildtime)
	nlog.Flush(true)
	os.Exit(ecode)
}
