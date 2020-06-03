// Package main for the AIS node executable.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"os"

	"github.com/NVIDIA/aistore/ais"
)

// NOTE: these variables are set by ldflags
var (
	version string
	build   string
)

func main() {
	os.Exit(ais.Run(version, build))
}
