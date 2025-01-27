// Package main for the `aisloader` executable.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/bench/tools/aisloader"
	"github.com/NVIDIA/aistore/cmn"
)

var (
	build     string
	buildtime string
)

func main() {
	if err := aisloader.Start(cmn.VersionLoader+"."+build, buildtime); err != nil {
		fmt.Fprintf(os.Stderr, "aisloader exited with error: %v\n", err)
		os.Exit(1)
	}
}
