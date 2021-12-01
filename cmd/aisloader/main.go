// Package main for the `aisloader` executable.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/bench/aisloader"
)

var (
	version   = "1.3"
	build     string
	buildtime string
)

func main() {
	if err := aisloader.Start(version, build, buildtime); err != nil {
		fmt.Fprintf(os.Stderr, "aisloader exited with error: %v\n", err)
		os.Exit(1)
	}
}
