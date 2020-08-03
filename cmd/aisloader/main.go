// Package main for the `aisloader` executable.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/bench/aisloader"
)

func main() {
	if err := aisloader.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "aisloader exited with error: %s", err.Error())
		os.Exit(1)
	}
}
