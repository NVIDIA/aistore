// Package main for the `ishard` executable.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/tools/ishard"
)

func main() {
	ishard.Init(nil)
	if err := ishard.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "ishard exited with error: %v\n", err)
		os.Exit(1)
	}
}
