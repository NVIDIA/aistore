// Package main for the `ishard` executable.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cmd/ishard/ishard"
)

func main() {
	isharder, err := ishard.NewISharder(nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ishard initialization failed: %v\n", err)
		os.Exit(1)
	}

	if err := isharder.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "ishard execution failed: %v\n", err)
		os.Exit(1)
	}
}
