// Package cli is used as command-line interpreter for AIS
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/NVIDIA/aistore/cmd/cli/cli"
	"github.com/NVIDIA/aistore/cmn"
)

var (
	build     string
	buildtime string
)

func dispatchInterruptHandler() {
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt)
	go func() {
		<-stopCh
		os.Exit(0)
	}()
}

func main() {
	dispatchInterruptHandler()

	if err := cli.Init(os.Args); err != nil {
		exitf("%v", err)
	}

	if err := cli.Run(cmn.VersionCLI+"."+build, buildtime, os.Args); err != nil {
		exitf("%v", err)
	}
}

func exitf(f string, a ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(1)
}
