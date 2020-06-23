// Package cli is used as command-line interpreter for AIS
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/NVIDIA/aistore/cmd/cli/commands"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	version = "0.4"
)

var (
	build string
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

	err := commands.Init()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}

	aisCLI := commands.New(build, version)
	if err := aisCLI.Run(os.Args); err != nil {
		cmn.Exitf("%s", err.Error())
	}
}
