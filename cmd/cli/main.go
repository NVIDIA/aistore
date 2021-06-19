// Package cli is used as command-line interpreter for AIS
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"os"
	"os/signal"

	"github.com/NVIDIA/aistore/cmd/cli/commands"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	version = "0.6"
)

var build string

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

	if err := commands.Init(); err != nil {
		cos.Exitf("%v", err)
	}

	aisCLI := commands.New(build, version)
	if err := aisCLI.Run(os.Args); err != nil {
		cos.Exitf("%v", err)
	}
}
