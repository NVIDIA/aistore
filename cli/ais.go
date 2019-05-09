// Package main is used as command-line interpreter for AIS
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/NVIDIA/aistore/cli/commands"
	"github.com/NVIDIA/aistore/cmn"
)

var (
	build   string
	version string
	url     string
)

func main() {
	commands.SetClusterURL(url)

	if !commands.IsAutoCompConfigured() {
		fmt.Printf("Auto complete script not installed in %q.\n", commands.AutoCompDir)
	}

	aisCLI := commands.New(build, version)

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt)

	// Handle exit
	go func() {
		<-stopCh
		os.Exit(0)
	}()

	if err := aisCLI.RunLong(os.Args); err != nil {
		cmn.ExitInfof("%s.", cmn.CapitalizeString(err.Error()))
	}
}
