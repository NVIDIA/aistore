// Package main is used as command-line interpreter for AIS
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"
	"os/signal"
	"sort"

	"github.com/NVIDIA/aistore/cli/commands"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	build   string
	version string
	url     string
)

func main() {
	if err := commands.SetNTestAISURL(url); err != nil {
		cmn.ExitInfof("Could not connect to AIS cluster: %s.", err)
	}

	if !commands.IsAutoCompConfigured() {
		fmt.Printf("Auto complete script not installed in %q.\n", commands.AutoCompDir)
	}

	aisCLI := commands.New(build, version)
	aisCLI.Commands = append(aisCLI.Commands, commands.DownloaderCmds...)
	aisCLI.Commands = append(aisCLI.Commands, commands.DSortCmds...)
	aisCLI.Commands = append(aisCLI.Commands, commands.ObjectCmds...)
	aisCLI.Commands = append(aisCLI.Commands, commands.BucketCmds...)
	aisCLI.Commands = append(aisCLI.Commands, commands.DaeCluCmds...)
	aisCLI.Commands = append(aisCLI.Commands, commands.XactCmds...)
	sort.Sort(cli.CommandsByName(aisCLI.Commands))

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt)

	// Handle exit
	go func() {
		<-stopCh
		os.Exit(0)
	}()

	if err := aisCLI.RunLong(os.Args); err != nil {
		cmn.ExitInfof("%s", err)
	}
}
