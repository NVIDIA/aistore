//This file is used as command-line interpreter for AIS
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
	"github.com/urfave/cli"
)

func main() {
	if err := commands.TestAISURL(commands.ClusterURL); err != nil {
		fmt.Printf("Could not connect to AIS cluster: %v\n", err)
		os.Exit(1)
	}

	aisCLI := commands.New()
	aisCLI.Commands = append(aisCLI.Commands, commands.DownloaderCmds...)
	aisCLI.Commands = append(aisCLI.Commands, commands.ObjectCmds...)
	aisCLI.Commands = append(aisCLI.Commands, commands.BucketCmds...)
	aisCLI.Commands = append(aisCLI.Commands, commands.DaeCluCmds...)
	sort.Sort(cli.CommandsByName(aisCLI.Commands))

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt)

	// Handle exit
	go func() {
		<-stopCh
		os.Exit(0)
	}()

	if err := aisCLI.RunLong(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

}
