//This file is used as command-line interpreter for AIS
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/NVIDIA/aistore/aiscli/commands"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

func main() {
	if err := commands.TestAISURL(commands.ClusterURL); err != nil {
		fmt.Printf("Could not connect to AIS cluster: %v\n", err)
		os.Exit(1)
	}

	aisCLI := commands.New()

	aisCLI.Commands = []cli.Command{
		// AIS API Query Commands
		{
			Name:         cmn.GetWhatSmap,
			Usage:        "returns cluster map (Smap)",
			Action:       commands.GetQueryHandler,
			Flags:        []cli.Flag{commands.JSONFlag},
			BashComplete: commands.DaemonList,
		},
		{
			Name:         cmn.GetWhatDaemonStatus,
			Usage:        "returns status of AIS Daemon",
			Action:       commands.DaemonStatus,
			Flags:        []cli.Flag{commands.JSONFlag},
			BashComplete: commands.DaemonList,
		},
		{
			Name:         cmn.GetWhatConfig,
			Usage:        "returns config of AIS daemon",
			Action:       commands.GetQueryHandler,
			Flags:        []cli.Flag{commands.JSONFlag},
			BashComplete: commands.DaemonList,
		},
		{
			Name:         cmn.GetWhatStats,
			Usage:        "returns stats of AIS daemon",
			Action:       commands.GetQueryHandler,
			Flags:        []cli.Flag{commands.JSONFlag},
			BashComplete: commands.DaemonList,
		},
		{
			Name:    "list",
			Aliases: []string{"ls"},
			Usage:   "returns list of AIS Daemons",
			Action:  commands.ListAIS,
			Flags:   []cli.Flag{commands.VerboseFlag},
		},
	}

	sort.Sort(cli.CommandsByName(aisCLI.Commands))
	err := aisCLI.Run(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
