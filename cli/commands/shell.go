// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles bash completions for the CLI
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

// Bash Completion
func daemonList(_ *cli.Context) {
	baseParams := cliAPIParams(ClusterURL)
	smap, _ := api.GetClusterMap(baseParams)

	for dae := range smap.Pmap {
		fmt.Println(dae)
	}
	for dae := range smap.Tmap {
		fmt.Println(dae)
	}
}

func targetList(_ *cli.Context) {
	baseParams := cliAPIParams(ClusterURL)
	smap, _ := api.GetClusterMap(baseParams)

	for dae := range smap.Tmap {
		fmt.Println(dae)
	}
}

// Returns flags for command
func flagList(c *cli.Context) {
	for _, flag := range c.Command.Flags {
		fmt.Printf("--%s\n", cleanFlag(flag.GetName()))
	}
}

// Xaction list
func xactList(_ *cli.Context) {
	for key := range cmn.XactKind {
		fmt.Println(key)
	}
}
