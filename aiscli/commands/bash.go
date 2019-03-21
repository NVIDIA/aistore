// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles bash completions for the CLI
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/urfave/cli"
)

// Bash Completion
func DaemonList(_ *cli.Context) {
	fillMap(ClusterURL)
	for dae := range proxy {
		fmt.Println(dae)
	}
	for dae := range target {
		fmt.Println(dae)
	}
}
