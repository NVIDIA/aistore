// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS buckets.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"fmt"
	"os/exec"

	"github.com/urfave/cli"
)

var (
	k8sCmdsFlags = map[string][]cli.Flag{
		cmdK8sSvc:     {},
		cmdK8sCluster: {},
	}

	k8sCmd = cli.Command{
		Name:  cmdK8s,
		Usage: "Show Kubernetes pods and services",
		Subcommands: []cli.Command{
			{
				Name:   cmdK8sSvc,
				Usage:  "Show Kubernetes services",
				Flags:  sortFlags(k8sCmdsFlags[cmdK8sSvc]),
				Action: k8sShowSvcHandler,
			},
			{
				Name:      cmdK8sCluster,
				Usage:     "Show AIS cluster",
				Flags:     sortFlags(k8sCmdsFlags[cmdK8sCluster]),
				ArgsUsage: optionalNodeIDArgument,
				Action:    k8sShowClusterHandler,
				BashComplete: func(c *cli.Context) {
					if c.NArg() != 0 {
						return
					}
					suggestAllNodes(c)
				},
			},
		},
	}

	// kubectl command lines
	cmdPodList  = []string{"get", "pods"}
	cmdSvcList  = []string{"get", "svc", "-n", "ais"}
	cmdNodeInfo = []string{"get", "pods", "-n", "ais", "-o=wide"}
)

func k8sShowSvcHandler(c *cli.Context) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), execLinuxCommandTimeLong)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", cmdSvcList...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}
	fmt.Fprint(c.App.Writer, string(output))
	return nil
}

func k8sShowClusterHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return k8sShowEntireCluster(c)
	}
	return k8sShowSingleDaemon(c)
}

func k8sShowEntireCluster(c *cli.Context) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), execLinuxCommandTimeLong)
	defer cancel()
	cmd := exec.CommandContext(ctx, cmdK8s, cmdPodList...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}
	fmt.Fprint(c.App.Writer, string(output))
	return
}

func k8sShowSingleDaemon(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	node, _, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}

	cmdLine := make([]string, 0, len(cmdNodeInfo)+1)
	cmdLine = append(cmdLine, cmdNodeInfo...)
	cmdLine = append(cmdLine, "--selector=ais-daemon-id="+node.ID())
	ctx, cancel := context.WithTimeout(context.Background(), execLinuxCommandTimeLong)
	defer cancel()
	cmd := exec.CommandContext(ctx, cmdK8s, cmdLine...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer, string(output))
	return nil
}
