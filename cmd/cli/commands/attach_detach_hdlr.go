// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides commands that show and update bucket properties and configuration.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/urfave/cli"
)

// TODO: detach remote cluster
// TODO: show attached clusters and their props
// TODO: attach or enable mountpath
// TODO: detach/disable ALL OF THE ABOVE
// TODO: help messages and other usability

var (
	attachCmdsFlags = map[string][]cli.Flag{
		subcmdAttachRemoteAIS: {},
		subcmdAttachMountpath: {},
	}

	attachCmds = []cli.Command{
		{
			Name:  commandAttach,
			Usage: "attach remote cluster; attach mountpath",
			Subcommands: []cli.Command{
				{
					Name:         subcmdAttachRemoteAIS,
					Usage:        "attach remote cluster",
					ArgsUsage:    attachRemoteAISArgument,
					Flags:        attachCmdsFlags[subcmdAttachRemoteAIS],
					Action:       attachRemoteAISHandler,
					BashComplete: attachRemoteAISCompletions,
				},
				{
					Name:         subcmdAttachMountpath,
					Usage:        "attach mountpath",
					ArgsUsage:    attachMountpathArgument,
					Flags:        attachCmdsFlags[subcmdAttachMountpath],
					Action:       attachMountpathHandler,
					BashComplete: attachMountpathCompletions,
				},
			},
		},
	}
)

func attachRemoteAISHandler(c *cli.Context) (err error) {
	alias, url, err := parseAliasURL(c)
	if err != nil {
		return
	}
	if err = api.AttachRemoteAIS(defaultAPIParams, alias, url); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "Remote cluster (%s=%s) successfully attached\n", alias, url)
	return
}

func attachMountpathHandler(c *cli.Context) (err error) {
	return incorrectUsageMsg(c, "not implemented yet")
}
