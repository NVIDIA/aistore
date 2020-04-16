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

// TODO: attach/enable/detach/disable mountpath
// TODO: remote detach: use showRemoteAISHandler() to populate completions
// TODO: help messages and usability

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
	detachCmdsFlags = map[string][]cli.Flag{
		subcmdAttachRemoteAIS: {},
		subcmdAttachMountpath: {},
	}

	detachCmds = []cli.Command{
		{
			Name:  commandDetach,
			Usage: "detach remote cluster; detach mountpath",
			Subcommands: []cli.Command{
				{
					Name:         subcmdDetachRemoteAIS,
					Usage:        "detach remote cluster",
					ArgsUsage:    detachRemoteAISArgument,
					Flags:        detachCmdsFlags[subcmdDetachRemoteAIS],
					Action:       detachRemoteAISHandler,
					BashComplete: detachRemoteAISCompletions,
				},
				{
					Name:         subcmdDetachMountpath,
					Usage:        "detach mountpath",
					ArgsUsage:    detachMountpathArgument,
					Flags:        detachCmdsFlags[subcmdDetachMountpath],
					Action:       detachMountpathHandler,
					BashComplete: detachMountpathCompletions,
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

func detachRemoteAISHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		err = missingArgumentsError(c, aliasArgument)
		return
	}
	alias := c.Args().Get(0)
	if err = api.DetachRemoteAIS(defaultAPIParams, alias); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "remote cluster successfully detached\n")
	return
}

func attachMountpathHandler(c *cli.Context) (err error) {
	return incorrectUsageMsg(c, "not implemented yet")
}
func detachMountpathHandler(c *cli.Context) (err error) {
	return incorrectUsageMsg(c, "not implemented yet")
}
