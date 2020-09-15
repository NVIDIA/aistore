// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides commands that show and update bucket properties and configuration.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
					Name:      subcmdAttachRemoteAIS,
					Usage:     "attach remote cluster",
					ArgsUsage: attachRemoteAISArgument,
					Flags:     attachCmdsFlags[subcmdAttachRemoteAIS],
					Action:    attachRemoteAISHandler,
				},
				{
					Name:      subcmdAttachMountpath,
					Usage:     "attach mountpath",
					ArgsUsage: attachMountpathArgument,
					Flags:     attachCmdsFlags[subcmdAttachMountpath],
					Action:    attachMountpathHandler,
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
					Name:      subcmdDetachRemoteAIS,
					Usage:     "detach remote cluster",
					ArgsUsage: detachRemoteAISArgument,
					Flags:     detachCmdsFlags[subcmdDetachRemoteAIS],
					Action:    detachRemoteAISHandler,
				},
				{
					Name:      subcmdDetachMountpath,
					Usage:     "detach mountpath",
					ArgsUsage: detachMountpathArgument,
					Flags:     detachCmdsFlags[subcmdDetachMountpath],
					Action:    detachMountpathHandler,
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
	fmt.Fprintf(c.App.Writer, "Remote cluster %s successfully detached\n", alias)
	return
}

func attachMountpathHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, daemonMountpathPairArgument)
	}

	kvs, err := makePairs(c.Args())
	if err != nil {
		return err
	}
	smap, err := fillMap()
	if err != nil {
		return err
	}
	for nodeID, mountpath := range kvs {
		si := smap.GetTarget(nodeID)
		if si == nil {
			return fmt.Errorf("daemon with ID (%s) does not exist", nodeID)
		}
		if err := api.AddMountpath(defaultAPIParams, si, mountpath); err != nil {
			return err
		}
		fmt.Fprintf(c.App.Writer, "Node %q: attached mountpath %q\n", si.DaemonID, mountpath)
	}
	return nil
}

func detachMountpathHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, daemonMountpathPairArgument)
	}

	kvs, err := makePairs(c.Args())
	if err != nil {
		return err
	}
	smap, err := fillMap()
	if err != nil {
		return err
	}
	for nodeID, mountpath := range kvs {
		si := smap.GetTarget(nodeID)
		if si == nil {
			return fmt.Errorf("daemon with ID (%s) does not exist", nodeID)
		}
		if err := api.RemoveMountpath(defaultAPIParams, si.DaemonID, mountpath); err != nil {
			return err
		}
		fmt.Fprintf(c.App.Writer, "Node %q: detached mountpath %q\n", si.DaemonID, mountpath)
	}
	return nil
}
