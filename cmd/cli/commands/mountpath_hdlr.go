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

var (
	mpathCmdsFlags = map[string][]cli.Flag{
		subcmdDiskAttach: {},
		subcmdDiskDetach: {},
	}

	mpathCmds = []cli.Command{
		{
			Name:  commandMountpath,
			Usage: "manage mountpaths (disks) in a given storage target",
			Subcommands: []cli.Command{
				makeAlias(showCmdMpath, "", true, commandShow), // alias for `ais show`
				{
					Name:      subcmdDiskAttach,
					Usage:     "attach a mountpath (i.e. disk or volume)",
					ArgsUsage: diskAttachArgument,
					Flags:     mpathCmdsFlags[subcmdDiskAttach],
					Action:    diskAttachHandler,
				},
				{
					Name:      subcmdDiskDetach,
					Usage:     "detach a mountpath",
					ArgsUsage: diskDetachArgument,
					Flags:     mpathCmdsFlags[subcmdDiskDetach],
					Action:    diskDetachHandler,
				},
			},
		},
	}
)

func diskAttachHandler(c *cli.Context) (err error) {
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
			si = smap.GetProxy(nodeID)
			if si == nil {
				return fmt.Errorf("daemon %q does not exist", nodeID)
			}
			return fmt.Errorf("daemon %q is a proxy, run \"ais show cluster target\" to list targets", nodeID)
		}
		if err := api.AddMountpath(defaultAPIParams, si, mountpath); err != nil {
			return err
		}
		fmt.Fprintf(c.App.Writer, "Node %q attached mountpath %q\n", si.DaemonID, mountpath)
	}
	return nil
}

func diskDetachHandler(c *cli.Context) (err error) {
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
		fmt.Fprintf(c.App.Writer, "Node %q detached mountpath %q\n", si.DaemonID, mountpath)
	}
	return nil
}
