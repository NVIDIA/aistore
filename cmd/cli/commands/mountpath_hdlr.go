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

// TODO: enable/disable mountpath

var (
	mpathCmdsFlags = map[string][]cli.Flag{
		subcmdDiskAttach: {},
		subcmdDiskDetach: {},
	}

	mpathCmd = cli.Command{
		Name:   commandMountpath,
		Usage:  "show and attach/detach target mountpaths",
		Action: showMpathHandler,
		Subcommands: []cli.Command{
			makeAlias(showCmdMpath, "", true, commandShow), // alias for `ais show`
			{
				Name:         subcmdDiskAttach,
				Usage:        "attach mountpath (i.e., formatted disk or RAID) to a target node",
				ArgsUsage:    diskAttachArgument,
				Flags:        mpathCmdsFlags[subcmdDiskAttach],
				Action:       diskAttachHandler,
				BashComplete: daemonCompletions(completeTargets),
			},
			{
				Name:         subcmdDiskDetach,
				Usage:        "detach mountpath (i.e., formatted disk or RAID) from a target node",
				ArgsUsage:    diskDetachArgument,
				Flags:        mpathCmdsFlags[subcmdDiskDetach],
				Action:       diskDetachHandler,
				BashComplete: daemonCompletions(completeTargets),
			},
		},
	}
)

func diskAttachHandler(c *cli.Context) (err error) {
	return _diskAttachDetach(c, true /*attach*/)
}

func diskDetachHandler(c *cli.Context) (err error) {
	return _diskAttachDetach(c, false /*attach*/)
}

func _diskAttachDetach(c *cli.Context, attach bool) error {
	action, acted := "attach", "attached"
	if !attach {
		action, acted = "detach", "detached"
	}
	if c.NArg() == 0 {
		return missingArgumentsError(c, daemonMountpathPairArgument)
	}
	smap, err := fillMap()
	if err != nil {
		return err
	}
	kvs, err := makePairs(c.Args())
	if err != nil {
		// check whether user typed target ID with no mountpath
		first, tail, nodeID := c.Args().First(), c.Args().Tail(), ""
		if len(tail) == 0 {
			nodeID = first
		} else {
			nodeID = tail[len(tail)-1]
		}
		if nodeID != "" && smap.GetTarget(nodeID) != nil {
			return fmt.Errorf("target %s: missing mountpath to %s", first, action)
		}
		return err
	}
	for nodeID, mountpath := range kvs {
		si := smap.GetTarget(nodeID)
		if si == nil {
			si = smap.GetProxy(nodeID)
			if si == nil {
				return fmt.Errorf("node %q does not exist", nodeID)
			}
			return fmt.Errorf("node %q is a proxy, <TAB-TAB> target IDs or run \"ais show cluster target\" to select target",
				nodeID)
		}
		if attach {
			if err := api.AttachMountpath(defaultAPIParams, si, mountpath); err != nil {
				return err
			}
		} else {
			if err := api.DetachMountpath(defaultAPIParams, si, mountpath); err != nil {
				return err
			}
		}
		fmt.Fprintf(c.App.Writer, "Node %q %s mountpath %q\n", si.ID(), acted, mountpath)
	}
	return nil
}
