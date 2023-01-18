// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file provides commands that show and update bucket properties and configuration.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/urfave/cli"
)

// TODO: enable/disable mountpath

var (
	mpathCmdsFlags = map[string][]cli.Flag{
		subcmdMpathAttach: {
			forceFlag,
		},
		subcmdMpathEnable: {},
		subcmdMpathDetach: {
			noResilverFlag,
		},
		subcmdMpathDisable: {
			noResilverFlag,
		},
	}

	mpathCmd = cli.Command{
		Name:   commandMountpath,
		Usage:  "show and attach/detach target mountpaths",
		Action: showMpathHandler,
		Subcommands: []cli.Command{
			makeAlias(showCmdMpath, "", true, commandShow), // alias for `ais show`
			{
				Name:         subcmdMpathAttach,
				Usage:        "attach mountpath (i.e., formatted disk or RAID) to a target node",
				ArgsUsage:    nodeMountpathPairArgument,
				Flags:        mpathCmdsFlags[subcmdMpathAttach],
				Action:       mpathAttachHandler,
				BashComplete: suggestTargetNodes,
			},
			{
				Name:         subcmdMpathEnable,
				Usage:        "(re)enable target's mountpath",
				ArgsUsage:    nodeMountpathPairArgument,
				Flags:        mpathCmdsFlags[subcmdMpathEnable],
				Action:       mpathEnableHandler,
				BashComplete: suggestTargetNodes,
			},
			{
				Name:         subcmdMpathDetach,
				Usage:        "detach mountpath (i.e., formatted disk or RAID) from a target node",
				ArgsUsage:    nodeMountpathPairArgument,
				Flags:        mpathCmdsFlags[subcmdMpathDetach],
				Action:       mpathDetachHandler,
				BashComplete: suggestTargetNodes,
			},
			{
				Name:         subcmdMpathDisable,
				Usage:        "disable mountpath (deactivate but keep in a target's volume)",
				ArgsUsage:    nodeMountpathPairArgument,
				Flags:        mpathCmdsFlags[subcmdMpathDisable],
				Action:       mpathDisableHandler,
				BashComplete: suggestTargetNodes,
			},
		},
	}
)

func mpathAttachHandler(c *cli.Context) (err error)  { return mpathAction(c, apc.ActMountpathAttach) }
func mpathEnableHandler(c *cli.Context) (err error)  { return mpathAction(c, apc.ActMountpathEnable) }
func mpathDetachHandler(c *cli.Context) (err error)  { return mpathAction(c, apc.ActMountpathDetach) }
func mpathDisableHandler(c *cli.Context) (err error) { return mpathAction(c, apc.ActMountpathDisable) }

func mpathAction(c *cli.Context, action string) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	smap, errMap := fillNodeStatusMap(c)
	if errMap != nil {
		return errMap
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
		nodeID = cluster.N2ID(nodeID)
		if nodeID != "" && smap.GetTarget(nodeID) != nil {
			return fmt.Errorf("target %s: missing mountpath to %s", first, action)
		}
		return err
	}
	for nodeID, mountpath := range kvs {
		var (
			err   error
			acted string
		)
		nodeID = cluster.N2ID(nodeID)
		si := smap.GetTarget(nodeID)
		if si == nil {
			si = smap.GetProxy(nodeID)
			if si == nil {
				return fmt.Errorf("node %q does not exist", nodeID)
			}
			return fmt.Errorf("node %q is a proxy, <TAB-TAB> target IDs or run \"ais show cluster target\" to select target",
				nodeID)
		}
		switch action {
		case apc.ActMountpathAttach:
			acted = "attached"
			err = api.AttachMountpath(apiBP, si, mountpath, flagIsSet(c, forceFlag))
		case apc.ActMountpathEnable:
			acted = "enabled"
			err = api.EnableMountpath(apiBP, si, mountpath)
		case apc.ActMountpathDetach:
			acted = "detached"
			err = api.DetachMountpath(apiBP, si, mountpath, flagIsSet(c, noResilverFlag))
		case apc.ActMountpathDisable:
			acted = "disabled"
			err = api.DisableMountpath(apiBP, si, mountpath, flagIsSet(c, noResilverFlag))
		default:
			return incorrectUsageMsg(c, "invalid mountpath action %q", action)
		}
		if err != nil {
			return err
		}
		fmt.Fprintf(c.App.Writer, "Node %q %s mountpath %q\n", si.ID(), acted, mountpath)
	}
	return nil
}
