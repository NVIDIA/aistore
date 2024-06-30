// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file provides advanced commands that are useful for testing or development but not everyday use.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

var (
	advancedCmd = cli.Command{
		Name:  commandAdvanced,
		Usage: "special commands intended for development and advanced usage",
		Subcommands: []cli.Command{
			jobStartResilver,
			{
				Name:         cmdPreload,
				Usage:        "preload object metadata into in-memory cache",
				ArgsUsage:    bucketArgument,
				Action:       loadLomCacheHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			{
				Name:         cmdRmSmap,
				Usage:        "immediately remove node from cluster map (beware: potential data loss!)",
				ArgsUsage:    nodeIDArgument,
				Action:       removeNodeFromSmap,
				BashComplete: suggestAllNodes,
			},
			{
				Name:   cmdRandNode,
				Usage:  "print random node ID (by default, ID of a randomly selected target)",
				Action: randNode,
				BashComplete: func(c *cli.Context) {
					if c.NArg() == 0 {
						fmt.Println(apc.Proxy, apc.Target)
					}
				},
			},
			{
				Name:         cmdRandMountpath,
				Usage:        "print a random mountpath from a given target",
				Action:       randMountpath,
				BashComplete: suggestTargets,
			},
			{
				Name:         cmdRotateLogs,
				Usage:        "rotate aistore logs",
				ArgsUsage:    optionalNodeIDArgument,
				Action:       rotateLogs,
				BashComplete: suggestAllNodes,
			},
			{
				Name:         cmdBackendEnable,
				Usage:        "(re)enable cloud backend",
				ArgsUsage:    cloudProviderArg,
				Action:       backendEnableHandler,
				BashComplete: suggestCloudProvider,
			},
			{
				Name:         cmdBackendDisable,
				Usage:        "disable cloud backend",
				ArgsUsage:    cloudProviderArg,
				Action:       backendDisableHandler,
				BashComplete: suggestCloudProvider,
			},
		},
	}
)

func loadLomCacheHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	} else if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}
	bck, err := parseBckURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}
	xargs := xact.ArgsMsg{Kind: apc.ActLoadLomCache, Bck: bck}
	return startXaction(c, &xargs, "")
}

func removeNodeFromSmap(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}
	node, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	if node.IsProxy() {
		smap, err := getClusterMap(c)
		if err != nil {
			return err // cannot happen
		}
		if smap.IsPrimary(node) {
			return fmt.Errorf("%s is primary (cannot remove the primary node)", sname)
		}
	}
	return api.RemoveNodeUnsafe(apiBP, node.ID())
}

func randNode(c *cli.Context) error {
	var (
		si        *meta.Snode
		smap, err = getClusterMap(c)
	)
	if err != nil {
		return err
	}
	if c.NArg() > 0 && c.Args().Get(0) == apc.Proxy {
		si, err = smap.GetRandProxy(false) // _not_ excluding primary
	} else {
		si, err = smap.GetRandTarget()
	}
	if err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer, si.ID())
	return nil
}

func randMountpath(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	tsi, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	if tsi.IsProxy() {
		return fmt.Errorf("%s is a 'proxy' (expecting 'target')", sname)
	}
	daeStatus, err := _status(tsi)
	if err != nil {
		return V(err)
	}
	cdf := daeStatus.Node.TargetCDF
	for mpath := range cdf.Mountpaths {
		fmt.Fprintln(c.App.Writer, mpath)
		break
	}
	return nil
}

func rotateLogs(c *cli.Context) error {
	node, sname, err := arg0Node(c)
	if err != nil {
		return err
	}
	// 1. node
	if node != nil {
		if err := api.RotateLogs(apiBP, node.ID()); err != nil {
			return V(err)
		}
		actionDone(c, sname+": rotated logs")
		return nil
	}
	// 2. or cluster
	if err := api.RotateClusterLogs(apiBP); err != nil {
		return V(err)
	}
	actionDone(c, "cluster: rotated all logs")
	return nil
}

func backendEnableHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	cloudProvider := c.Args().Get(0)
	if err := api.EnableBackend(apiBP, cloudProvider); err != nil {
		return err
	}
	actionDone(c, "cluster: enabled "+cloudProvider+" backend")
	return nil
}

func backendDisableHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	cloudProvider := c.Args().Get(0)
	if err := api.DisableBackend(apiBP, cloudProvider); err != nil {
		return err
	}
	actionDone(c, "cluster: disabled "+cloudProvider+" backend")
	return nil
}
