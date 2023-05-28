// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file provides advanced commands that are useful for testing or development but not everyday use.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
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
				Usage:        "immediately remove node from cluster map (advanced usage - potential data loss!)",
				ArgsUsage:    nodeIDArgument,
				Action:       removeNodeFromSmap,
				BashComplete: suggestAllNodes,
			},
			{
				Name:   cmdRandNode,
				Usage:  "print random node ID (by default, random target)",
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
	return startXaction(c, apc.ActLoadLomCache, bck, "")
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
	daeStatus, err := api.GetStatsAndStatus(apiBP, tsi)
	if err != nil {
		return err
	}
	cdf := daeStatus.Node.TargetCDF
	for mpath := range cdf.Mountpaths {
		fmt.Fprintln(c.App.Writer, mpath)
		break
	}
	return nil
}
