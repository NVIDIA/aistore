// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	joinCmdsFlags = map[string][]cli.Flag{
		subcmdJoinProxy:  {},
		subcmdJoinTarget: {},
	}

	cluSpecificCmds = []cli.Command{
		{
			Name:  commandJoin,
			Usage: "add a node to the cluster",
			Subcommands: []cli.Command{
				{
					Name:      subcmdJoinProxy,
					Usage:     "add a proxy node to the cluster",
					ArgsUsage: joinNodeArgument,
					Flags:     joinCmdsFlags[subcmdJoinProxy],
					Action:    joinNodeHandler,
				},
				{
					Name:      subcmdJoinTarget,
					Usage:     "add a target node to the cluster",
					ArgsUsage: joinNodeArgument,
					Flags:     joinCmdsFlags[subcmdJoinTarget],
					Action:    joinNodeHandler,
				},
			},
		},
	}
)

func joinNodeHandler(c *cli.Context) (err error) {
	var (
		daemonType      = c.Command.Name // proxy|target
		prefix          string
		daemonID        string
		socketAddr      string
		socketAddrParts []string
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "public socket address to communicate with the node")
	}
	socketAddr = c.Args().Get(0)

	socketAddrParts = strings.Split(socketAddr, ":")
	if len(socketAddrParts) != 2 {
		return fmt.Errorf("invalid socket address, format 'IP:PORT' expected")
	}

	daemonID = c.Args().Get(1) // user-given ID
	if daemonID == "" {
		// default is a random generated string
		daemonID = cmn.RandString(8)
	}

	if prefix, err = getPrefixFromPrimary(); err != nil {
		return
	}

	netInfo := cluster.NetInfo{
		NodeIPAddr: socketAddrParts[0],
		DaemonPort: socketAddrParts[1],
		DirectURL:  prefix + socketAddr,
	}
	nodeInfo := &cluster.Snode{
		DaemonID:        daemonID,
		DaemonType:      daemonType,
		PublicNet:       netInfo,
		IntraControlNet: netInfo,
		IntraDataNet:    netInfo,
	}
	if err = api.JoinCluster(defaultAPIParams, nodeInfo); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%s with ID %q successfully joined the cluster\n", cmn.CapitalizeString(daemonType), daemonID)
	return
}
