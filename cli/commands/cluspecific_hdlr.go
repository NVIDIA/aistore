// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
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
	cluSpecificCmdsFlags = map[string][]cli.Flag{
		commandStatus: append(longRunFlags, jsonFlag, noHeaderFlag),
	}

	registerCmdsFlags = map[string][]cli.Flag{
		subcmdRegisterProxy:  {},
		subcmdRegisterTarget: {},
	}

	cluSpecificCmds = []cli.Command{
		{
			Name:  commandRegister,
			Usage: "adds a node to the cluster",
			Subcommands: []cli.Command{
				{
					Name:      subcmdRegisterProxy,
					Usage:     "adds a proxy node to the cluster",
					ArgsUsage: registerNodeArgument,
					Flags:     registerCmdsFlags[subcmdRegisterProxy],
					Action:    registerNodeHandler,
				},
				{
					Name:      subcmdRegisterTarget,
					Usage:     "adds a target node to the cluster",
					ArgsUsage: registerNodeArgument,
					Flags:     registerCmdsFlags[subcmdRegisterTarget],
					Action:    registerNodeHandler,
				},
			},
		},
		{
			Name:         commandStatus,
			Usage:        "displays status of a daemon",
			ArgsUsage:    daemonStatusArgument,
			Action:       statusHandler,
			Flags:        cluSpecificCmdsFlags[commandStatus],
			BashComplete: daemonCompletions(true /* optional */, false /* omit proxies */),
		},
	}
)

func registerNodeHandler(c *cli.Context) (err error) {
	var (
		baseParams      = cliAPIParams(clusterURL)
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
		return fmt.Errorf("invalid socket address, should be in format: 'IP:PORT'")
	}

	daemonID = c.Args().Get(1) // user-given ID
	if daemonID == "" {
		// default is a random generated string
		daemonID = cmn.RandString(8)
	}

	if prefix, err = getPrefixFromPrimary(baseParams); err != nil {
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
	if err = api.RegisterNode(baseParams, nodeInfo); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Node with ID %q has been successfully added to the cluster\n", daemonID)
	return
}

func statusHandler(c *cli.Context) (err error) {
	daemonID := c.Args().First() // empty string if no arg given

	primarySmap, err := fillMap(clusterURL)
	if err != nil {
		return
	}

	if err = updateLongRunParams(c); err != nil {
		return
	}

	return clusterDaemonStatus(c, primarySmap, daemonID, flagIsSet(c, jsonFlag), flagIsSet(c, noHeaderFlag))
}
