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

// TODO: remote detach: use showRemoteAISHandler() to populate completions

var (
	clusterCmdsFlags = map[string][]cli.Flag{
		subcmdCluAttach: {},
		subcmdCluDetach: {},
		subcmdCluConfig: {},
		subcmdShutdown:  {},
		subcmdRebalance: {},
		subcmdPrimary:   {},
		subcmdJoin: {
			nodeTypeFlag,
		},
		subcmdStartMaint: {
			noRebalanceFlag,
		},
		subcmdDecommission: {
			shutdownFlag,
			noRebalanceFlag,
		},
	}

	clusterCmds = []cli.Command{
		{
			Name:  commandCluster,
			Usage: "monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc.",
			Subcommands: []cli.Command{
				makeAlias(showCmdCluster, "", true, commandShow), // alias for`ais show`
				{
					Name:      subcmdCluAttach,
					Usage:     "attach a remote ais cluster",
					ArgsUsage: attachRemoteAISArgument,
					Flags:     clusterCmdsFlags[subcmdAttach],
					Action:    attachRemoteAISHandler,
				},
				{
					Name:      subcmdCluDetach,
					Usage:     "detach a remote ais cluster",
					ArgsUsage: detachRemoteAISArgument,
					Flags:     clusterCmdsFlags[subcmdDetach],
					Action:    detachRemoteAISHandler,
				},
				{
					Name:         subcmdCluConfig,
					Usage:        "configure the cluster or a specific node",
					ArgsUsage:    cluConfigArgument,
					Flags:        clusterCmdsFlags[subcmdCluConfig],
					Action:       cluConfigHandler,
					BashComplete: cluConfigCompletions,
				},
				{
					Name:   subcmdRebalance,
					Usage:  "rebalance data among targets in the cluster",
					Flags:  clusterCmdsFlags[subcmdRebalance],
					Action: startXactionHandler,
				},
				{
					Name:         subcmdPrimary,
					Usage:        "set a new primary proxy",
					ArgsUsage:    daemonIDArgument,
					Flags:        clusterCmdsFlags[subcmdPrimary],
					Action:       setPrimaryHandler,
					BashComplete: daemonCompletions(completeProxies),
				},
				{
					Name:   subcmdShutdown,
					Usage:  "shut down the cluster",
					Flags:  clusterCmdsFlags[subcmdShutdown],
					Action: clusterShutdownHandler,
				},
				{
					Name:  subcmdMembership,
					Usage: "manage members of the ais cluster",
					Subcommands: []cli.Command{
						{
							Name:      subcmdJoin,
							Usage:     "add a node to the cluster",
							ArgsUsage: joinNodeArgument,
							Flags:     clusterCmdsFlags[subcmdJoin],
							Action:    joinNodeHandler,
						},
						{
							Name:         subcmdStartMaint,
							Usage:        "mark a node to be under \"maintenance\"",
							ArgsUsage:    daemonIDArgument,
							Flags:        clusterCmdsFlags[subcmdStartMaint],
							Action:       nodeMaintenanceHandler,
							BashComplete: daemonCompletions(completeAllDaemons),
						},
						{
							Name:         subcmdStopMaint,
							Usage:        "remove a node from \"maintenance\"",
							ArgsUsage:    daemonIDArgument,
							Action:       nodeMaintenanceHandler,
							BashComplete: daemonCompletions(completeAllDaemons),
						},
						{
							Name:         subcmdDecommission,
							Usage:        "safely remove a node in the cluster",
							ArgsUsage:    daemonIDArgument,
							Flags:        clusterCmdsFlags[subcmdDecommission],
							Action:       nodeMaintenanceHandler,
							BashComplete: daemonCompletions(completeAllDaemons),
						},
						{
							Name:         subcmdShutdown,
							Usage:        "shutdown a node in the cluster",
							ArgsUsage:    daemonIDArgument,
							Flags:        clusterCmdsFlags[subcmdDecommission],
							Action:       nodeMaintenanceHandler,
							BashComplete: daemonCompletions(completeAllDaemons),
						},
					},
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
	if alias == cmn.URLParamWhat {
		return fmt.Errorf("cannot use %q as an alias", cmn.URLParamWhat)
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

func cluConfigHandler(c *cli.Context) (err error) {
	if _, err = fillMap(); err != nil {
		return
	}
	return cluConfig(c)
}

func clusterShutdownHandler(c *cli.Context) (err error) {
	if err := api.ShutdownCluster(defaultAPIParams); err != nil {
		return err
	}

	fmt.Fprint(c.App.Writer, "All nodes in the cluster are being shut down.\n")
	return
}

func joinNodeHandler(c *cli.Context) (err error) {
	var (
		daemonType      = parseStrFlag(c, nodeTypeFlag)
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
		NodeHostname: socketAddrParts[0],
		DaemonPort:   socketAddrParts[1],
		DirectURL:    prefix + socketAddr,
	}
	nodeInfo := &cluster.Snode{
		DaemonID:        daemonID,
		DaemonType:      daemonType,
		PublicNet:       netInfo,
		IntraControlNet: netInfo,
		IntraDataNet:    netInfo,
	}
	if _, err = api.JoinCluster(defaultAPIParams, nodeInfo); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%s with ID %q successfully joined the cluster\n", cmn.CapitalizeString(daemonType), daemonID)
	return
}

func nodeMaintenanceHandler(c *cli.Context) (err error) {
	if c.NArg() < 1 {
		return missingArgumentsError(c, "daemon ID")
	}
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return err
	}

	var (
		xactID string
		sid    string = c.Args().First()
		action string = c.Command.Name

		node *cluster.Snode = smap.GetNode(sid)
	)
	if node == nil {
		return fmt.Errorf("node %q does not exist", sid)
	}
	if smap.IsPrimary(node) {
		return fmt.Errorf("node %q is primary, cannot %s", sid, action)
	}

	skipRebalance := flagIsSet(c, noRebalanceFlag) || node.IsProxy()
	if skipRebalance && node.IsTarget() {
		fmt.Fprintln(c.App.Writer, "Warning: Skipping Rebalance could lead to data loss! To rebalance the cluster manually at a later time, please run: `ais job start rebalance`")
	}

	actValue := &cmn.ActValDecommision{DaemonID: sid, SkipRebalance: skipRebalance}
	switch action {
	case subcmdStartMaint:
		xactID, err = api.StartMaintenance(defaultAPIParams, actValue)
	case subcmdStopMaint:
		xactID, err = api.StopMaintenance(defaultAPIParams, actValue)
	case subcmdDecommission:
		xactID, err = api.Decommission(defaultAPIParams, actValue)
	case subcmdShutdown:
		xactID, err = api.ShutdownNode(defaultAPIParams, actValue)
	}
	if err != nil {
		return err
	}
	if xactID != "" {
		fmt.Fprintf(c.App.Writer,
			"Started rebalance %q, use 'ais show job xaction %s' to monitor progress\n", xactID, xactID)
	}
	if action == subcmdStopMaint {
		fmt.Fprintf(c.App.Writer, "Node %q removed from maintenance\n", sid)
	} else if action == subcmdDecommission && skipRebalance {
		fmt.Fprintf(c.App.Writer, "Node %q removed from the cluster\n", sid)
	} else if action == subcmdShutdown && skipRebalance {
		fmt.Fprintf(c.App.Writer, "Node %q is being shut down\n", sid)
	} else {
		fmt.Fprintf(c.App.Writer, "Node %q is under maintenance\n", sid)
	}
	return nil
}

func setPrimaryHandler(c *cli.Context) (err error) {
	daemonID := c.Args().First()
	if daemonID == "" {
		return missingArgumentsError(c, "proxy daemon ID")
	}
	primarySmap, err := fillMap()
	if err != nil {
		return
	}
	if _, ok := primarySmap.Pmap[daemonID]; !ok {
		return incorrectUsageMsg(c, "%s: is not a proxy", daemonID)
	}
	err = api.SetPrimaryProxy(defaultAPIParams, daemonID)
	if err == nil {
		fmt.Fprintf(c.App.Writer, "%s is now a new primary\n", daemonID)
	}
	return err
}
