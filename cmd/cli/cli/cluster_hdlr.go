// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

const (
	fmtRebalanceStarted = "Started rebalance %q (to monitor, run 'ais show rebalance %s').\n"

	roleProxyShort  = "p"
	roleTargetShort = "t"
)

var (
	clusterCmdsFlags = map[string][]cli.Flag{
		cmdCluAttach: {},
		cmdCluDetach: {},
		cmdCluConfig: {
			transientFlag,
		},
		cmdShutdown: {},
		cmdPrimary:  {},
		cmdJoin: {
			roleFlag,
		},
		cmdStartMaint: {
			noRebalanceFlag,
		},
		cmdShutdown + ".node": {
			noRebalanceFlag,
			noShutdownFlag,
			rmUserDataFlag,
		},
		cmdNodeDecommission + ".node": {
			noRebalanceFlag,
			noShutdownFlag,
			rmUserDataFlag,
			yesFlag,
		},
		cmdClusterDecommission: {
			rmUserDataFlag,
			yesFlag,
		},
		commandStart: {},
		commandStop:  {},
		commandShow: {
			allJobsFlag,
			noHeaderFlag,
		},
		cmdResetStats: {
			errorsOnlyFlag,
		},
	}

	startRebalance = cli.Command{
		Name:   commandStart,
		Usage:  "rebalance ais cluster",
		Flags:  clusterCmdsFlags[commandStart],
		Action: startClusterRebalanceHandler,
	}
	stopRebalance = cli.Command{
		Name:   commandStop,
		Usage:  "stop rebalancing ais cluster",
		Flags:  clusterCmdsFlags[commandStop],
		Action: stopClusterRebalanceHandler,
	}

	clusterCmd = cli.Command{
		Name:  commandCluster,
		Usage: "monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc.",
		Subcommands: []cli.Command{
			makeAlias(showCmdCluster, "", true, commandShow), // alias for `ais show`
			{
				Name:      cmdCluAttach,
				Usage:     "attach remote ais cluster",
				ArgsUsage: attachRemoteAISArgument,
				Flags:     clusterCmdsFlags[cmdAttach],
				Action:    attachRemoteAISHandler,
			},
			{
				Name:         cmdCluDetach,
				Usage:        "detach remote ais cluster",
				ArgsUsage:    detachRemoteAISArgument,
				Flags:        clusterCmdsFlags[cmdDetach],
				Action:       detachRemoteAISHandler,
				BashComplete: suggestRemote,
			},
			{
				Name: cmdRebalance,
				Subcommands: []cli.Command{
					startRebalance,
					stopRebalance,
					{
						Name:         commandShow,
						Usage:        "show global rebalance",
						Flags:        clusterCmdsFlags[commandShow],
						BashComplete: rebalanceCompletions,
						Action:       showClusterRebalanceHandler,
					},
				},
			},
			{
				Name:         cmdPrimary,
				Usage:        "select a new primary proxy/gateway",
				ArgsUsage:    nodeIDArgument,
				Flags:        clusterCmdsFlags[cmdPrimary],
				Action:       setPrimaryHandler,
				BashComplete: suggestProxyNodes,
			},
			{
				Name:   cmdShutdown,
				Usage:  "shutdown cluster",
				Flags:  clusterCmdsFlags[cmdShutdown],
				Action: clusterShutdownHandler,
			},
			{
				Name:   cmdClusterDecommission,
				Usage:  "decommission entire cluster",
				Flags:  clusterCmdsFlags[cmdClusterDecommission],
				Action: clusterDecommissionHandler,
			},
			{
				Name:  cmdMembership,
				Usage: "manage cluster membership (scale up or down)",
				Subcommands: []cli.Command{
					{
						Name:      cmdJoin,
						Usage:     "add a node to the cluster",
						ArgsUsage: joinNodeArgument,
						Flags:     clusterCmdsFlags[cmdJoin],
						Action:    joinNodeHandler,
					},
					{
						Name:         cmdStartMaint,
						Usage:        "put node in maintenance mode, temporarily suspend its operation",
						ArgsUsage:    nodeIDArgument,
						Flags:        clusterCmdsFlags[cmdStartMaint],
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
					{
						Name:         cmdStopMaint,
						Usage:        "activate node by taking it back from \"maintenance\"",
						ArgsUsage:    nodeIDArgument,
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
					{
						Name:         cmdNodeDecommission,
						Usage:        "safely and permanently remove node from the cluster",
						ArgsUsage:    nodeIDArgument,
						Flags:        clusterCmdsFlags[cmdNodeDecommission+".node"],
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
					{
						Name:         cmdShutdown,
						Usage:        "shutdown a node",
						ArgsUsage:    nodeIDArgument,
						Flags:        clusterCmdsFlags[cmdShutdown+".node"],
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
				},
			},
			{
				Name:         cmdResetStats,
				Usage:        "reset cluster or node stats (all cumulative metrics or only errors)",
				ArgsUsage:    optionalNodeIDArgument,
				Flags:        clusterCmdsFlags[cmdResetStats],
				Action:       resetStatsHandler,
				BashComplete: suggestAllNodes,
			},
		},
	}
)

func attachRemoteAISHandler(c *cli.Context) (err error) {
	alias, url, err := parseRemAliasURL(c)
	if err != nil {
		return
	}
	if err = api.AttachRemoteAIS(apiBP, alias, url); err != nil {
		return
	}
	msg := fmt.Sprintf("Remote cluster (%s=%s) successfully attached", alias, url)
	actionDone(c, msg)
	return
}

func detachRemoteAISHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		err = missingArgumentsError(c, c.Command.ArgsUsage)
		return
	}
	alias := c.Args().Get(0)
	if err = api.DetachRemoteAIS(apiBP, alias); err != nil {
		return
	}
	msg := fmt.Sprintf("Remote cluster %s successfully detached", alias)
	actionDone(c, msg)
	return
}

func clusterShutdownHandler(c *cli.Context) (err error) {
	if err := api.ShutdownCluster(apiBP); err != nil {
		return err
	}
	actionDone(c, "Cluster successfully shut down")
	return
}

func clusterDecommissionHandler(c *cli.Context) error {
	// ask confirmation
	if !flagIsSet(c, yesFlag) {
		if ok := confirm(c, "Proceed?", "about to permanently decommission AIS cluster. The operation cannot be undone!"); !ok {
			return nil
		}
	}
	rmUserData := flagIsSet(c, rmUserDataFlag)
	if err := api.DecommissionCluster(apiBP, rmUserData); err != nil {
		return err
	}
	actionDone(c, "Cluster successfully decommissioned")
	return nil
}

func joinNodeHandler(c *cli.Context) (err error) {
	var (
		daemonType, prefix string
		addr, rebID        string
		addrParts          []string
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "public IPv4:PORT address to communicate with the node")
	}
	addr = c.Args().Get(0)
	addrParts = strings.Split(addr, ":")
	if len(addrParts) != 2 {
		return fmt.Errorf("invalid address, expecting 'IPv4:PORT'")
	}

	switch parseStrFlag(c, roleFlag) {
	case apc.Proxy, roleProxyShort:
		daemonType = apc.Proxy
	case apc.Target, roleTargetShort:
		daemonType = apc.Target
	default:
		return fmt.Errorf("invalid aisnode role, must be one of: %q (or %q), %q (or %q)",
			apc.Proxy, roleProxyShort, apc.Target, roleTargetShort)
	}

	if addrParts[0] == "localhost" {
		addrParts[0] = "127.0.0.1"
	}

	prefix = getPrefixFromPrimary()
	netInfo := cluster.NetInfo{
		Hostname: addrParts[0],
		Port:     addrParts[1],
		URL:      prefix + addr,
	}
	nodeInfo := &cluster.Snode{
		// once contacted, aisnode reports its ID (see also: `envDaemonID` and `genDaemonID`)
		DaeID: "",
		// (important to have it right)
		DaeType: daemonType,
		// for the primary to perform initial handshake, validation, and the rest of it (NOTE: control-net)
		ControlNet: netInfo,
	}
	if rebID, nodeInfo.DaeID, err = api.JoinCluster(apiBP, nodeInfo); err != nil {
		return
	}

	// double check
	var sname string
	_, sname, err = getNodeIDName(c, nodeInfo.DaeID)
	if err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "Node %s successfully joined the cluster\n", sname)

	if rebID != "" {
		fmt.Fprintf(c.App.Writer, fmtRebalanceStarted, rebID, rebID)
	}
	return
}

func nodeMaintShutDecommHandler(c *cli.Context) error {
	if c.NArg() < 1 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	smap, err := getClusterMap(c)
	if err != nil {
		return err
	}
	sid, sname, err := getNodeIDName(c, c.Args().First())
	if err != nil {
		return err
	}
	node := smap.GetNode(sid)
	debug.Assert(node != nil)

	action := c.Command.Name
	if smap.IsPrimary(node) {
		return fmt.Errorf("%s is primary (cannot %s the primary node)", sname, action)
	}
	var (
		xid           string
		skipRebalance = flagIsSet(c, noRebalanceFlag) || node.IsProxy()
		noShutdown    = flagIsSet(c, noShutdownFlag)
		rmUserData    = flagIsSet(c, rmUserDataFlag)
		actValue      = &apc.ActValRmNode{DaemonID: sid, SkipRebalance: skipRebalance, NoShutdown: noShutdown}
	)
	if skipRebalance && node.IsTarget() {
		fmt.Fprintf(c.App.Writer,
			"Beware: performing %q and _not_ running global rebalance may lead to a loss of data!\n", action)
		fmt.Fprintln(c.App.Writer,
			"To rebalance the cluster manually at a later time, please run: `ais job start rebalance`")
	}
	if action == cmdNodeDecommission {
		if !flagIsSet(c, yesFlag) {
			warn := fmt.Sprintf("about to permanently decommission node %s. The operation cannot be undone!", sname)
			if ok := confirm(c, "Proceed?", warn); !ok {
				return nil
			}
		}
		actValue.NoShutdown = noShutdown
		actValue.RmUserData = rmUserData
	} else {
		const fmterr = "option %s is valid only for decommissioning\n"
		if noShutdown {
			return fmt.Errorf(fmterr, qflprn(noShutdownFlag))
		}
		if rmUserData {
			return fmt.Errorf(fmterr, qflprn(rmUserDataFlag))
		}
	}
	switch action {
	case cmdStartMaint:
		xid, err = api.StartMaintenance(apiBP, actValue)
	case cmdStopMaint:
		xid, err = api.StopMaintenance(apiBP, actValue)
	case cmdNodeDecommission:
		xid, err = api.DecommissionNode(apiBP, actValue)
	case cmdShutdown:
		xid, err = api.ShutdownNode(apiBP, actValue)
	}
	if err != nil {
		return err
	}
	if xid != "" {
		fmt.Fprintf(c.App.Writer, fmtRebalanceStarted, xid, xid)
	}
	switch action {
	case cmdStopMaint:
		fmt.Fprintf(c.App.Writer, "%s is now active (maintenance done)\n", sname)
	case cmdNodeDecommission:
		if skipRebalance || node.IsProxy() {
			fmt.Fprintf(c.App.Writer, "%s has been decommissioned (permanently removed from the cluster)\n", sname)
		} else {
			fmt.Fprintf(c.App.Writer,
				"%s is being decommissioned, please wait for cluster rebalancing to finish...\n", sname)
		}
	case cmdShutdown:
		if skipRebalance || node.IsProxy() {
			fmt.Fprintf(c.App.Writer, "%s has been shutdown\n", sname)
		} else {
			fmt.Fprintf(c.App.Writer,
				"%s is shutting down, please wait for cluster rebalancing to finish...\n", sname)
		}
	case cmdStartMaint:
		fmt.Fprintf(c.App.Writer, "%s is now in maintenance\n", sname)
	}
	return nil
}

func setPrimaryHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	sid, sname, err := getNodeIDName(c, c.Args().First())
	if err != nil {
		return err
	}
	smap, err := getClusterMap(c)
	if err != nil {
		return err
	}
	node := smap.GetNode(sid)
	debug.Assert(node != nil)
	if !node.IsProxy() {
		return incorrectUsageMsg(c, "%s is not a proxy", sname)
	}

	switch {
	case node.Flags.IsSet(cluster.NodeFlagMaint):
		return fmt.Errorf("%s is currently in maintenance", sname)
	case node.Flags.IsSet(cluster.NodeFlagDecomm):
		return fmt.Errorf("%s is currently being decommissioned", sname)
	case node.Flags.IsSet(cluster.SnodeNonElectable):
		return fmt.Errorf("%s is non-electable", sname)
	}

	err = api.SetPrimaryProxy(apiBP, sid, false /*force*/)
	if err == nil {
		actionDone(c, sname+" is now a new primary")
	}
	return err
}

func startClusterRebalanceHandler(c *cli.Context) (err error) {
	return startXactionKind(c, apc.ActRebalance)
}

func stopClusterRebalanceHandler(c *cli.Context) error {
	xargs := xact.ArgsMsg{Kind: apc.ActRebalance, OnlyRunning: true}
	snap, err := getXactSnap(xargs)
	if err != nil {
		return err
	}
	if snap == nil {
		return errors.New("rebalance is not running")
	}

	xargs.ID, xargs.OnlyRunning = snap.ID, false
	if err := api.AbortXaction(apiBP, xargs); err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "Stopped %s[%s]\n", apc.ActRebalance, snap.ID)
	return nil
}

func showClusterRebalanceHandler(c *cli.Context) (err error) {
	var (
		xid      = c.Args().Get(0)
		daemonID = c.Args().Get(1)
	)
	if daemonID == "" && xid != "" {
		// either/or
		if strings.HasPrefix(xid, cluster.TnamePrefix) {
			sid, _, err := getNodeIDName(c, xid)
			if err != nil {
				return err
			}
			daemonID, xid = sid, ""
		}
	}
	xargs := xact.ArgsMsg{
		ID:          xid,
		Kind:        apc.ActRebalance,
		DaemonID:    daemonID,
		OnlyRunning: !flagIsSet(c, allJobsFlag),
	}
	_, err = xactList(c, xargs, false)
	return
}

func resetStatsHandler(c *cli.Context) error {
	var (
		errorsOnly      = flagIsSet(c, errorsOnlyFlag)
		tag             = "stats"
		sid, sname, err = argNode(c)
	)
	if err != nil {
		return err
	}
	if errorsOnly {
		tag = "error metrics"
	}
	// node
	if sid != "" {
		smap, errV := getClusterMap(c)
		debug.AssertNoErr(errV)
		node := smap.GetNode(sid)
		if err := api.ResetDaemonStats(apiBP, node, errorsOnly); err != nil {
			return err
		}
		msg := fmt.Sprintf("%s %s successfully reset", sname, tag)
		actionDone(c, msg)
		return nil
	}
	// cluster
	if err := api.ResetClusterStats(apiBP, errorsOnly); err != nil {
		return err
	}
	msg := fmt.Sprintf("Cluster %s successfully reset", tag)
	actionDone(c, msg)
	return nil
}
