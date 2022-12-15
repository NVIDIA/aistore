// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

const (
	fmtRebalanceStarted = "Started rebalance %q (to monitor, run 'ais show rebalance %s').\n"

	roleProxyShort  = "p"
	roleTargetShort = "t"
)

var (
	clusterCmdsFlags = map[string][]cli.Flag{
		subcmdCluAttach: {},
		subcmdCluDetach: {},
		subcmdCluConfig: {
			transientFlag,
		},
		subcmdShutdown: {},
		subcmdPrimary:  {},
		subcmdJoin: {
			roleFlag,
		},
		subcmdStartMaint: {
			noRebalanceFlag,
		},
		subcmdShutdown + ".node": {
			noRebalanceFlag,
			noShutdownFlag,
			rmUserDataFlag,
		},
		subcmdNodeDecommission + ".node": {
			noRebalanceFlag,
			noShutdownFlag,
			rmUserDataFlag,
			yesFlag,
		},
		subcmdClusterDecommission: {
			rmUserDataFlag,
			yesFlag,
		},
		commandStart: {},
		commandStop:  {},
		commandShow: {
			allXactionsFlag,
			noHeaderFlag,
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
				Name:      subcmdCluAttach,
				Usage:     "attach remote ais cluster",
				ArgsUsage: attachRemoteAISArgument,
				Flags:     clusterCmdsFlags[subcmdAttach],
				Action:    attachRemoteAISHandler,
			},
			{
				Name:         subcmdCluDetach,
				Usage:        "detach remote ais cluster",
				ArgsUsage:    detachRemoteAISArgument,
				Flags:        clusterCmdsFlags[subcmdDetach],
				Action:       detachRemoteAISHandler,
				BashComplete: suggestRemote,
			},
			{
				Name: subcmdRebalance,
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
				Name:         subcmdPrimary,
				Usage:        "select a new primary proxy/gateway",
				ArgsUsage:    daemonIDArgument,
				Flags:        clusterCmdsFlags[subcmdPrimary],
				Action:       setPrimaryHandler,
				BashComplete: suggestProxyNodes,
			},
			{
				Name:   subcmdShutdown,
				Usage:  "shutdown cluster",
				Flags:  clusterCmdsFlags[subcmdShutdown],
				Action: clusterShutdownHandler,
			},
			{
				Name:   subcmdClusterDecommission,
				Usage:  "decommission entire cluster",
				Flags:  clusterCmdsFlags[subcmdClusterDecommission],
				Action: clusterDecommissionHandler,
			},
			{
				Name:  subcmdMembership,
				Usage: "manage cluster membership (scale up or down)",
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
						Usage:        "put node under \"maintenance\", temporarily suspend its operation",
						ArgsUsage:    daemonIDArgument,
						Flags:        clusterCmdsFlags[subcmdStartMaint],
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
					{
						Name:         subcmdStopMaint,
						Usage:        "activate node by taking it back from \"maintenance\"",
						ArgsUsage:    daemonIDArgument,
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
					{
						Name:         subcmdNodeDecommission,
						Usage:        "safely and permanently remove node from the cluster",
						ArgsUsage:    daemonIDArgument,
						Flags:        clusterCmdsFlags[subcmdNodeDecommission+".node"],
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
					{
						Name:         subcmdShutdown,
						Usage:        "shutdown a node",
						ArgsUsage:    daemonIDArgument,
						Flags:        clusterCmdsFlags[subcmdShutdown+".node"],
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
				},
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
	fmt.Fprintf(c.App.Writer, "Remote cluster (%s=%s) successfully attached\n", alias, url)
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
	fmt.Fprintf(c.App.Writer, "Remote cluster %s successfully detached\n", alias)
	return
}

func clusterShutdownHandler(c *cli.Context) (err error) {
	if err := api.ShutdownCluster(apiBP); err != nil {
		return err
	}

	fmt.Fprint(c.App.Writer, "AIS cluster shut down.\n")
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
	fmt.Fprint(c.App.Writer, "AIS cluster decommissioned.\n")
	return nil
}

func joinNodeHandler(c *cli.Context) (err error) {
	var (
		daemonType, prefix, socketAddr, rebID string
		socketAddrParts                       []string
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "public socket address to communicate with the node")
	}
	socketAddr = c.Args().Get(0)
	socketAddrParts = strings.Split(socketAddr, ":")
	if len(socketAddrParts) != 2 {
		return fmt.Errorf("invalid socket address, format 'IP:PORT' expected")
	}

	switch parseStrFlag(c, roleFlag) {
	case apc.Proxy, roleProxyShort:
		daemonType = apc.Proxy
	case apc.Target, roleTargetShort:
		daemonType = apc.Target
	default:
		return fmt.Errorf("invalid daemon role, one of %q, %q, %q, %q expected",
			apc.Proxy, apc.Target, roleProxyShort, roleTargetShort)
	}

	prefix = getPrefixFromPrimary()
	netInfo := cluster.NetInfo{
		Hostname: socketAddrParts[0],
		Port:     socketAddrParts[1],
		URL:      prefix + socketAddr,
	}
	nodeInfo := &cluster.Snode{
		DaeID:      "", // id is generated by the daemon, see ais.initDaemonID()
		DaeType:    daemonType,
		PubNet:     netInfo,
		ControlNet: netInfo,
		DataNet:    netInfo,
	}
	if rebID, nodeInfo.DaeID, err = api.JoinCluster(apiBP, nodeInfo); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%s %s successfully joined the cluster\n",
		cos.CapitalizeString(daemonType), nodeInfo.Name())

	if rebID != "" {
		fmt.Fprintf(c.App.Writer, fmtRebalanceStarted, rebID, rebID)
	}
	return
}

func nodeMaintShutDecommHandler(c *cli.Context) error {
	if c.NArg() < 1 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return err
	}
	var (
		action   = c.Command.Name
		daemonID = argDaemonID(c.Args().First())
		node     = smap.GetNode(daemonID)
	)
	if node == nil {
		return fmt.Errorf("node %q does not exist", daemonID)
	}
	name := node.StringEx()
	if smap.IsPrimary(node) {
		return fmt.Errorf("%s is primary, cannot %s", name, action)
	}
	var (
		xactID        string
		skipRebalance = flagIsSet(c, noRebalanceFlag) || node.IsProxy()
		noShutdown    = flagIsSet(c, noShutdownFlag)
		rmUserData    = flagIsSet(c, rmUserDataFlag)
		actValue      = &apc.ActValRmNode{DaemonID: daemonID, SkipRebalance: skipRebalance, NoShutdown: noShutdown}
	)
	if skipRebalance && node.IsTarget() {
		fmt.Fprintf(c.App.Writer,
			"Beware: performing %q and _not_ running global rebalance may lead to a loss of data!\n", action)
		fmt.Fprintln(c.App.Writer,
			"To rebalance the cluster manually at a later time, please run: `ais job start rebalance`")
	}
	if action == subcmdNodeDecommission {
		if !flagIsSet(c, yesFlag) {
			warn := fmt.Sprintf("about to permanently decommission node %s. The operation cannot be undone!", name)
			if ok := confirm(c, "Proceed?", warn); !ok {
				return nil
			}
		}
		actValue.NoShutdown = noShutdown
		actValue.RmUserData = rmUserData
	} else {
		const fmterr = "option '--%s' is valid only for decommissioning\n"
		if noShutdown {
			return fmt.Errorf(fmterr, noShutdownFlag.Name)
		}
		if rmUserData {
			return fmt.Errorf(fmterr, rmUserDataFlag.Name)
		}
	}
	switch action {
	case subcmdStartMaint:
		xactID, err = api.StartMaintenance(apiBP, actValue)
	case subcmdStopMaint:
		xactID, err = api.StopMaintenance(apiBP, actValue)
	case subcmdNodeDecommission:
		xactID, err = api.DecommissionNode(apiBP, actValue)
	case subcmdShutdown:
		xactID, err = api.ShutdownNode(apiBP, actValue)
	}
	if err != nil {
		return err
	}
	if xactID != "" {
		fmt.Fprintf(c.App.Writer, fmtRebalanceStarted, xactID, xactID)
	}
	switch action {
	case subcmdStopMaint:
		fmt.Fprintf(c.App.Writer, "%s is now active (maintenance done)\n", name)
	case subcmdNodeDecommission:
		if skipRebalance || node.IsProxy() {
			fmt.Fprintf(c.App.Writer, "%s has been decommissioned (permanently removed from the cluster)\n", name)
		} else {
			fmt.Fprintf(c.App.Writer,
				"%s is being decommissioned, please wait for cluster rebalancing to finish...\n", name)
		}
	case subcmdShutdown:
		if skipRebalance || node.IsProxy() {
			fmt.Fprintf(c.App.Writer, "%s has been shutdown\n", name)
		} else {
			fmt.Fprintf(c.App.Writer,
				"%s is shutting down, please wait for cluster rebalancing to finish...\n", name)
		}
	case subcmdStartMaint:
		fmt.Fprintf(c.App.Writer, "%s is now in maintenance\n", name)
	}
	return nil
}

func setPrimaryHandler(c *cli.Context) error {
	daemonID := argDaemonID(c.Args().First())
	if daemonID == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	primarySmap, err := fillMap()
	if err != nil {
		return err
	}
	if _, ok := primarySmap.Pmap[daemonID]; !ok {
		return incorrectUsageMsg(c, "%s: is not a proxy", daemonID)
	}
	err = api.SetPrimaryProxy(apiBP, daemonID, false /*force*/)
	if err == nil {
		fmt.Fprintf(c.App.Writer, "%s is now a new primary\n", daemonID)
	}
	return err
}

func startClusterRebalanceHandler(c *cli.Context) (err error) {
	return startXactionKindHandler(c, apc.ActRebalance)
}

func stopClusterRebalanceHandler(c *cli.Context) error {
	xactArgs := api.XactReqArgs{Kind: apc.ActRebalance, OnlyRunning: true}
	snap, err := getXactSnap(xactArgs)
	if err != nil {
		return err
	}
	if snap == nil {
		return errors.New("rebalance is not running")
	}

	xactArgs.ID, xactArgs.OnlyRunning = snap.ID, false
	if err := api.AbortXaction(apiBP, xactArgs); err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "Stopped %s[%s]\n", apc.ActRebalance, snap.ID)
	return nil
}

func showClusterRebalanceHandler(c *cli.Context) error {
	var (
		id       = c.Args().Get(0)
		daemonID = c.Args().Get(1)
	)
	if daemonID == "" && id != "" {
		if strings.HasPrefix(id, cluster.TnamePrefix) {
			daemonID, id = argDaemonID(id), ""
		}
	}
	xactArgs := api.XactReqArgs{
		ID:          id,
		Kind:        apc.ActRebalance,
		DaemonID:    daemonID,
		OnlyRunning: !flagIsSet(c, allXactionsFlag),
	}
	return xactList(c, xactArgs, "")
}
