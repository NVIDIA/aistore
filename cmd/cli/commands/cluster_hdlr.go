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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

const (
	fmtRebalanceStarted = "Started rebalance %q, use 'ais show job xaction %s' to monitor progress\n"

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
		subcmdShutdown:  {},
		subcmdRebalance: {},
		subcmdPrimary:   {},
		subcmdJoin: {
			roleFlag,
		},
		subcmdStartMaint: {
			noRebalanceFlag,
		},
		subcmdDecommission: {
			noRebalanceFlag,
			noShutdownFlag,
			dontRmUserDataFlag,
		},
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
				Name:   subcmdRebalance,
				Usage:  "rebalance ais cluster",
				Flags:  clusterCmdsFlags[subcmdRebalance],
				Action: startXactionHandler,
			},
			{
				Name:         subcmdPrimary,
				Usage:        "select a new primary proxy/gateway",
				ArgsUsage:    daemonIDArgument,
				Flags:        clusterCmdsFlags[subcmdPrimary],
				Action:       setPrimaryHandler,
				BashComplete: daemonCompletions(completeProxies),
			},
			{
				Name:   subcmdShutdown,
				Usage:  "shutdown cluster",
				Flags:  clusterCmdsFlags[subcmdShutdown],
				Action: clusterShutdownHandler,
			},
			{
				Name:   subcmdDecommission,
				Usage:  "decommission entire cluster",
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
						Action:       maintShutDecommHandler,
						BashComplete: daemonCompletions(completeAllDaemons),
					},
					{
						Name:         subcmdStopMaint,
						Usage:        "activate node by taking it back from \"maintenance\"",
						ArgsUsage:    daemonIDArgument,
						Action:       maintShutDecommHandler,
						BashComplete: daemonCompletions(completeAllDaemons),
					},
					{
						Name:         subcmdDecommission,
						Usage:        "safely and permanently remove node from the cluster",
						ArgsUsage:    daemonIDArgument,
						Flags:        clusterCmdsFlags[subcmdDecommission],
						Action:       maintShutDecommHandler,
						BashComplete: daemonCompletions(completeAllDaemons),
					},
					{
						Name:         subcmdShutdown,
						Usage:        "shutdown a node",
						ArgsUsage:    daemonIDArgument,
						Flags:        clusterCmdsFlags[subcmdDecommission],
						Action:       maintShutDecommHandler,
						BashComplete: daemonCompletions(completeAllDaemons),
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

func clusterShutdownHandler(c *cli.Context) (err error) {
	if err := api.ShutdownCluster(defaultAPIParams); err != nil {
		return err
	}

	fmt.Fprint(c.App.Writer, "All nodes in the cluster are being shut down.\n")
	return
}

func clusterDecommissionHandler(c *cli.Context) (err error) {
	if err := api.DecommissionCluster(defaultAPIParams); err != nil {
		return err
	}

	fmt.Fprint(c.App.Writer, "All nodes in the cluster are being decommissioned.\n")
	return
}

func joinNodeHandler(c *cli.Context) (err error) {
	const ()
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
	case cmn.Proxy, roleProxyShort:
		daemonType = cmn.Proxy
	case cmn.Target, roleTargetShort:
		daemonType = cmn.Target
	default:
		return fmt.Errorf("invalid daemon role, one of %q, %q, %q, %q expected",
			cmn.Proxy, cmn.Target, roleProxyShort, roleTargetShort)
	}

	prefix = getPrefixFromPrimary()
	netInfo := cluster.NetInfo{
		NodeHostname: socketAddrParts[0],
		DaemonPort:   socketAddrParts[1],
		DirectURL:    prefix + socketAddr,
	}
	nodeInfo := &cluster.Snode{
		DaemonID:        "", // id is generated by the daemon, see ais.initDaemonID()
		DaemonType:      daemonType,
		PublicNet:       netInfo,
		IntraControlNet: netInfo,
		IntraDataNet:    netInfo,
	}
	if rebID, nodeInfo.DaemonID, err = api.JoinCluster(defaultAPIParams, nodeInfo); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%s %s successfully joined the cluster\n",
		cos.CapitalizeString(daemonType), nodeInfo.Name())

	if rebID != "" {
		fmt.Fprintf(c.App.Writer, fmtRebalanceStarted, rebID, rebID)
	}
	return
}

func maintShutDecommHandler(c *cli.Context) (err error) {
	if c.NArg() < 1 {
		return missingArgumentsError(c, "daemon ID")
	}
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return err
	}
	sid := c.Args().First()
	action := c.Command.Name
	node := smap.GetNode(sid)
	if node == nil {
		return fmt.Errorf("node %q does not exist", sid)
	}
	if smap.IsPrimary(node) {
		return fmt.Errorf("node %q is primary, cannot %s", sid, action)
	}

	var (
		xactID         string
		skipRebalance  = flagIsSet(c, noRebalanceFlag) || node.IsProxy()
		noShutdown     = flagIsSet(c, noShutdownFlag)
		dontRmUserData = flagIsSet(c, dontRmUserDataFlag)
		actValue       = &cmn.ActValRmNode{DaemonID: sid, SkipRebalance: skipRebalance, NoShutdown: noShutdown}
	)
	if skipRebalance && node.IsTarget() {
		fmt.Fprintf(c.App.Writer,
			"Beware: performing %q and _not_ running global rebalance may lead to a loss of data!\n", action)
		fmt.Fprintln(c.App.Writer,
			"To rebalance the cluster manually at a later time, please run: `ais job start rebalance`")
	}
	if noShutdown && action != subcmdDecommission {
		fmt.Fprintf(c.App.Writer, "Warning: option `--%s` is valid only for %q and will be ignored\n",
			noShutdownFlag.Name, subcmdDecommission)
		noShutdown = false
	}
	if dontRmUserData && action != subcmdDecommission {
		fmt.Fprintf(c.App.Writer, "Warning: option `--%s` is valid only for %q and will be ignored\n",
			dontRmUserDataFlag.Name, subcmdDecommission)
		dontRmUserData = false
	}
	actValue.NoShutdown = noShutdown
	actValue.RmUserData = !dontRmUserData

	switch action {
	case subcmdStartMaint:
		xactID, err = api.StartMaintenance(defaultAPIParams, actValue)
	case subcmdStopMaint:
		xactID, err = api.StopMaintenance(defaultAPIParams, actValue)
	case subcmdDecommission:
		xactID, err = api.DecommissionNode(defaultAPIParams, actValue)
	case subcmdShutdown:
		xactID, err = api.ShutdownNode(defaultAPIParams, actValue)
	}
	if err != nil {
		return err
	}
	if xactID != "" {
		fmt.Fprintf(c.App.Writer, fmtRebalanceStarted, xactID, xactID)
	}
	if action == subcmdStopMaint {
		fmt.Fprintf(c.App.Writer, "Node %q is now active (maintenance done)\n", sid)
	} else if action == subcmdDecommission && skipRebalance {
		fmt.Fprintf(c.App.Writer, "Node %q has been decommissioned - permanently removed from the cluster\n", sid)
	} else if action == subcmdShutdown && skipRebalance {
		fmt.Fprintf(c.App.Writer, "Node %q has been shutdown\n", sid)
	} else if action == subcmdStartMaint {
		fmt.Fprintf(c.App.Writer, "Node %q is now under maintenance\n", sid)
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
