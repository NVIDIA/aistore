// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

const (
	fmtRebalanceStarted = "Started rebalance %q (to monitor, run 'ais show rebalance').\n"

	roleProxyShort  = "p"
	roleTargetShort = "t"
)

// (compare with getLogUsage)
const getCluLogsUsage = "download log archives from all clustered nodes (one TAR.GZ per node), e.g.:\n" +
	indent4 + "\t - 'download-logs /tmp/www' - save log archives to /tmp/www directory\n" +
	indent4 + "\t - 'download-logs --severity w' - errors and warnings to /tmp directory\n" +
	indent4 + "\t   (see related: 'ais log show', 'ais log get')"

const shutdownUsage = "shutdown a node, gracefully or immediately;\n" +
	indent4 + "\tnote: upon shutdown the node won't be decommissioned - it'll remain in the cluster map\n" +
	indent4 + "\tand can be manually restarted to rejoin the cluster at any later time;\n" +
	indent4 + "\tsee also: 'ais advanced " + cmdRmSmap + "'"

var (
	clusterCmdsFlags = map[string][]cli.Flag{
		cmdCluAttach: {},
		cmdCluDetach: {},
		cmdCluConfig: {
			transientFlag,
		},
		cmdShutdown: {
			yesFlag,
		},
		cmdPrimary: {
			forceFlag,
		},
		cmdJoin: {
			roleFlag,
		},
		cmdStartMaint: {
			noRebalanceFlag,
			yesFlag,
		},
		cmdStopMaint: {
			noRebalanceFlag,
			yesFlag,
		},
		cmdShutdown + ".node": {
			noRebalanceFlag,
			rmUserDataFlag,
			yesFlag,
		},
		cmdNodeDecommission + ".node": {
			noRebalanceFlag,
			noShutdownFlag,
			rmUserDataFlag,
			keepInitialConfigFlag,
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
				Name:  cmdRebalance,
				Usage: "administratively start and stop global rebalance; show global rebalance",
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
				BashComplete: suggestProxies,
			},
			{
				Name:      cmdDownloadLogs,
				Usage:     getCluLogsUsage,
				ArgsUsage: "[OUT_DIR]",
				Flags:     []cli.Flag{logSevFlag},
				Action:    downloadAllLogs,
			},

			// cluster level (compare with the below)
			{
				Name:   cmdShutdown,
				Usage:  "shut down entire cluster",
				Flags:  clusterCmdsFlags[cmdShutdown],
				Action: clusterShutdownHandler,
			},
			{
				Name:   cmdClusterDecommission,
				Usage:  "decommission entire cluster",
				Flags:  clusterCmdsFlags[cmdClusterDecommission],
				Action: clusterDecommissionHandler,
			},
			// node level
			{
				Name:  cmdMembership,
				Usage: "manage cluster membership (add/remove nodes, temporarily or permanently)",
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
						Usage:        "take node out of maintenance mode - activate",
						ArgsUsage:    nodeIDArgument,
						Flags:        clusterCmdsFlags[cmdStopMaint],
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestNodesInMaint,
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
						Usage:        shutdownUsage,
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

// (compare with node-level `nodeMaintShutDecommHandler` operations)

func clusterShutdownHandler(c *cli.Context) (err error) {
	smap, err := getClusterMap(c)
	if err != nil {
		return err
	}
	if !flagIsSet(c, yesFlag) {
		warn := fmt.Sprintf("shutting down cluster (UUID=%s, primary=[%s, %s])",
			smap.UUID, smap.Primary.ID(), smap.Primary.PubNet.URL)
		actionWarn(c, warn)
		if ok := confirm(c, "Proceed?"); !ok {
			return nil
		}
	}
	if err := api.ShutdownCluster(apiBP); err != nil {
		return V(err)
	}
	actionDone(c, "Cluster successfully shut down")
	return
}

func clusterDecommissionHandler(c *cli.Context) error {
	smap, err := getClusterMap(c)
	if err != nil {
		return err
	}
	if !flagIsSet(c, yesFlag) {
		warn := fmt.Sprintf("about to permanently decommission cluster (UUID=%s, primary=[%s, %s]).",
			smap.UUID, smap.Primary.ID(), smap.Primary.PubNet.URL)
		actionWarn(c, warn)
		if ok := confirm(c, "The operation cannot be undone. Proceed?"); !ok {
			return nil
		}
	}
	rmUserData := flagIsSet(c, rmUserDataFlag)
	if err := api.DecommissionCluster(apiBP, rmUserData); err != nil {
		return V(err)
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
		return errors.New("invalid address, expecting 'IPv4:PORT'")
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
	netInfo := meta.NetInfo{
		Hostname: addrParts[0],
		Port:     addrParts[1],
		URL:      prefix + addr,
	}
	nodeInfo := &meta.Snode{
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
	_, sname, err = getNode(c, nodeInfo.DaeID)
	if err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "%s successfully joined the cluster\n", sname)

	if rebID != "" {
		fmt.Fprintf(c.App.Writer, fmtRebalanceStarted, rebID)
	}
	return
}

// (compare w/ cluster-level clusterDecommissionHandler & clusterShutdownHandler)
func nodeMaintShutDecommHandler(c *cli.Context) error {
	if c.NArg() < 1 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	smap, err := getClusterMap(c)
	if err != nil {
		return err
	}
	node, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	action := c.Command.Name
	if smap.IsPrimary(node) {
		return fmt.Errorf("%s is primary (cannot %s the primary node)", sname, action)
	}
	var (
		xid               string
		skipRebalance     = flagIsSet(c, noRebalanceFlag) || node.IsProxy()
		noShutdown        = flagIsSet(c, noShutdownFlag)
		rmUserData        = flagIsSet(c, rmUserDataFlag)
		keepInitialConfig = flagIsSet(c, keepInitialConfigFlag)
		actValue          = &apc.ActValRmNode{
			DaemonID:      node.ID(),
			SkipRebalance: skipRebalance,
			NoShutdown:    noShutdown,
		}
	)
	if skipRebalance && node.IsTarget() {
		warn := fmt.Sprintf("executing %q _and_ not running global rebalance may lead to a loss of data!", action)
		actionWarn(c, warn)
		fmt.Fprintln(c.App.Writer,
			"To rebalance the cluster manually at a later time, run: `ais start rebalance`")
	}
	if action == cmdNodeDecommission {
		actValue.NoShutdown = noShutdown
		actValue.RmUserData = rmUserData
		actValue.KeepInitialConfig = keepInitialConfig
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
		if !flagIsSet(c, yesFlag) {
			warn := fmt.Sprintf("about to put %s in maintenance mode", sname)
			if ok := confirm(c, "Proceed?", warn); !ok {
				return nil
			}
		}
		xid, err = api.StartMaintenance(apiBP, actValue)
	case cmdStopMaint:
		if !flagIsSet(c, yesFlag) {
			prompt := fmt.Sprintf("Take %s out of maintenance mode", sname)
			if ok := confirm(c, prompt); !ok {
				return nil
			}
		}
		xid, err = api.StopMaintenance(apiBP, actValue)
	case cmdNodeDecommission:
		if !flagIsSet(c, yesFlag) {
			warn := "about to permanently decommission " + sname + ". The operation cannot be undone!"
			if ok := confirm(c, "Proceed?", warn); !ok {
				return nil
			}
		}
		xid, err = api.DecommissionNode(apiBP, actValue)
	case cmdShutdown:
		if !flagIsSet(c, yesFlag) {
			prompt := "Shut down " + sname
			if ok := confirm(c, prompt); !ok {
				return nil
			}
		}
		xid, err = api.ShutdownNode(apiBP, actValue)
	}
	if err != nil {
		return err
	}
	if xid != "" {
		fmt.Fprintf(c.App.Writer, fmtRebalanceStarted, xid)
	}
	switch action {
	case cmdStopMaint:
		fmt.Fprintf(c.App.Writer, "%s is now active\n", sname)
	case cmdNodeDecommission:
		if skipRebalance || node.IsProxy() {
			fmt.Fprintf(c.App.Writer, "%s has been decommissioned (permanently removed from the cluster)\n", sname)
		} else {
			fmt.Fprintf(c.App.Writer,
				"%s is being decommissioned, please wait for cluster rebalancing to finish...\n", sname)
		}
	case cmdShutdown:
		if skipRebalance || node.IsProxy() {
			fmt.Fprintf(c.App.Writer, "%s has been shut down\n", sname)
		} else {
			fmt.Fprintf(c.App.Writer,
				"%s is shutting down, please wait for cluster rebalancing to finish\n", sname)
		}
		fmt.Fprintf(c.App.Writer, "\nNote: the node %s is _not_ decommissioned - it remains in the cluster map and can be manually\n", sname)
		fmt.Fprintf(c.App.Writer, "restarted at any later time (and subsequently activated via '%s' operation).\n", cmdStopMaint)
	case cmdStartMaint:
		fmt.Fprintf(c.App.Writer, "%s is now in maintenance mode\n", sname)
	}
	return nil
}

func setPrimaryHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	node, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	if !node.IsProxy() {
		return incorrectUsageMsg(c, "%s is not a proxy", sname)
	}

	switch {
	case node.Flags.IsSet(meta.SnodeMaint):
		return fmt.Errorf("%s is currently in maintenance", sname)
	case node.Flags.IsSet(meta.SnodeDecomm):
		return fmt.Errorf("%s is currently being decommissioned", sname)
	case node.Flags.IsSet(meta.SnodeNonElectable):
		return fmt.Errorf("%s is non-electable", sname)
	}

	err = api.SetPrimaryProxy(apiBP, node.ID(), flagIsSet(c, forceFlag))
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
	_, snap, err := getAnyXactSnap(&xargs)
	if err != nil {
		return err
	}
	if snap == nil {
		return errors.New("rebalance is not running")
	}

	xargs.ID, xargs.OnlyRunning = snap.ID, false
	if err := api.AbortXaction(apiBP, &xargs); err != nil {
		return V(err)
	}
	fmt.Fprintf(c.App.Writer, "Stopped %s[%s]\n", apc.ActRebalance, snap.ID)
	return nil
}

func showClusterRebalanceHandler(c *cli.Context) error {
	var (
		xid      = c.Args().Get(0)
		daemonID = c.Args().Get(1)
	)
	if daemonID == "" && xid != "" {
		// either/or
		if strings.HasPrefix(xid, meta.PnamePrefix) {
			return fmt.Errorf("%s appears to be a 'proxy' (expecting 'target' or empty)", xid)
		}
		if strings.HasPrefix(xid, meta.TnamePrefix) {
			node, _, err := getNode(c, xid)
			if err != nil {
				return err
			}
			daemonID, xid = node.ID(), ""
		}
	}
	xargs := xact.ArgsMsg{
		ID:          xid,
		Kind:        apc.ActRebalance,
		DaemonID:    daemonID,
		OnlyRunning: !flagIsSet(c, allJobsFlag),
	}
	_, err := xactList(c, &xargs, false)
	return err
}

func resetStatsHandler(c *cli.Context) error {
	var (
		errorsOnly       = flagIsSet(c, errorsOnlyFlag)
		tag              = "stats"
		node, sname, err = arg0Node(c)
	)
	if err != nil {
		return err
	}
	if errorsOnly {
		tag = "error metrics"
	}
	// 1. node
	if node != nil {
		if err := api.ResetDaemonStats(apiBP, node, errorsOnly); err != nil {
			return V(err)
		}
		msg := fmt.Sprintf("%s %s successfully reset", sname, tag)
		actionDone(c, msg)
		return nil
	}
	// 2. or cluster
	if err := api.ResetClusterStats(apiBP, errorsOnly); err != nil {
		return V(err)
	}
	msg := fmt.Sprintf("Cluster %s successfully reset", tag)
	actionDone(c, msg)
	return nil
}

func downloadAllLogs(c *cli.Context) error {
	sev, err := parseLogSev(c)
	if err != nil {
		return err
	}
	outFile := c.Args().Get(0)
	err = _getAllClusterLogs(c, sev, outFile)
	if err == nil {
		actionDone(c, "Done")
	}
	return err
}
