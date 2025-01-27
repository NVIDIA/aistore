// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
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
const getCluLogsUsage = "Download log archives from all clustered nodes (one TAR.GZ per node), e.g.:\n" +
	indent4 + "\t - 'download-logs /tmp/www' - save log archives to /tmp/www directory\n" +
	indent4 + "\t - 'download-logs --severity w' - errors and warnings to /tmp directory\n" +
	indent4 + "\t   (see related: 'ais log show', 'ais log get')"

const shutdownUsage = "Shutdown a node, gracefully or immediately;\n" +
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
			nonElectableFlag,
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
		Name:      commandStart,
		Usage:     jobStartRebalance.Usage,
		ArgsUsage: jobStartRebalance.ArgsUsage,
		Flags:     sortFlags(jobStartRebalance.Flags),
		Action:    jobStartRebalance.Action,
	}
	stopRebalance = cli.Command{
		Name:   commandStop,
		Usage:  "Stop rebalancing ais cluster",
		Flags:  sortFlags(clusterCmdsFlags[commandStop]),
		Action: stopRebHandler,
	}

	clusterCmd = cli.Command{
		Name:  commandCluster,
		Usage: "Monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc.",
		Subcommands: []cli.Command{
			makeAlias(showCmdCluster, "", true, commandShow), // alias for `ais show`
			{
				Name:      cmdCluAttach,
				Usage:     "Attach remote ais cluster",
				ArgsUsage: attachRemoteAISArgument,
				Flags:     sortFlags(clusterCmdsFlags[cmdAttach]),
				Action:    attachRemoteAISHandler,
			},
			{
				Name:         cmdCluDetach,
				Usage:        "Detach remote ais cluster",
				ArgsUsage:    detachRemoteAISArgument,
				Flags:        sortFlags(clusterCmdsFlags[cmdDetach]),
				Action:       detachRemoteAISHandler,
				BashComplete: suggestRemote,
			},
			{
				Name:  cmdRebalance,
				Usage: "Administratively start and stop global rebalance; show global rebalance",
				Subcommands: []cli.Command{
					startRebalance,
					stopRebalance,
					{
						Name:         commandShow,
						Usage:        "Show global rebalance",
						Flags:        sortFlags(clusterCmdsFlags[commandShow]),
						BashComplete: rebalanceCompletions,
						Action:       showClusterRebalanceHandler,
					},
				},
			},
			{
				Name:         cmdPrimary,
				Usage:        "Select a new primary proxy/gateway",
				ArgsUsage:    nodeIDArgument + " [URL]",
				Flags:        sortFlags(clusterCmdsFlags[cmdPrimary]),
				Action:       setPrimaryHandler,
				BashComplete: suggestProxies,
			},
			{
				Name:      cmdDownloadLogs,
				Usage:     getCluLogsUsage,
				ArgsUsage: "[OUT_DIR]",
				Flags:     sortFlags([]cli.Flag{logSevFlag}),
				Action:    downloadAllLogs,
			},

			// cluster level (compare with the below)
			{
				Name:   cmdShutdown,
				Usage:  "Shut down entire cluster",
				Flags:  sortFlags(clusterCmdsFlags[cmdShutdown]),
				Action: clusterShutdownHandler,
			},
			{
				Name:   cmdClusterDecommission,
				Usage:  "Decommission entire cluster",
				Flags:  sortFlags(clusterCmdsFlags[cmdClusterDecommission]),
				Action: clusterDecommissionHandler,
			},
			// node level
			{
				Name:  cmdMembership,
				Usage: "Manage cluster membership (add/remove nodes, temporarily or permanently)",
				Subcommands: []cli.Command{
					{
						Name:      cmdJoin,
						Usage:     "Add a node to the cluster",
						ArgsUsage: joinNodeArgument,
						Flags:     sortFlags(clusterCmdsFlags[cmdJoin]),
						Action:    joinNodeHandler,
					},
					{
						Name:         cmdStartMaint,
						Usage:        "Put node in maintenance mode, temporarily suspend its operation",
						ArgsUsage:    nodeIDArgument,
						Flags:        sortFlags(clusterCmdsFlags[cmdStartMaint]),
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
					{
						Name:         cmdStopMaint,
						Usage:        "Take node out of maintenance mode - activate",
						ArgsUsage:    nodeIDArgument,
						Flags:        sortFlags(clusterCmdsFlags[cmdStopMaint]),
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestNodesInMaint,
					},
					{
						Name:         cmdNodeDecommission,
						Usage:        "Safely and permanently remove node from the cluster",
						ArgsUsage:    nodeIDArgument,
						Flags:        sortFlags(clusterCmdsFlags[cmdNodeDecommission+".node"]),
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
					{
						Name:         cmdShutdown,
						Usage:        shutdownUsage,
						ArgsUsage:    nodeIDArgument,
						Flags:        sortFlags(clusterCmdsFlags[cmdShutdown+".node"]),
						Action:       nodeMaintShutDecommHandler,
						BashComplete: suggestAllNodes,
					},
				},
			},
			{
				Name:         cmdResetStats,
				Usage:        "Reset cluster or node stats (all cumulative metrics or only errors)",
				ArgsUsage:    optionalNodeIDArgument,
				Flags:        sortFlags(clusterCmdsFlags[cmdResetStats]),
				Action:       resetStatsHandler,
				BashComplete: suggestAllNodes,
			},
			{
				Name:         cmdReloadCreds,
				Usage:        "Reload (updated) backend credentials",
				ArgsUsage:    "[PROVIDER]",
				Action:       reloadCredsHandler,
				BashComplete: suggestProvider,
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
		if !confirm(c, "Proceed?") {
			return nil
		}
		// on the off-chance anything changed during 'confirm' interaction
		curSmap = nil
		smap, err = getClusterMap(c)
		if err != nil {
			return err
		}
	}

	// [NOTE]
	// - cluster (shutdown|decommission) via non-primary works as well
	// - still, _primary_ would be a better choice
	bp := apiBP
	bp.URL = smap.Primary.PubNet.URL
	if err := api.ShutdownCluster(bp); err != nil {
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
		if !confirm(c, "The operation cannot be undone. Proceed?") {
			return nil
		}
		curSmap = nil
		smap, err = getClusterMap(c)
		if err != nil {
			return err
		}
	}
	rmUserData := flagIsSet(c, rmUserDataFlag)

	// [NOTE] ditto (see above)
	bp := apiBP
	bp.URL = smap.Primary.PubNet.URL

	if err := api.DecommissionCluster(bp, rmUserData); err != nil {
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

	var flags cos.BitFlags
	if flagIsSet(c, nonElectableFlag) {
		if daemonType == apc.Target {
			return fmt.Errorf("option %s does not apply - targets are non-electable", qflprn(nonElectableFlag))
		}
		flags = meta.SnodeNonElectable
	}
	if rebID, nodeInfo.DaeID, err = api.JoinCluster(apiBP, nodeInfo, flags); err != nil {
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
			if !confirm(c, "Proceed?", warn) {
				return nil
			}
		}
		xid, err = api.StartMaintenance(apiBP, actValue)
	case cmdStopMaint:
		if !flagIsSet(c, yesFlag) {
			prompt := fmt.Sprintf("Take %s out of maintenance mode", sname)
			if !confirm(c, prompt) {
				return nil
			}
		}
		xid, err = api.StopMaintenance(apiBP, actValue)
	case cmdNodeDecommission:
		if !flagIsSet(c, yesFlag) {
			warn := "about to permanently decommission " + sname + ". The operation cannot be undone!"
			if !confirm(c, "Proceed?", warn) {
				return nil
			}
		}
		xid, err = api.DecommissionNode(apiBP, actValue)
	case cmdShutdown:
		if !flagIsSet(c, yesFlag) {
			prompt := "Shut down " + sname
			if !confirm(c, prompt) {
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
	var (
		toID  = c.Args().Get(0)
		toURL = c.Args().Get(1)
		force = flagIsSet(c, forceFlag)
	)
	if force {
		err := api.SetPrimary(apiBP, toID, toURL, force)
		if err == nil {
			var s string
			if toURL != "" {
				s = " (at " + toURL + ")"
			}
			actionDone(c, fmt.Sprintf("%s%s is now a new primary", toID, s))
		}
		return err
	}

	node, sname, err := getNode(c, toID)
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

	err = api.SetPrimary(apiBP, toID, toURL, force)
	if err == nil {
		actionDone(c, sname+" is now a new primary")
	}
	return err
}

func startRebHandler(c *cli.Context) (err error) {
	var (
		extra, prefix string
		xargs         = xact.ArgsMsg{Kind: apc.ActRebalance}
	)
	if flagIsSet(c, verbObjPrefixFlag) {
		prefix = parseStrFlag(c, verbObjPrefixFlag)
	}
	if c.NArg() > 0 {
		uri := preparseBckObjURI(c.Args().Get(0))
		bck, pref, err := parseBckObjURI(c, uri, true /*emptyObjnameOK*/)
		if err != nil {
			return err
		}
		if _, err := headBucket(bck, false /* don't add */); err != nil {
			return err
		}
		xargs.Bck = bck

		// embedded prefix vs '--prefix'
		switch {
		case pref != "" && prefix != "":
			s := fmt.Sprintf(": via '%s' and %s option", uri, qflprn(verbObjPrefixFlag))
			if pref != prefix {
				return errors.New("two different prefix values" + s)
			}
			actionWarn(c, "redundant and duplicated prefix assignment"+s)
		case pref != "":
			prefix = pref
		}
	}
	if xargs.Bck.IsEmpty() && prefix != "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if !xargs.Bck.IsEmpty() {
		extra = prefix
		actionWarn(c, "limiting the scope of rebalance to only '"+xargs.Bck.Cname(extra)+"' is not recommended!")
		briefPause(2)
	}
	return startXaction(c, &xargs, extra)
}

func stopRebHandler(c *cli.Context) error {
	xargs := xact.ArgsMsg{Kind: apc.ActRebalance, OnlyRunning: true}
	_, snap, err := getAnyXactSnap(&xargs)
	if err != nil {
		return err
	}
	if snap == nil {
		return errors.New("rebalance is not running")
	}
	return stopReb(c, snap.ID)
}

func stopReb(c *cli.Context, xid string) error {
	xargs := xact.ArgsMsg{Kind: apc.ActRebalance, ID: xid, Force: flagIsSet(c, forceFlag)}
	if err := xstop(&xargs); err != nil {
		return V(err)
	}
	actionDone(c, fmt.Sprintf("Stopped %s[%s]\n", apc.ActRebalance, xid))
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

func reloadCredsHandler(c *cli.Context) error {
	p := c.Args().Get(0)
	if p == scopeAll {
		p = ""
	}
	return api.ReloadBackendCreds(apiBP, p)
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
