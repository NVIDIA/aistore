// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains implementation of the top-level `show` command.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

type (
	daemonTemplateXactSnaps struct {
		DaemonID  string
		XactSnaps []*xact.SnapExt
	}

	targetMpath struct {
		DaemonID string
		Mpl      *apc.MountpathList
	}
)

var (
	showCmdsFlags = map[string][]cli.Flag{
		subcmdShowStorage: append(
			longRunFlags,
			jsonFlag,
		),
		subcmdShowDisk: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
		),
		subcmdShowMpath: append(
			longRunFlags,
			jsonFlag,
		),
		subcmdShowDownload: {
			regexFlag,
			progressBarFlag,
			refreshFlag,
			verboseFlag,
		},
		subcmdShowDsort: {
			regexFlag,
			refreshFlag,
			verboseFlag,
			logFlag,
			jsonFlag,
		},
		subcmdShowObject: {
			objPropsFlag,
			allPropsFlag,
			objNotCachedFlag,
			noHeaderFlag,
			jsonFlag,
		},
		subcmdShowCluster: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
		),
		subcmdSmap: append(
			longRunFlags,
			jsonFlag,
		),
		subcmdBMD: {
			jsonFlag,
		},
		subcmdShowXaction: {
			jsonFlag,
			allXactionsFlag,
			activeFlag,
			noHeaderFlag,
			verboseFlag,
		},
		subcmdShowRebalance: {
			refreshFlag,
			allXactionsFlag,
			noHeaderFlag,
		},
		subcmdShowBucket: {
			jsonFlag,
			compactPropFlag,
		},
		subcmdShowConfig: {
			jsonFlag,
		},
		subcmdShowRemoteAIS: {
			noHeaderFlag,
			verboseFlag,
			jsonFlag,
		},
		subcmdShowLog: {
			logSevFlag,
		},
		subcmdShowClusterStats: {
			jsonFlag,
			rawFlag,
			refreshFlag,
		},
	}

	showCmd = cli.Command{
		Name:  commandShow,
		Usage: "show configuration, buckets, jobs, etc. - all managed entities in the cluster, and the cluster itself",
		Subcommands: []cli.Command{
			makeAlias(authCmdShow, "", true, commandAuth), // alias for `ais auth show`
			showCmdObject,
			showCmdCluster,
			showCmdRebalance,
			showCmdBucket,
			showCmdConfig,
			showCmdRemoteAIS,
			showCmdStorage,
			showCmdJob,
			showCmdLog,
		},
	}

	showCmdStorage = cli.Command{
		Name:      subcmdShowStorage,
		Usage:     "show storage usage and utilization, disks and mountpaths",
		ArgsUsage: "[TARGET_ID]",
		Flags:     showCmdsFlags[subcmdShowStorage],
		Action:    showStorageHandler,
		BashComplete: func(c *cli.Context) {
			if c.NArg() == 0 {
				fmt.Printf("%s\n%s\n%s\n", subcmdShowDisk, subcmdShowMpath, subcmdStgSummary)
			}
			daemonCompletions(completeTargets)(c)
		},
		Subcommands: []cli.Command{
			showCmdDisk,
			showCmdMpath,
			showCmdStgSummary,
		},
	}
	showCmdObject = cli.Command{
		Name:         subcmdShowObject,
		Usage:        "show object details",
		ArgsUsage:    objectArgument,
		Flags:        showCmdsFlags[subcmdShowObject],
		Action:       showObjectHandler,
		BashComplete: bucketCompletions(bckCompletionsOpts{separator: true}),
	}
	showCmdCluster = cli.Command{
		Name:      subcmdShowCluster,
		Usage:     "show cluster details",
		ArgsUsage: "[DAEMON_ID|DAEMON_TYPE|smap|bmd|config|stats]",
		Flags:     showCmdsFlags[subcmdShowCluster],
		Action:    showClusterHandler,
		BashComplete: func(c *cli.Context) {
			if c.NArg() == 0 {
				fmt.Printf("%s\n%s\n%s\n%s\n%s\n%s\n",
					apc.Proxy, apc.Target, subcmdSmap, subcmdBMD, subcmdConfig, subcmdShowClusterStats)
			}
			daemonCompletions(completeAllDaemons)(c)
		},
		Subcommands: []cli.Command{
			{
				Name:         subcmdSmap,
				Usage:        "show Smap (cluster map)",
				ArgsUsage:    optionalDaemonIDArgument,
				Flags:        showCmdsFlags[subcmdSmap],
				Action:       showSmapHandler,
				BashComplete: daemonCompletions(completeAllDaemons),
			},
			{
				Name:         subcmdBMD,
				Usage:        "show BMD (bucket metadata)",
				ArgsUsage:    optionalDaemonIDArgument,
				Flags:        showCmdsFlags[subcmdBMD],
				Action:       showBMDHandler,
				BashComplete: daemonCompletions(completeAllDaemons),
			},
			{
				Name:      subcmdShowConfig,
				Usage:     "show cluster configuration",
				ArgsUsage: showClusterConfigArgument,
				Flags:     showCmdsFlags[subcmdShowConfig],
				Action:    showClusterConfigHandler,
			},
			{
				Name:         subcmdShowClusterStats,
				Usage:        "show cluster statistics",
				ArgsUsage:    showStatsArgument,
				Flags:        showCmdsFlags[subcmdShowClusterStats],
				Action:       showClusterStatsHandler,
				BashComplete: daemonCompletions(completeAllDaemons),
			},
		},
	}
	showCmdRebalance = cli.Command{
		Name:      subcmdShowRebalance,
		Usage:     "show rebalance details",
		ArgsUsage: noArguments,
		Flags:     showCmdsFlags[subcmdShowRebalance],
		Action:    showRebalanceHandler,
	}
	showCmdBucket = cli.Command{
		Name:         subcmdShowBucket,
		Usage:        "show bucket properties",
		ArgsUsage:    bucketAndPropsArgument,
		Flags:        showCmdsFlags[subcmdShowBucket],
		Action:       showBckPropsHandler,
		BashComplete: bucketAndPropsCompletions,
	}
	showCmdConfig = cli.Command{
		Name:         subcmdShowConfig,
		Usage:        "show CLI, cluster, or node configurations (nodes inherit cluster and have local)",
		ArgsUsage:    showConfigArgument,
		Flags:        showCmdsFlags[subcmdShowConfig],
		Action:       showConfigHandler,
		BashComplete: showConfigCompletions,
	}
	showCmdRemoteAIS = cli.Command{
		Name:         subcmdShowRemoteAIS,
		Usage:        "show attached AIS clusters",
		ArgsUsage:    "",
		Flags:        showCmdsFlags[subcmdShowRemoteAIS],
		Action:       showRemoteAISHandler,
		BashComplete: daemonCompletions(completeTargets), // NOTE: not using remais.smap yet
	}

	showCmdLog = cli.Command{
		Name:         subcmdShowLog,
		Usage:        "show log",
		ArgsUsage:    daemonIDArgument,
		Flags:        showCmdsFlags[subcmdShowLog],
		Action:       showDaemonLogHandler,
		BashComplete: daemonCompletions(completeAllDaemons),
	}

	showCmdJob = cli.Command{
		Name:  subcmdShowJob,
		Usage: "show running and completed jobs (xactions)",
		Subcommands: []cli.Command{
			showCmdDownload,
			showCmdDsort,
			showCmdXaction,
			appendSubcommand(makeAlias(showCmdETL, "", true, commandETL), logsCmdETL),
		},
	}
	showCmdDownload = cli.Command{
		Name:         subcmdShowDownload,
		Usage:        "show active downloads",
		ArgsUsage:    optionalJobIDArgument,
		Flags:        showCmdsFlags[subcmdShowDownload],
		Action:       showDownloadsHandler,
		BashComplete: downloadIDAllCompletions,
	}
	showCmdDsort = cli.Command{
		Name:      subcmdShowDsort,
		Usage:     fmt.Sprintf("show information about %s jobs", dsort.DSortName),
		ArgsUsage: optionalJobIDDaemonIDArgument,
		Flags:     showCmdsFlags[subcmdShowDsort],
		Action:    showDsortHandler,
		BashComplete: func(c *cli.Context) {
			if c.NArg() == 0 {
				dsortIDAllCompletions(c)
			}
			if c.NArg() == 1 {
				daemonCompletions(completeTargets)(c)
			}
		},
	}
	showCmdXaction = cli.Command{
		Name:         subcmdShowXaction,
		Usage:        "show xaction details",
		ArgsUsage:    "[TARGET_ID] [XACTION_ID|XACTION_NAME] [BUCKET]",
		Description:  xactionDesc(false),
		Flags:        showCmdsFlags[subcmdShowXaction],
		Action:       showXactionHandler,
		BashComplete: daemonXactionCompletions,
	}

	// `show storage` sub-commands
	showCmdDisk = cli.Command{
		Name:         subcmdShowDisk,
		Usage:        "show disk utilization and read/write statistics",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showCmdsFlags[subcmdShowDisk],
		Action:       showDisksHandler,
		BashComplete: daemonCompletions(completeTargets),
	}
	showCmdStgSummary = cli.Command{
		Name:         subcmdStgSummary,
		Usage:        "show bucket sizes and %% of used capacity on a per-bucket basis",
		ArgsUsage:    listAnyCommandArgument,
		Flags:        storageCmdFlags[subcmdStgSummary],
		Action:       showBucketSummary,
		BashComplete: bucketCompletions(),
	}
	showCmdMpath = cli.Command{
		Name:         subcmdShowMpath,
		Usage:        "show target mountpaths",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showCmdsFlags[subcmdShowMpath],
		Action:       showMpathHandler,
		BashComplete: daemonCompletions(completeTargets),
	}
)

func showDisksHandler(c *cli.Context) (err error) {
	daemonID := argDaemonID(c)
	if _, err = fillMap(); err != nil {
		return
	}
	if err = updateLongRunParams(c); err != nil {
		return
	}
	return daemonDiskStats(c, daemonID)
}

func showDownloadsHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() < 1 { // list all download jobs
		return downloadJobsList(c, parseStrFlag(c, regexFlag))
	}

	// display status of a download job with given id
	return downloadJobStatus(c, id)
}

func showDsortHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() < 1 { // list all dsort jobs
		return dsortJobsList(c, parseStrFlag(c, regexFlag))
	}

	// display status of a dsort job with given id
	return dsortJobStatus(c, id)
}

func showClusterHandler(c *cli.Context) error {
	var (
		daemonID         = argDaemonID(c)
		primarySmap, err = fillMap()
	)
	if err != nil {
		return err
	}
	cluConfig, err := api.GetClusterConfig(apiBP)
	if err != nil {
		return err
	}
	if err := updateLongRunParams(c); err != nil {
		return err
	}
	return clusterDaemonStatus(c, primarySmap, cluConfig, daemonID, flagIsSet(c, jsonFlag), flagIsSet(c, noHeaderFlag))
}

func showStorageHandler(c *cli.Context) (err error) {
	if err = updateLongRunParams(c); err != nil {
		return
	}
	return showDisksHandler(c)
}

func showXactionHandler(c *cli.Context) (err error) {
	nodeID, xactID, xactKind, bck, errP := parseXactionFromArgs(c)
	if errP != nil {
		return errP
	}
	return _showXactList(c, nodeID, xactID, xactKind, bck)
}

func _showXactList(c *cli.Context, nodeID, xactID, xactKind string, bck cmn.Bck) (err error) {
	latest := !flagIsSet(c, allXactionsFlag)
	if xactID != "" {
		latest = false
	}

	var (
		xs       api.NodesXactMultiSnap
		xactArgs = api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck, OnlyRunning: latest}
	)
	xs, err = api.QueryXactionSnaps(apiBP, xactArgs)
	if err != nil {
		return
	}
	if flagIsSet(c, activeFlag) {
		for tid, snaps := range xs {
			if len(snaps) == 0 {
				continue
			}
			runningStats := xs[tid][:0]
			for _, xctn := range snaps {
				if xctn.Running() {
					runningStats = append(runningStats, xctn)
				}
			}
			xs[tid] = runningStats
		}
	}

	if nodeID != "" {
		for tid := range xs {
			if tid != nodeID {
				delete(xs, tid)
			}
		}
	}

	dts := make([]daemonTemplateXactSnaps, len(xs))
	i := 0
	for tid, snaps := range xs {
		sort.Slice(snaps, func(i, j int) bool {
			di, dj := snaps[i], snaps[j]
			if di.Kind == dj.Kind {
				// ascending by running
				if di.Running() && dj.Running() {
					return di.StartTime.After(dj.StartTime) // descending by start time (if both running)
				} else if di.Running() && !dj.Running() {
					return true
				} else if !di.Running() && dj.Running() {
					return false
				}
				return di.EndTime.After(dj.EndTime) // descending by end time
			}
			return di.Kind < dj.Kind // ascending by kind
		})

		dts[i] = daemonTemplateXactSnaps{DaemonID: tid, XactSnaps: snaps}
		i++
	}
	sort.Slice(dts, func(i, j int) bool {
		return dts[i].DaemonID < dts[j].DaemonID // ascending by node id/name
	})

	// To display verbose stats the list must have less than 2 records
	canVerbose := len(dts) == 0 || (len(dts) == 1 && len(dts[0].XactSnaps) < 2)
	if !canVerbose && flagIsSet(c, verboseFlag) {
		fmt.Fprintf(c.App.ErrWriter, "Option `--verbose` is ignored when multiple xactions are displayed.\n")
	}

	useJSON := flagIsSet(c, jsonFlag)
	if useJSON {
		return tmpls.Print(dts, c.App.Writer, tmpls.XactionsBodyTmpl, nil, useJSON)
	}

	if canVerbose && flagIsSet(c, verboseFlag) {
		var props nvpairList
		if len(dts) == 0 || len(dts[0].XactSnaps) == 0 {
			props = make(nvpairList, 0)
		} else {
			props = flattenXactStats(dts[0].XactSnaps[0])
		}
		return tmpls.Print(props, c.App.Writer, tmpls.PropsSimpleTmpl, nil, useJSON)
	}

	hideHeader := flagIsSet(c, noHeaderFlag)
	switch xactKind {
	case apc.ActECGet:
		// TODO: hideHeader
		return tmpls.Print(dts, c.App.Writer, tmpls.XactionECGetBodyTmpl, nil, useJSON)
	case apc.ActECPut:
		// TODO: ditto
		return tmpls.Print(dts, c.App.Writer, tmpls.XactionECPutBodyTmpl, nil, useJSON)
	default:
		if hideHeader {
			return tmpls.Print(dts, c.App.Writer, tmpls.XactionsBodyNoHeaderTmpl, nil, useJSON)
		}
		return tmpls.Print(dts, c.App.Writer, tmpls.XactionsBodyTmpl, nil, useJSON)
	}
}

func showObjectHandler(c *cli.Context) (err error) {
	fullObjName := c.Args().Get(0) // empty string if no arg given

	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in format bucket/object")
	}
	bck, object, err := parseBckObjectURI(c, fullObjName)
	if err != nil {
		return err
	}
	if _, err := headBucket(bck, true /* don't add */); err != nil {
		return err
	}
	return showObjProps(c, bck, object)
}

func showRebalanceHandler(c *cli.Context) (err error) {
	return showRebalance(c, flagIsSet(c, refreshFlag), calcRefreshRate(c))
}

func showBckPropsHandler(c *cli.Context) (err error) {
	return showBucketProps(c)
}

func showSmapHandler(c *cli.Context) (err error) {
	var (
		primarySmap *cluster.Smap
		daemonID    = argDaemonID(c)
	)
	if primarySmap, err = fillMap(); err != nil {
		return
	}
	if err = updateLongRunParams(c); err != nil {
		return
	}
	return clusterSmap(c, primarySmap, daemonID, flagIsSet(c, jsonFlag))
}

func showBMDHandler(c *cli.Context) (err error) {
	return getBMD(c)
}

func showClusterConfigHandler(c *cli.Context) (err error) {
	return showClusterConfig(c, c.Args().First())
}

func showConfigHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, "missing arguments (hint: press <TAB-TAB>)")
	}
	if c.Args().First() == subcmdCLI {
		return showCLIConfigHandler(c)
	}
	if c.Args().First() == subcmdCluster {
		return showClusterConfig(c, c.Args().Get(1))
	}
	return showNodeConfig(c)
}

func showClusterConfig(c *cli.Context, what string) error {
	var (
		section        = strings.Split(what, "=")[0] // when called to show set-config result
		useJSON        = flagIsSet(c, jsonFlag)
		cluConfig, err = api.GetClusterConfig(apiBP)
	)
	if err != nil {
		return err
	}

	if useJSON && section != "" {
		if printSectionJSON(c, cluConfig, section) {
			return nil
		}
		useJSON = false
	}

	if useJSON {
		return tmpls.Print(cluConfig, c.App.Writer, "", nil, useJSON)
	}
	flat := flattenConfig(cluConfig, section)
	return tmpls.Print(flat, c.App.Writer, tmpls.ConfigTmpl, nil, false)
}

func showNodeConfig(c *cli.Context) error {
	var (
		node           *cluster.Snode
		section, scope string
		daemonID       = argDaemonID(c)
		useJSON        = flagIsSet(c, jsonFlag)
	)
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return err
	}
	if node = smap.GetNode(daemonID); node == nil {
		return fmt.Errorf("node %q does not exist (see 'ais show cluster')", daemonID)
	}
	sname := node.StringEx()
	config, err := api.GetDaemonConfig(apiBP, node)
	if err != nil {
		return err
	}

	data := struct {
		ClusterConfigDiff []propDiff
		LocalConfigPairs  nvpairList
	}{}
	for _, a := range c.Args().Tail() {
		if a == scopeAll || a == cfgScopeInherited || a == cfgScopeLocal {
			if scope != "" {
				return incorrectUsageMsg(c, "... %s %s ...", scope, a)
			}
			scope = a
		} else {
			if scope == "" {
				return incorrectUsageMsg(c, "... %v ...", c.Args().Tail())
			}
			if section != "" {
				return incorrectUsageMsg(c, "... %s %s ...", section, a)
			}
			section = a
			if i := strings.IndexByte(section, '='); i > 0 {
				section = section[:i] // when called to show set-config result (same as above)
			}
		}
	}

	if useJSON {
		warn := "option '--" + strings.Split(jsonFlag.Name, ",")[0] +
			"' won't show node <=> cluster configuration differences, if any."
		switch scope {
		case cfgScopeLocal:
			if section == "" {
				return tmpls.Print(&config.LocalConfig, c.App.Writer, "", nil, true /* use JSON*/)
			}
			if !printSectionJSON(c, &config.LocalConfig, section) {
				fmt.Fprintln(c.App.Writer)
			}
			return nil
		case cfgScopeInherited:
			actionWarn(c, warn)
			if section == "" {
				return tmpls.Print(&config.ClusterConfig, c.App.Writer, "", nil, true)
			}
			if !printSectionJSON(c, &config.ClusterConfig, section) {
				fmt.Fprintln(c.App.Writer)
			}
			return nil
		default: // cfgScopeAll
			if section == "" {
				actionCptn(c, sname, " local config:")
				if err := tmpls.Print(&config.LocalConfig, c.App.Writer, "", nil, true); err != nil {
					return err
				}
				fmt.Fprintln(c.App.Writer)
				actionCptn(c, sname, " inherited config:")
				actionWarn(c, warn)
				return tmpls.Print(&config.ClusterConfig, c.App.Writer, "", nil, true)
			}
			// fall through on purpose
		}
	}

	useJSON = false

	// fill-in `data`
	switch scope {
	case cfgScopeLocal:
		data.LocalConfigPairs = flattenConfig(config.LocalConfig, section)
	default: // cfgScopeInherited | cfgScopeAll
		cluConf, err := api.GetClusterConfig(apiBP)
		if err != nil {
			return err
		}
		// diff cluster <=> this node
		flatNode := flattenConfig(config.ClusterConfig, section)
		flatCluster := flattenConfig(cluConf, section)
		data.ClusterConfigDiff = diffConfigs(flatNode, flatCluster)
		if scope == cfgScopeAll {
			data.LocalConfigPairs = flattenConfig(config.LocalConfig, section)
		}
	}
	// show "flat" diff-s
	if len(data.LocalConfigPairs) == 0 && len(data.ClusterConfigDiff) == 0 {
		fmt.Fprintf(c.App.Writer, "PROPERTY\t VALUE\n\n")
		return nil
	}
	return tmpls.Print(data, c.App.Writer, tmpls.DaemonConfigTmpl, nil, useJSON)
}

func showDaemonLogHandler(c *cli.Context) (err error) {
	if c.NArg() < 1 {
		return missingArgumentsError(c, "daemon ID")
	}
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return err
	}
	daemonID := argDaemonID(c)
	node := smap.GetNode(daemonID)
	if node == nil {
		return fmt.Errorf("node %q does not exist (see 'ais show cluster')", daemonID)
	}

	sev := strings.ToLower(parseStrFlag(c, logSevFlag))
	if sev != "" {
		switch sev[0] {
		case apc.LogInfo[0], apc.LogWarn[0], apc.LogErr[0]:
		default:
			return fmt.Errorf("invalid log severity, expecting empty or one of: %s, %s, %s",
				apc.LogInfo, apc.LogWarn, apc.LogErr)
		}
	}
	args := api.GetLogInput{Writer: os.Stdout, Severity: sev}
	return api.GetDaemonLog(apiBP, node, args)
}

func showRemoteAISHandler(c *cli.Context) error {
	all, err := api.GetRemoteAIS(apiBP)
	if err != nil {
		return err
	}
	tw := &tabwriter.Writer{}
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)
	if !flagIsSet(c, noHeaderFlag) {
		fmt.Fprintln(tw, "UUID\tURL\tAlias\tPrimary\tSmap\tTargets\tOnline")
	}
	for _, ra := range all.A {
		online := "no"
		if ra.Online {
			online = "yes"
		}
		if ra.Smap != nil {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\tv%d\t%d\t%s\n",
				ra.UUID, ra.URL, ra.Alias, ra.Smap.Primary, ra.Smap.Version, ra.Smap.CountTargets(), online)
		} else {
			url := ra.URL
			if url[0] == '[' {
				url = strings.Replace(url, "[", "<", 1)
				url = strings.Replace(url, "]", ">", 1)
			}
			fmt.Fprintf(tw, "<%s>\t%s\t%s\t%s\t%s\t%s\t%s\n", ra.UUID, url, ra.Alias, "n/a", "n/a", "n/a", online)
		}
	}
	tw.Flush()

	if flagIsSet(c, verboseFlag) {
		for _, ra := range all.A {
			fmt.Fprintln(c.App.Writer)
			actionCptn(c, ra.Alias+"["+ra.UUID+"]", " cluster map:")
			err := clusterSmap(c, ra.Smap, "" /*daemonID*/, flagIsSet(c, jsonFlag))
			if err != nil {
				actionWarn(c, err.Error())
			}
		}
	}
	return nil
}

func showMpathHandler(c *cli.Context) error {
	var (
		daemonID = argDaemonID(c)
		nodes    []*cluster.Snode
	)
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return err
	}
	if err = updateLongRunParams(c); err != nil {
		return err
	}
	if daemonID != "" {
		tgt := smap.GetTarget(daemonID)
		if tgt == nil {
			return fmt.Errorf("target ID %q invalid - no such target", daemonID)
		}
		nodes = []*cluster.Snode{tgt}
	} else {
		nodes = make(cluster.Nodes, 0, len(smap.Tmap))
		for _, tgt := range smap.Tmap {
			nodes = append(nodes, tgt)
		}
	}
	wg := &sync.WaitGroup{}
	mpCh := make(chan *targetMpath, len(nodes))
	erCh := make(chan error, len(nodes))
	for _, node := range nodes {
		wg.Add(1)
		go func(node *cluster.Snode) {
			defer wg.Done()
			mpl, err := api.GetMountpaths(apiBP, node)
			if err != nil {
				erCh <- err
			} else {
				mpCh <- &targetMpath{
					DaemonID: node.ID(),
					Mpl:      mpl,
				}
			}
		}(node)
	}
	wg.Wait()
	close(erCh)
	close(mpCh)
	for err := range erCh {
		return err
	}
	mpls := make([]*targetMpath, 0, len(nodes))
	for mp := range mpCh {
		mpls = append(mpls, mp)
	}
	sort.Slice(mpls, func(i, j int) bool {
		return mpls[i].DaemonID < mpls[j].DaemonID // ascending by node id
	})
	useJSON := flagIsSet(c, jsonFlag)
	return tmpls.Print(mpls, c.App.Writer, tmpls.TargetMpathListTmpl, nil, useJSON)
}

func fmtStatValue(name string, value int64, human bool) string {
	if human {
		return formatStatHuman(name, value)
	}
	return fmt.Sprintf("%v", value)
}

func appendStatToProps(props nvpairList, name string, value int64, prefix, filter string, human bool) nvpairList {
	name = prefix + name
	if filter != "" && !strings.Contains(name, filter) {
		return props
	}
	return append(props, nvpair{Name: name, Value: fmtStatValue(name, value, human)})
}

func showDaemonStats(c *cli.Context, node *cluster.Snode) error {
	stats, err := api.GetDaemonStats(apiBP, node)
	if err != nil {
		return err
	}
	if flagIsSet(c, jsonFlag) {
		return tmpls.Print(stats, c.App.Writer, tmpls.ConfigTmpl, nil, true)
	}

	human := !flagIsSet(c, rawFlag)
	filter := c.Args().Get(1)
	props := make(nvpairList, 0, len(stats.Tracker))
	for k, v := range stats.Tracker {
		props = appendStatToProps(props, k, v.Value, "", filter, human)
	}
	sort.Slice(props, func(i, j int) bool {
		return props[i].Name < props[j].Name
	})
	if node.IsTarget() {
		mID := 0
		// Make mountpaths always sorted.
		mpathSorted := make([]string, 0, len(stats.MPCap))
		for mpath := range stats.MPCap {
			mpathSorted = append(mpathSorted, mpath)
		}
		sort.Strings(mpathSorted)
		for _, mpath := range mpathSorted {
			mstat := stats.MPCap[mpath]
			prefix := fmt.Sprintf("mountpath.%d.", mID)
			if filter != "" && !strings.HasPrefix(prefix, filter) {
				continue
			}
			props = append(props,
				nvpair{Name: prefix + "path", Value: mpath},
				nvpair{Name: prefix + "used", Value: fmtStatValue(".size", int64(mstat.Used), human)},
				nvpair{Name: prefix + "avail", Value: fmtStatValue(".size", int64(mstat.Avail), human)},
				nvpair{Name: prefix + "%used", Value: fmt.Sprintf("%d", mstat.PctUsed)})
			mID++
		}
	}
	return tmpls.Print(props, c.App.Writer, tmpls.ConfigTmpl, nil, false)
}

func showClusterTotalStats(c *cli.Context) (err error) {
	st, err := api.GetClusterStats(apiBP)
	if err != nil {
		return err
	}

	useJSON := flagIsSet(c, jsonFlag)
	if useJSON {
		return tmpls.Print(st, c.App.Writer, tmpls.TargetMpathListTmpl, nil, useJSON)
	}

	human := !flagIsSet(c, rawFlag)
	filter := c.Args().Get(0)
	props := make(nvpairList, 0, len(st.Proxy.Tracker))
	for k, v := range st.Proxy.Tracker {
		props = appendStatToProps(props, k, v.Value, "proxy.", filter, human)
	}
	tgtStats := make(map[string]int64)
	for _, tgt := range st.Target {
		for k, v := range tgt.Tracker {
			if strings.HasSuffix(k, ".time") {
				continue
			}
			if totalVal, ok := tgtStats[k]; ok {
				v.Value += totalVal
			}
			tgtStats[k] = v.Value
		}
	}
	// Replace all "*.ns" counters with their average values.
	tgtCnt := int64(len(st.Target))
	for k, v := range tgtStats {
		if strings.HasSuffix(k, ".ns") {
			tgtStats[k] = v / tgtCnt
		}
	}

	for k, v := range tgtStats {
		props = appendStatToProps(props, k, v, "target.", filter, human)
	}

	sort.Slice(props, func(i, j int) bool {
		return props[i].Name < props[j].Name
	})

	return tmpls.Print(props, c.App.Writer, tmpls.ConfigTmpl, nil, false)
}

func showClusterStatsHandler(c *cli.Context) (err error) {
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return err
	}
	var (
		node     *cluster.Snode
		daemonID = argDaemonID(c)
	)
	if daemonID != "" {
		node = smap.GetNode(daemonID)
	}
	refresh := flagIsSet(c, refreshFlag)
	sleep := calcRefreshRate(c)

	for {
		if node != nil {
			err = showDaemonStats(c, node)
		} else {
			err = showClusterTotalStats(c)
		}
		if err != nil || !refresh {
			return err
		}

		time.Sleep(sleep)
	}
}
