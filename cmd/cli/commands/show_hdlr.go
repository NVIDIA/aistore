// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file contains implementation of the top-level `show` command.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/urfave/cli"
)

type (
	daemonTemplateStats struct {
		DaemonID string
		Stats    []*xaction.BaseStatsExt
	}

	xactionTemplateCtx struct {
		Stats   *[]daemonTemplateStats
		Verbose bool
	}

	targetMpath struct {
		DaemonID  string   `json:"daemon_id"`
		Available []string `json:"available"`
		Disabled  []string `json:"disabled"`
	}
)

var (
	showCmdsFlags = map[string][]cli.Flag{
		subcmdShowDisk: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
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
			noHeaderFlag,
			jsonFlag,
		},
		subcmdShowCluster: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
			verboseFlag,
		),
		subcmdSmap: {
			jsonFlag,
		},
		subcmdBMD: {
			jsonFlag,
		},
		subcmdShowXaction: {
			jsonFlag,
			allXactionsFlag,
			activeFlag,
			verboseFlag,
		},
		subcmdShowRebalance: {
			refreshFlag,
			allXactionsFlag,
		},
		subcmdShowBucket: {
			jsonFlag,
			compactPropFlag,
		},
		subcmdShowConfig: {
			configTypeFlag,
			jsonFlag,
		},
		subcmdShowRemoteAIS: {
			noHeaderFlag,
		},
		subcmdShowMpath: {
			jsonFlag,
		},
		subcmdShowLog: {
			logSevFlag,
		},
	}

	showCmd = cli.Command{
		Name:  commandShow,
		Usage: "show information about buckets, jobs, all other managed entities in the cluster and the cluster itself",
		Subcommands: []cli.Command{
			makeAlias(authCmdShow, "", true, commandAuth),               // alias for `ais auth show`
			makeAlias(storageCmd, commandStorage, true, commandStorage), // alias for `ais storage ...`
			showCmdDisk,
			showCmdObject,
			showCmdCluster,
			showCmdRebalance,
			showCmdBucket,
			showCmdConfig,
			showCmdRemoteAIS,
			showCmdMpath,
			showCmdJob,
			showCmdLog,
		},
	}

	// define separately to allow for aliasing
	showCmdDisk = cli.Command{
		Name:         subcmdShowDisk,
		Usage:        "show disk statistics for targets",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showCmdsFlags[subcmdShowDisk],
		Action:       showDisksHandler,
		BashComplete: daemonCompletions(completeTargets),
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
		ArgsUsage: "[DAEMON_ID|DAEMON_TYPE|smap|bmd|config]",
		Flags:     showCmdsFlags[subcmdShowCluster],
		Action:    showClusterHandler,
		BashComplete: func(c *cli.Context) {
			if c.NArg() == 0 {
				fmt.Printf("%s\n%s\n%s\n%s\n%s\n", cmn.Proxy, cmn.Target, subcmdSmap, subcmdBMD, subcmdConfig)
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
		Usage:        "show daemon or cluster configuration",
		ArgsUsage:    showConfigArgument,
		Flags:        showCmdsFlags[subcmdShowConfig],
		Action:       showDaemonConfigHandler,
		BashComplete: daemonConfigSectionCompletions,
	}
	showCmdRemoteAIS = cli.Command{
		Name:         subcmdShowRemoteAIS,
		Usage:        "show attached AIS clusters",
		ArgsUsage:    "",
		Flags:        showCmdsFlags[subcmdShowRemoteAIS],
		Action:       showRemoteAISHandler,
		BashComplete: daemonCompletions(completeTargets),
	}
	showCmdLog = cli.Command{
		Name:         subcmdShowLog,
		Usage:        "show daemon log",
		ArgsUsage:    daemonIDArgument,
		Flags:        showCmdsFlags[subcmdShowLog],
		Action:       showDaemonLogHandler,
		BashComplete: daemonCompletions(completeAllDaemons),
	}
	showCmdMpath = cli.Command{
		Name:         subcmdShowMpath,
		Usage:        "show target mountpaths",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showCmdsFlags[subcmdShowMpath],
		Action:       showMpathHandler,
		BashComplete: daemonCompletions(completeTargets),
	}
	showCmdJob = cli.Command{
		Name:  subcmdShowJob,
		Usage: "show running and completed jobs (xactions)",
		Subcommands: []cli.Command{
			showCmdDownload,
			showCmdDsort,
			showCmdXaction,
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
		Usage:     fmt.Sprintf("show information about %s jobs", cmn.DSortName),
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
		ArgsUsage:    "[XACTION_ID|XACTION_NAME] [BUCKET]",
		Description:  xactionDesc(false),
		Flags:        showCmdsFlags[subcmdShowXaction],
		Action:       showXactionHandler,
		BashComplete: xactionCompletions(""),
	}
)

func showDisksHandler(c *cli.Context) (err error) {
	daemonID := c.Args().First()
	if _, err = fillMap(); err != nil {
		return
	}

	if err = updateLongRunParams(c); err != nil {
		return
	}

	return daemonDiskStats(c, daemonID, flagIsSet(c, jsonFlag), flagIsSet(c, noHeaderFlag))
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

func showClusterHandler(c *cli.Context) (err error) {
	daemonID := c.Args().First()
	primarySmap, err := fillMap()
	if err != nil {
		return
	}

	if err = updateLongRunParams(c); err != nil {
		return
	}
	return clusterDaemonStatus(c, primarySmap, daemonID, flagIsSet(c, jsonFlag), flagIsSet(c, noHeaderFlag), flagIsSet(c, verboseFlag))
}

func showXactionHandler(c *cli.Context) (err error) {
	xactID, xactKind, bck, err := parseXactionFromArgs(c)
	if err != nil {
		return err
	}
	var xactStats api.NodesXactMultiStats
	latest := !flagIsSet(c, allXactionsFlag)
	if xactID != "" {
		latest = false
	}

	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck, OnlyRunning: latest}
	xactStats, err = api.QueryXactionStats(defaultAPIParams, xactArgs)

	if err != nil {
		return
	}

	if flagIsSet(c, activeFlag) {
		for daemonID, daemonStats := range xactStats {
			if len(daemonStats) == 0 {
				continue
			}
			runningStats := xactStats[daemonID][:0]
			for _, xact := range daemonStats {
				if xact.Running() {
					runningStats = append(runningStats, xact)
				}
			}
			xactStats[daemonID] = runningStats
		}
	}

	dts := make([]daemonTemplateStats, len(xactStats))
	i := 0
	for daemonID, daemonStats := range xactStats {
		sort.Slice(daemonStats, func(i, j int) bool {
			di, dj := daemonStats[i], daemonStats[j]
			if di.Kind() == dj.Kind() {
				// ascending by running
				if di.Running() && dj.Running() {
					return di.StartTime().After(dj.StartTime()) // descending by start time (if both running)
				} else if di.Running() && !dj.Running() {
					return true
				} else if !di.Running() && dj.Running() {
					return false
				}
				return di.EndTime().After(dj.EndTime()) // descending by end time
			}
			return di.Kind() < dj.Kind() // ascending by kind
		})

		s := daemonTemplateStats{
			DaemonID: daemonID,
			Stats:    daemonStats,
		}
		dts[i] = s
		i++
	}
	sort.Slice(dts, func(i, j int) bool {
		return dts[i].DaemonID < dts[j].DaemonID // ascending by node id/name
	})
	ctx := xactionTemplateCtx{
		Stats:   &dts,
		Verbose: flagIsSet(c, verboseFlag),
	}

	useJSON := flagIsSet(c, jsonFlag)
	if useJSON {
		return templates.DisplayOutput(ctx, c.App.Writer, templates.XactionsBodyTmpl, useJSON)
	}

	switch xactKind {
	case cmn.ActECGet:
		return templates.DisplayOutput(ctx, c.App.Writer, templates.XactionECGetBodyTmpl, useJSON)
	case cmn.ActECPut:
		return templates.DisplayOutput(ctx, c.App.Writer, templates.XactionECPutBodyTmpl, useJSON)
	default:
		return templates.DisplayOutput(ctx, c.App.Writer, templates.XactionsBodyTmpl, useJSON)
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
	if _, err := headBucket(bck); err != nil {
		return err
	}
	return objectStats(c, bck, object)
}

func showRebalanceHandler(c *cli.Context) (err error) {
	return showRebalance(c, flagIsSet(c, refreshFlag), calcRefreshRate(c))
}

func showBckPropsHandler(c *cli.Context) (err error) {
	return showBucketProps(c)
}

func showSmapHandler(c *cli.Context) (err error) {
	var (
		daemonID    = c.Args().First()
		primarySmap *cluster.Smap
	)
	if primarySmap, err = fillMap(); err != nil {
		return
	}
	return clusterSmap(c, primarySmap, daemonID, flagIsSet(c, jsonFlag))
}

func showBMDHandler(c *cli.Context) (err error) {
	if _, err = fillMap(); err != nil {
		return
	}
	return getBMD(c)
}

func showClusterConfigHandler(c *cli.Context) (err error) {
	if _, err = fillMap(); err != nil {
		return
	}
	return getClusterConfig(c, c.Args().First())
}

func showDaemonConfigHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "'cluster' or daemon ID")
	}
	if c.Args().Get(1) == "" && !flagIsSet(c, configTypeFlag) && !flagIsSet(c, jsonFlag) {
		cli.ShowSubcommandHelp(c)
		return fmt.Errorf("must specify --type or a configuration prefix")
	}
	filter := parseStrFlag(c, configTypeFlag)
	if !cos.NewStringSet("all", "cluster", "local", "").Contains(filter) {
		return fmt.Errorf("invalid value provided for --type, expected one of: 'all','cluster','local'")
	}

	if _, err = fillMap(); err != nil {
		return
	}
	if c.Args().First() == subcmdCluster {
		return getClusterConfig(c, c.Args().Get(1))
	}
	return getDaemonConfig(c)
}

func showDaemonLogHandler(c *cli.Context) (err error) {
	if c.NArg() < 1 {
		return missingArgumentsError(c, "daemon ID")
	}
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return err
	}
	sid := c.Args().First()
	node := smap.GetNode(sid)
	if node == nil {
		return fmt.Errorf("%s does not exist (see 'ais show cluster')", sid)
	}

	sev := strings.ToLower(parseStrFlag(c, logSevFlag))
	if sev != "" {
		switch sev[0] {
		case cmn.LogInfo[0], cmn.LogWarn[0], cmn.LogErr[0]:
		default:
			return fmt.Errorf("invalid log severity, expecting empty or one of: %s, %s, %s",
				cmn.LogInfo, cmn.LogWarn, cmn.LogErr)
		}
	}
	args := api.GetLogInput{Writer: os.Stdout, Severity: sev}
	return api.GetDaemonLog(defaultAPIParams, node, args)
}

func showRemoteAISHandler(c *cli.Context) (err error) {
	aisCloudInfo, err := api.GetRemoteAIS(defaultAPIParams)
	if err != nil {
		return err
	}
	tw := &tabwriter.Writer{}
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)
	if !flagIsSet(c, noHeaderFlag) {
		fmt.Fprintln(tw, "UUID\tURL\tAlias\tPrimary\tSmap\tTargets\tOnline")
	}
	for uuid, info := range aisCloudInfo {
		online := "no"
		if info.Online {
			online = "yes"
		}
		if info.Smap > 0 {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\tv%d\t%d\t%s\n",
				uuid, info.URL, info.Alias, info.Primary, info.Smap, info.Targets, online)
		} else {
			url := info.URL
			if url[0] == '[' {
				url = strings.Replace(url, "[", "<", 1)
				url = strings.Replace(url, "]", ">", 1)
			}
			fmt.Fprintf(tw, "<%s>\t%s\t%s\t%s\t%s\t%s\t%s\n",
				uuid, url, info.Alias, "n/a", "n/a", "n/a", online)
		}
	}
	tw.Flush()
	return
}

func showMpathHandler(c *cli.Context) (err error) {
	daemonID := c.Args().First()
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return err
	}
	var nodes []*cluster.Snode
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
			mpl, err := api.GetMountpaths(defaultAPIParams, node)
			if err != nil {
				erCh <- err
			} else {
				mpCh <- &targetMpath{
					DaemonID:  node.ID(),
					Available: mpl.Available,
					Disabled:  mpl.Disabled,
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
		return mpls[i].DaemonID < mpls[j].DaemonID // ascending by node id/name
	})
	useJSON := flagIsSet(c, jsonFlag)
	return templates.DisplayOutput(mpls, c.App.Writer, templates.TargetMpathListTmpl, useJSON)
}
