// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file contains implementation of the top-level `show` command.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/urfave/cli"
)

type (
	daemonTemplateStats struct {
		DaemonID string
		Stats    []*xaction.BaseXactStatsExt
	}

	xactionTemplateCtx struct {
		Stats   *[]daemonTemplateStats
		Verbose bool
	}

	targetMpath struct {
		DaemonID string
		Avail    []string
		Disabled []string
	}
)

var (
	showCmdsFlags = map[string][]cli.Flag{
		subcmdShowBucket: {
			cachedFlag,
			allFlag,
			verboseFlag,
		},
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
		),
		subcmdSmap: {
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
		},
		subcmdShowBckProps: {
			jsonFlag,
			verboseFlag,
		},
		subcmdShowConfig: {
			jsonFlag,
		},
		subcmdShowRemoteAIS: {
			noHeaderFlag,
		},
		subcmdShowMpath: {
			jsonFlag,
		},
	}

	showCmds = []cli.Command{
		{
			Name:  commandShow,
			Usage: "show control info about entities in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdShowBucket,
					Usage:        "show bucket details",
					ArgsUsage:    optionalBucketArgument,
					Flags:        showCmdsFlags[subcmdShowBucket],
					Action:       showBucketHandler,
					BashComplete: bucketCompletions(),
				},
				{
					Name:         subcmdShowDisk,
					Usage:        "show disk stats for targets",
					ArgsUsage:    optionalTargetIDArgument,
					Flags:        showCmdsFlags[subcmdShowDisk],
					Action:       showDisksHandler,
					BashComplete: daemonCompletions(completeTargets),
				},
				{
					Name:         subcmdShowDownload,
					Usage:        "show information about download jobs",
					ArgsUsage:    optionalJobIDArgument,
					Flags:        showCmdsFlags[subcmdShowDownload],
					Action:       showDownloadsHandler,
					BashComplete: downloadIDAllCompletions,
				},
				{
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
				},
				{
					Name:         subcmdShowObject,
					Usage:        "show object details",
					ArgsUsage:    objectArgument,
					Flags:        showCmdsFlags[subcmdShowObject],
					Action:       showObjectHandler,
					BashComplete: bucketCompletions(bckCompletionsOpts{separator: true}),
				},
				{
					Name:      subcmdShowCluster,
					Usage:     "show cluster details",
					ArgsUsage: "[DAEMON_ID|DAEMON_TYPE]",
					Flags:     showCmdsFlags[subcmdShowCluster],
					Action:    showClusterHandler,
					BashComplete: func(c *cli.Context) {
						if c.NArg() == 0 {
							fmt.Printf("%s\n%s\n%s\n", cmn.Proxy, cmn.Target, subcmdSmap)
						}
						daemonCompletions(completeAllDaemons)(c)
					},
					Subcommands: []cli.Command{
						{
							Name:         subcmdSmap,
							Usage:        "display an smap copy of a node",
							ArgsUsage:    optionalDaemonIDArgument,
							Flags:        showCmdsFlags[subcmdSmap],
							Action:       showSmapHandler,
							BashComplete: daemonCompletions(completeAllDaemons),
						},
					},
				},
				{
					Name:         subcmdShowXaction,
					Usage:        "show xaction details",
					ArgsUsage:    "[XACTION_ID|XACTION_NAME] [BUCKET_NAME]",
					Description:  xactionDesc(false),
					Flags:        showCmdsFlags[subcmdShowXaction],
					Action:       showXactionHandler,
					BashComplete: xactionCompletions(""),
				},
				{
					Name:      subcmdShowRebalance,
					Usage:     "show rebalance details",
					ArgsUsage: noArguments,
					Flags:     showCmdsFlags[subcmdShowRebalance],
					Action:    showRebalanceHandler,
				},
				{
					Name:         subcmdShowBckProps,
					Usage:        "show bucket properties",
					ArgsUsage:    bucketAndPropsArgument,
					Flags:        showCmdsFlags[subcmdShowBckProps],
					Action:       showBckPropsHandler,
					BashComplete: bucketAndPropsCompletions,
				},
				{
					Name:         subcmdShowConfig,
					Usage:        "show daemon configuration",
					ArgsUsage:    showConfigArgument,
					Flags:        showCmdsFlags[subcmdShowConfig],
					Action:       showConfigHandler,
					BashComplete: daemonConfigSectionCompletions(false /* daemon optional */),
				},
				{
					Name:         subcmdShowRemoteAIS,
					Usage:        "show attached AIS clusters",
					ArgsUsage:    "",
					Flags:        showCmdsFlags[subcmdShowRemoteAIS],
					Action:       showRemoteAISHandler,
					BashComplete: daemonCompletions(completeTargets),
				},
				{
					Name:         subcmdShowMpath,
					Usage:        "show mountpath list for targets",
					ArgsUsage:    optionalTargetIDArgument,
					Flags:        showCmdsFlags[subcmdShowMpath],
					Action:       showMpathHandler,
					BashComplete: daemonCompletions(completeTargets),
				},
			},
		},
	}
)

func showBucketHandler(c *cli.Context) (err error) {
	var (
		bck     cmn.Bck
		objName string
		objPath = c.Args().First()
	)

	if isWebURL(objPath) {
		bck = parseURLtoBck(objPath)
	} else if bck, objName, err = cmn.ParseBckObjectURI(objPath, true); err != nil {
		return
	}

	if objName != "" {
		return objectNameArgumentNotSupported(c, objName)
	}
	var props *cmn.BucketProps
	if bck, props, err = validateBucket(c, bck, "", true); err != nil {
		return
	}

	summaries, err := fetchSummaries(cmn.QueryBcks(bck), flagIsSet(c, fastFlag), flagIsSet(c, cachedFlag))
	if err != nil {
		return
	}

	if flagIsSet(c, allFlag) && props != nil {
		return displayAllProps(c, summaries[0], props)
	}

	tmpl := templates.BucketsSummariesTmpl
	if flagIsSet(c, fastFlag) {
		tmpl = templates.BucketsSummariesFastTmpl
	}
	return templates.DisplayOutput(summaries, c.App.Writer, tmpl)
}

func displayAllProps(c *cli.Context, summary cmn.BucketSummary, props *cmn.BucketProps) (err error) {
	propList := bckSummaryList(summary, flagIsSet(c, fastFlag))
	bckProp, err := bckPropList(props, flagIsSet(c, verboseFlag))
	if err != nil {
		return
	}
	propList = append(propList, bckProp...)

	return templates.DisplayOutput(propList, c.App.Writer, templates.BucketPropsSimpleTmpl)
}

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
	return clusterDaemonStatus(c, primarySmap, daemonID, flagIsSet(c, jsonFlag), flagIsSet(c, noHeaderFlag))
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

	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck, Latest: latest}
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

	return templates.DisplayOutput(ctx, c.App.Writer, templates.XactionsBodyTmpl, flagIsSet(c, jsonFlag))
}

func showObjectHandler(c *cli.Context) (err error) {
	fullObjName := c.Args().Get(0) // empty string if no arg given

	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in format bucket/object")
	}
	bck, object, err := cmn.ParseBckObjectURI(fullObjName)
	if err != nil {
		return
	}
	if bck, _, err = validateBucket(c, bck, fullObjName, false); err != nil {
		return
	}
	if object == "" {
		return incorrectUsageMsg(c, "no object specified in %q", fullObjName)
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

func showConfigHandler(c *cli.Context) (err error) {
	if _, err = fillMap(); err != nil {
		return
	}
	return getDaemonConfig(c)
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
		nodes = make([]*cluster.Snode, 0, len(smap.Tmap))
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
				mpCh <- &targetMpath{DaemonID: node.ID(), Avail: mpl.Available, Disabled: mpl.Disabled}
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
