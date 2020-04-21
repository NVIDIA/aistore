// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file contains implementation of the top-level `show` command.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	"github.com/urfave/cli"
)

type (
	daemonTemplateStats struct {
		DaemonID string
		Stats    []*stats.BaseXactStatsExt
	}

	xactionTemplateCtx struct {
		Stats   *[]daemonTemplateStats
		Verbose bool
	}
)

var (
	showCmdsFlags = map[string][]cli.Flag{
		subcmdShowBucket: {
			fastDetailsFlag,
			cachedFlag,
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
		},
		subcmdShowObject: {
			objPropsFlag,
			noHeaderFlag,
			jsonFlag,
		},
		subcmdShowNode: append(
			longRunFlags,
			jsonFlag,
		),
		subcmdShowXaction: {
			jsonFlag,
			allItemsFlag,
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
		subcmdShowSmap: {
			jsonFlag,
		},
		subcmdShowRemoteAIS: {
			noHeaderFlag,
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
					BashComplete: daemonCompletions(true /* omit proxies */),
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
					Name:         subcmdShowDsort,
					Usage:        fmt.Sprintf("show information about %s jobs", cmn.DSortName),
					ArgsUsage:    optionalJobIDArgument,
					Flags:        showCmdsFlags[subcmdShowDsort],
					Action:       showDsortHandler,
					BashComplete: dsortIDAllCompletions,
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
					Name:         subcmdShowNode,
					Usage:        "show node details",
					ArgsUsage:    optionalDaemonIDArgument,
					Flags:        showCmdsFlags[subcmdShowNode],
					Action:       showNodeHandler,
					BashComplete: daemonCompletions(false /* omit proxies */),
				},
				{
					Name:         subcmdShowXaction,
					Usage:        "show xaction details",
					ArgsUsage:    "[XACTION_ID|XACTION_NAME] [BUCKET_NAME]",
					Description:  xactionDesc(""),
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
					Name:         subcmdShowSmap,
					Usage:        "display an smap copy of a node",
					ArgsUsage:    optionalDaemonIDArgument,
					Flags:        showCmdsFlags[subcmdShowSmap],
					Action:       showSmapHandler,
					BashComplete: daemonCompletions(false /* omit proxies */),
				},
				{
					Name:         subcmdShowRemoteAIS,
					Usage:        "show attached AIS clusters",
					ArgsUsage:    "",
					Flags:        showCmdsFlags[subcmdShowRemoteAIS],
					Action:       showRemoteAISHandler,
					BashComplete: daemonCompletions(true /* omit proxies */),
				},
			},
		},
	}
)

func showBucketHandler(c *cli.Context) (err error) {
	bck, objName, err := parseBckObjectURI(c.Args().First())
	if err != nil {
		return
	}
	if objName != "" {
		return objectNameArgumentNotSupported(c, objName)
	}
	if bck, err = validateBucket(c, bck, "", true); err != nil {
		return
	}
	return bucketDetails(c, cmn.QueryBcks(bck))
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

func showNodeHandler(c *cli.Context) (err error) {
	daemonID := c.Args().First()
	if _, err = fillMap(); err != nil {
		return
	}

	if err = updateLongRunParams(c); err != nil {
		return
	}

	return daemonStats(c, daemonID, flagIsSet(c, jsonFlag))
}

func showXactionHandler(c *cli.Context) (err error) {
	xactID, xactKind, bck, err := parseXactionFromArgs(c)
	if err != nil {
		return err
	}

	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck, Latest: !flagIsSet(c, allItemsFlag)}
	xactStats, err := api.GetXactionStats(defaultAPIParams, xactArgs)
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
	var (
		fullObjName = c.Args().Get(0) // empty string if no arg given
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in format bucket/object")
	}
	bck, object, err := parseBckObjectURI(fullObjName)
	if err != nil {
		return
	}
	if bck, err = validateBucket(c, bck, fullObjName, false); err != nil {
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
