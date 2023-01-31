// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains implementation of the top-level `show` command.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

type (
	daemonTemplateXactSnaps struct {
		DaemonID  string
		XactSnaps []*cluster.Snap
	}

	targetMpath struct {
		DaemonID string
		Mpl      *apc.MountpathList
	}
)

var (
	showCmdsFlags = map[string][]cli.Flag{
		commandStorage: append(
			longRunFlags,
			jsonFlag,
		),
		cmdShowDisk: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
		),
		cmdMountpath: append(
			longRunFlags,
			jsonFlag,
		),
		commandJob: append(
			longRunFlags,
			jsonFlag,
			allJobsFlag,
			regexFlag,
			noHeaderFlag,
			verboseFlag,
			// download and dsort only
			progressBarFlag,
			logFlag,
		),
		cmdObject: {
			objPropsFlag,
			allPropsFlag,
			objNotCachedFlag,
			noHeaderFlag,
			jsonFlag,
		},
		cmdCluster: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
		),
		cmdSmap: append(
			longRunFlags,
			jsonFlag,
		),
		cmdBMD: {
			jsonFlag,
		},
		cmdBucket: {
			jsonFlag,
			compactPropFlag,
		},
		cmdConfig: {
			jsonFlag,
		},
		cmdShowRemoteAIS: {
			noHeaderFlag,
			verboseFlag,
			jsonFlag,
		},
		cmdLog: append(
			longRunFlags,
			logSevFlag,
			logFlushFlag,
		),
		cmdShowStats: {
			jsonFlag,
			rawFlag,
			refreshFlag,
			regexFlag,
		},
	}

	showCmd = cli.Command{
		Name:  commandShow,
		Usage: "show configuration, buckets, jobs, etc. - all managed entities in the cluster, and the cluster itself",
		Subcommands: []cli.Command{
			makeAlias(authCmdShow, "", true, commandAuth), // alias for `ais auth show`
			showCmdObject,
			showCmdBucket,
			showCmdCluster,
			showCmdPeformance,
			showCmdStorage,
			showCmdRebalance,
			showCmdConfig,
			showCmdRemoteAIS,
			showCmdJob,
			showCmdLog,
		},
	}

	showCmdStorage = cli.Command{
		Name:      commandStorage,
		Usage:     "show storage usage and utilization, disks and mountpaths",
		ArgsUsage: optionalTargetIDArgument,
		Flags:     showCmdsFlags[commandStorage],
		Action:    showStorageHandler,
		Subcommands: []cli.Command{
			showCmdDisk,
			showCmdMpath,
			showCmdStgSummary,
		},
	}
	showCmdObject = cli.Command{
		Name:         cmdObject,
		Usage:        "show object details",
		ArgsUsage:    objectArgument,
		Flags:        showCmdsFlags[cmdObject],
		Action:       showObjectHandler,
		BashComplete: bucketCompletions(bcmplop{separator: true}),
	}
	showCmdStats = cli.Command{ // TODO -- FIXME: provide via `ais show stats` as well, and include jobs somehow ============
		Name:         cmdShowStats,
		Usage:        "show cluster and node statistics",
		ArgsUsage:    showStatsArgument,
		Flags:        showCmdsFlags[cmdShowStats],
		Action:       showStatsHandler,
		BashComplete: suggestAllNodes,
	}
	showCmdCluster = cli.Command{
		Name:         cmdCluster,
		Usage:        "show cluster nodes and utilization",
		ArgsUsage:    showClusterArgument,
		Flags:        showCmdsFlags[cmdCluster],
		Action:       showClusterHandler,
		BashComplete: showClusterCompletions, // NOTE: level 0 hardcoded
		Subcommands: []cli.Command{
			{
				Name:         cmdSmap,
				Usage:        "show Smap (cluster map)",
				ArgsUsage:    optionalNodeIDArgument,
				Flags:        showCmdsFlags[cmdSmap],
				Action:       showSmapHandler,
				BashComplete: suggestAllNodes,
			},
			{
				Name:         cmdBMD,
				Usage:        "show BMD (bucket metadata)",
				ArgsUsage:    optionalNodeIDArgument,
				Flags:        showCmdsFlags[cmdBMD],
				Action:       showBMDHandler,
				BashComplete: suggestAllNodes,
			},
			{
				Name:      cmdConfig,
				Usage:     "show cluster and node configuration",
				ArgsUsage: showClusterConfigArgument,
				Flags:     showCmdsFlags[cmdConfig],
				Action:    showClusterConfigHandler,
			},
			showCmdStats,
		},
	}
	showCmdBucket = cli.Command{
		Name:         cmdBucket,
		Usage:        "show bucket properties",
		ArgsUsage:    bucketAndPropsArgument,
		Flags:        showCmdsFlags[cmdBucket],
		Action:       showBckPropsHandler,
		BashComplete: bucketAndPropsCompletions, // bucketCompletions(),
	}
	showCmdConfig = cli.Command{
		Name:         cmdConfig,
		Usage:        "show CLI, cluster, or node configurations (nodes inherit cluster and have local)",
		ArgsUsage:    showConfigArgument,
		Flags:        showCmdsFlags[cmdConfig],
		Action:       showConfigHandler,
		BashComplete: showConfigCompletions,
	}
	showCmdRemoteAIS = cli.Command{
		Name:         cmdShowRemoteAIS,
		Usage:        "show attached AIS clusters",
		ArgsUsage:    "",
		Flags:        showCmdsFlags[cmdShowRemoteAIS],
		Action:       showRemoteAISHandler,
		BashComplete: suggestTargetNodes, // NOTE: not using remais.smap yet
	}

	showCmdLog = cli.Command{
		Name:         cmdLog,
		Usage:        "show log",
		ArgsUsage:    nodeIDArgument,
		Flags:        showCmdsFlags[cmdLog],
		Action:       showNodeLogHandler,
		BashComplete: suggestAllNodes,
	}

	showCmdJob = cli.Command{
		Name:         commandJob,
		Usage:        "show running and finished jobs (use <TAB-TAB> to select, --all for all, help for options)",
		ArgsUsage:    jobShowStopWaitArgument,
		Flags:        showCmdsFlags[commandJob],
		Action:       showJobsHandler,
		BashComplete: runningJobCompletions,
	}

	// `show storage` sub-commands
	showCmdDisk = cli.Command{
		Name:         cmdShowDisk,
		Usage:        "show disk utilization and read/write statistics",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showCmdsFlags[cmdShowDisk],
		Action:       showDisksHandler,
		BashComplete: suggestTargetNodes,
	}
	showCmdStgSummary = cli.Command{
		Name:         cmdSummary,
		Usage:        "show bucket sizes and %% of used capacity on a per-bucket basis",
		ArgsUsage:    listAnyCommandArgument,
		Flags:        storageCmdFlags[cmdSummary],
		Action:       showBucketSummary,
		BashComplete: bucketCompletions(bcmplop{}),
	}
	showCmdMpath = cli.Command{
		Name:         cmdMountpath,
		Usage:        "show target mountpaths",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showCmdsFlags[cmdMountpath],
		Action:       showMpathHandler,
		BashComplete: suggestTargetNodes,
	}
)

func showDisksHandler(c *cli.Context) error {
	sid, sname, err := argNode(c)
	if err != nil {
		return err
	}
	if getNodeType(c, sid) == apc.Proxy {
		return fmt.Errorf("%s is a proxy (AIS gateways do not store user data and do not have any data drives)", sname)
	}
	return daemonDiskStats(c, sid)
}

// args [NAME] [JOB_ID] [NODE_ID] [BUCKET] may:
// - be omitted, in part or in total, and may
// - come in arbitrary order
func showJobsHandler(c *cli.Context) error {
	name, xid, daemonID, bck, err := jobArgs(c, 0, false /*ignore daemonID*/)
	if err != nil {
		return err
	}

	if name == cmdRebalance {
		return showRebalanceHandler(c)
	}

	setLongRunParams(c, 72)

	var l int
	l, err = showJobsDo(c, name, xid, daemonID, bck)
	if err == nil && l == 0 {
		n, h := qflprn(allJobsFlag), qflprn(cli.HelpFlag)
		fmt.Fprintf(c.App.Writer, "No running jobs. Use %s to show all, %s <TAB-TAB> to select, %s for details.\n", n, n, h)
	}
	return err
}

func showJobsDo(c *cli.Context, name, xid, daemonID string, bck cmn.Bck) (int, error) {
	if name != "" {
		return _showJobs(c, name, xid, daemonID, bck, xid == "" /*caption*/)
	}

	// special (best-effort)
	if xid != "" {
		var otherID string
		name, otherID = xid2Name(xid)
		switch name {
		case cmdDownload:
			return _showJobs(c, cmdDownload, xid, daemonID, bck, false)
		case cmdDsort:
			return _showJobs(c, cmdDsort, xid, daemonID, bck, false)
		case commandETL:
			return _showJobs(c, commandETL, otherID /*etl name*/, daemonID, bck, false)
		}
	}

	var (
		ll  int
		err error
	)
	names := xact.ListDisplayNames(false /*only-startable*/)
	names = append(names, dsort.DSortName)
	sort.Strings(names)
	for _, name = range names {
		l, errV := _showJobs(c, name, "" /*xid*/, daemonID, bck, true)
		if errV != nil {
			actionWarn(c, errV.Error())
			err = errV
		}
		ll += l
	}
	return ll, err
}

func jobCptn(c *cli.Context, name string, onlyActive bool, xid string, byTarget bool) {
	if xid != "" {
		actionCptn(c, name, fmt.Sprintf(" job %s:", xid))
		return
	}

	var s string
	if byTarget {
		s = " by target"
	}
	if onlyActive {
		actionCptn(c, name, " jobs"+s+":")
	} else {
		actionCptn(c, name, " jobs"+s+" (including finished):")
	}
}

func _showJobs(c *cli.Context, name, xid, daemonID string, bck cmn.Bck, caption bool) (int, error) {
	switch name {
	case cmdDownload:
		return showDownloads(c, xid, caption)
	case cmdDsort:
		return showDsorts(c, xid, caption)
	case commandETL:
		return showETLs(c, xid, caption)
	default:
		var (
			all         = flagIsSet(c, allJobsFlag)
			onlyActive  = !all
			xactKind, _ = xact.GetKindName(name)
			xargs       = xact.ArgsMsg{
				ID:          xid,
				Kind:        xactKind,
				DaemonID:    daemonID,
				Bck:         bck,
				OnlyRunning: onlyActive,
			}
		)
		return xactList(c, xargs, caption)
	}
}

func showDownloads(c *cli.Context, id string, caption bool) (int, error) {
	if id == "" { // list all download jobs
		return downloadJobsList(c, parseStrFlag(c, regexFlag), caption)
	}
	// display status of a download job identified by its JOB_ID
	return 1, downloadJobStatus(c, id)
}

func showDsorts(c *cli.Context, id string, caption bool) (int, error) {
	var (
		usejs      = flagIsSet(c, jsonFlag)
		onlyActive = !flagIsSet(c, allJobsFlag)
	)
	if id == "" {
		list, err := api.ListDSort(apiBP, parseStrFlag(c, regexFlag), onlyActive)
		l := len(list)
		if err != nil || l == 0 {
			return l, err
		}
		if caption {
			jobCptn(c, cmdDsort, onlyActive, id, false)
		}
		return l, dsortJobsList(c, list, usejs)
	}

	// ID-ed dsort
	return 1, dsortJobStatus(c, id)
}

func showClusterHandler(c *cli.Context) (err error) {
	var (
		what, sid  string
		targetOnly bool
	)
	if c.NArg() > 0 {
		what = c.Args().Get(0)
		if id, _, errV := getNodeIDName(c, what); errV == nil {
			sid, what = id, ""
		}
	}
	if c.NArg() > 1 {
		arg := c.Args().Get(1)
		if sid != "" {
			return incorrectUsageMsg(c, "", arg)
		}
		if sid, _, err = getNodeIDName(c, arg); err != nil {
			return err
		}
	}
	if sid != "" && getNodeType(c, sid) == apc.Target {
		targetOnly = true
	}

	setLongRunParams(c)

	smap, err := fillNodeStatusMap(c, targetOnly)
	if err != nil {
		return err
	}
	cluConfig, err := api.GetClusterConfig(apiBP)
	if err != nil {
		return err
	}
	if sid != "" {
		return cluDaeStatus(c, smap, cluConfig, sid)
	}
	return cluDaeStatus(c, smap, cluConfig, what)
}

func showStorageHandler(c *cli.Context) (err error) {
	return daemonDiskStats(c, "") // all targets, all disks
}

func xactList(c *cli.Context, xargs xact.ArgsMsg, caption bool) (int, error) {
	// override the caller's choice if explicitly identified
	if xargs.ID != "" {
		debug.Assert(xact.IsValidUUID(xargs.ID), xargs.ID)
		xargs.OnlyRunning = false
	}

	xs, err := queryXactions(xargs)
	if err != nil {
		return 0, err
	}
	var numSnaps int
	for _, snaps := range xs {
		numSnaps += len(snaps)
	}
	if numSnaps == 0 {
		return 0, nil
	}

	var (
		ll           int
		allXactKinds = extractXactKinds(xs)
	)
	for _, xactKind := range allXactKinds {
		xactIDs := extractXactIDsForKind(xs, xactKind)
		for _, xid := range xactIDs {
			xargs.Kind, xargs.ID = xactKind, xid
			l, err := xlistByKindID(c, xargs, caption, xs)
			if err != nil {
				actionWarn(c, err.Error())
			}
			ll += l
		}
	}
	return ll, nil
}

func xlistByKindID(c *cli.Context, xargs xact.ArgsMsg, caption bool, xs api.XactMultiSnap) (int, error) {
	// first, extract snaps for: xargs.ID, Kind
	filteredXs := make(api.XactMultiSnap, 8)
	for tid, snaps := range xs {
		for _, snap := range snaps {
			if snap.ID != xargs.ID {
				continue
			}
			debug.Assert(snap.Kind == xargs.Kind)
			if _, ok := filteredXs[tid]; !ok {
				filteredXs[tid] = make([]*cluster.Snap, 0, 8)
			}
			filteredXs[tid] = append(filteredXs[tid], snap)
		}
	}

	// second, filteredXs => dts templates
	var (
		fromToBck, haveBck bool
		dts                = make([]daemonTemplateXactSnaps, 0, len(filteredXs))
	)
	for tid, snaps := range filteredXs {
		if len(snaps) == 0 {
			continue
		}
		if xargs.DaemonID != "" && xargs.DaemonID != tid {
			continue
		}
		if !snaps[0].SrcBck.IsEmpty() {
			debug.Assert(!snaps[0].DstBck.IsEmpty())
			fromToBck = true
		} else if !snaps[0].Bck.IsEmpty() {
			haveBck = true
		}
		dts = append(dts, daemonTemplateXactSnaps{DaemonID: tid, XactSnaps: snaps})
	}
	sort.Slice(dts, func(i, j int) bool {
		return dts[i].DaemonID < dts[j].DaemonID // ascending by node id/name
	})

	_, xname := xact.GetKindName(xargs.Kind)
	if caption {
		jobCptn(c, xname, xargs.OnlyRunning, xargs.ID, xargs.DaemonID != "")
	}

	l := len(dts)
	var (
		usejs      = flagIsSet(c, jsonFlag)
		hideHeader = flagIsSet(c, noHeaderFlag)
		err        error
	)
	switch xargs.Kind {
	case apc.ActECGet:
		if hideHeader {
			return l, tmpls.Print(dts, c.App.Writer, tmpls.XactECGetNoHdrTmpl, nil, usejs)
		}
		return l, tmpls.Print(dts, c.App.Writer, tmpls.XactECGetTmpl, nil, usejs)
	case apc.ActECPut:
		if hideHeader {
			return l, tmpls.Print(dts, c.App.Writer, tmpls.XactECPutNoHdrTmpl, nil, usejs)
		}
		return l, tmpls.Print(dts, c.App.Writer, tmpls.XactECPutTmpl, nil, usejs)
	default:
		switch {
		case fromToBck && hideHeader:
			err = tmpls.Print(dts, c.App.Writer, tmpls.XactNoHdrFromToTmpl, nil, usejs)
		case fromToBck:
			err = tmpls.Print(dts, c.App.Writer, tmpls.XactFromToTmpl, nil, usejs)
		case haveBck && hideHeader:
			err = tmpls.Print(dts, c.App.Writer, tmpls.XactNoHdrBucketTmpl, nil, usejs)
		case haveBck:
			err = tmpls.Print(dts, c.App.Writer, tmpls.XactBucketTmpl, nil, usejs)
		default:
			if hideHeader {
				err = tmpls.Print(dts, c.App.Writer, tmpls.XactNoHdrNoBucketTmpl, nil, usejs)
			} else {
				err = tmpls.Print(dts, c.App.Writer, tmpls.XactNoBucketTmpl, nil, usejs)
			}
		}
	}
	if err != nil || !flagIsSet(c, verboseFlag) {
		return l, err
	}

	// show in/out stats when verbose
	for _, di := range dts {
		if len(dts[0].XactSnaps) == 0 {
			continue
		}
		props := flattenXactStats(di.XactSnaps[0])
		_, name := xact.GetKindName(di.XactSnaps[0].Kind)
		debug.Assert(name != "", di.XactSnaps[0].Kind)
		actionCptn(c, cluster.Tname(di.DaemonID)+": ", fmt.Sprintf("%s[%s] stats", name, di.XactSnaps[0].ID))
		if err := tmpls.Print(props, c.App.Writer, tmpls.PropsSimpleTmpl, nil, usejs); err != nil {
			return l, err
		}
	}
	return l, nil
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

func showBckPropsHandler(c *cli.Context) (err error) {
	return showBucketProps(c)
}

func showSmapHandler(c *cli.Context) error {
	var (
		smap            *cluster.Smap
		sid, sname, err = argNode(c)
		targetOnly      bool
	)
	if err != nil {
		return err
	}
	if sid != "" {
		targetOnly = getNodeType(c, sid) == apc.Target
	}

	setLongRunParams(c)

	smap, err = fillNodeStatusMap(c, targetOnly)
	if err != nil {
		return err
	}
	if sid != "" {
		actionCptn(c, "Cluster map from: ", sname)
	}
	return smapFromNode(c, smap, sid, flagIsSet(c, jsonFlag))
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
	if c.Args().First() == cmdCLI {
		return showCLIConfigHandler(c)
	}
	if c.Args().First() == cmdCluster {
		return showClusterConfig(c, c.Args().Get(1))
	}
	return showNodeConfig(c)
}

func showClusterConfig(c *cli.Context, section string) error {
	var (
		usejs          = flagIsSet(c, jsonFlag)
		cluConfig, err = api.GetClusterConfig(apiBP)
	)
	if err != nil {
		return err
	}

	if usejs && section != "" {
		if printSectionJSON(c, cluConfig, section) {
			return nil
		}
		usejs = false
	}

	if usejs {
		return tmpls.Print(cluConfig, c.App.Writer, "", nil, usejs)
	}
	flat := flattenConfig(cluConfig, section)
	err = tmpls.Print(flat, c.App.Writer, tmpls.ConfigTmpl, nil, false)
	if err == nil && section == "" {
		actionDone(c, fmt.Sprintf("(Hint: use '[SECTION] %s' to show config section(s), see '--help' for details)", flprn(jsonFlag)))
	}
	return err
}

func showNodeConfig(c *cli.Context) error {
	var (
		smap           *cluster.Smap
		node           *cluster.Snode
		section, scope string
		usejs          = flagIsSet(c, jsonFlag)
	)
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	sid, sname, err := argNode(c)
	if err == nil {
		return err
	}
	smap, err = getClusterMap(c)
	debug.AssertNoErr(err)
	node = smap.GetNode(sid)
	debug.Assert(node != nil)

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

	if usejs {
		warn := "option " + qflprn(jsonFlag) + " won't show node <=> cluster configuration differences, if any."
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

	usejs = false

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
	return tmpls.Print(data, c.App.Writer, tmpls.DaemonConfigTmpl, nil, usejs)
}

func showNodeLogHandler(c *cli.Context) error {
	if c.NArg() < 1 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	sid, sname, err := argNode(c)
	if err != nil {
		return err
	}
	smap, err := getClusterMap(c)
	debug.AssertNoErr(err)
	node := smap.GetNode(sid)
	debug.Assert(node != nil)

	firstIteration := setLongRunParams(c, 0)

	sev := strings.ToLower(parseStrFlag(c, logSevFlag))
	if sev != "" {
		switch sev[0] {
		case apc.LogInfo[0], apc.LogWarn[0], apc.LogErr[0]:
		default:
			return fmt.Errorf("invalid log severity, expecting empty string or one of: %s, %s, %s",
				apc.LogInfo, apc.LogWarn, apc.LogErr)
		}
	}
	if firstIteration && flagIsSet(c, logFlushFlag) {
		var (
			flushRate = parseDurationFlag(c, logFlushFlag)
			nvs       = make(cos.StrKVs)
		)
		config, err := api.GetDaemonConfig(apiBP, node)
		if err != nil {
			return err
		}
		if config.Log.FlushTime.D() != flushRate {
			nvs[nodeLogFlushName] = flushRate.String()
			if err := api.SetDaemonConfig(apiBP, sid, nvs, true /*transient*/); err != nil {
				return err
			}
			warn := fmt.Sprintf("run 'ais config node %s inherited %s %s' to change it back",
				sname, nodeLogFlushName, config.Log.FlushTime)
			actionWarn(c, warn)
			time.Sleep(2 * time.Second)
			fmt.Fprintln(c.App.Writer)
		}
	}

	args := api.GetLogInput{Writer: os.Stdout, Severity: sev, Offset: getLongRunOffset(c)}
	readsize, err := api.GetDaemonLog(apiBP, node, args)
	if err == nil {
		addLongRunOffset(c, readsize)
	}
	return err
}

func showRemoteAISHandler(c *cli.Context) error {
	all, err := api.GetRemoteAIS(apiBP)
	if err != nil {
		return err
	}
	tw := &tabwriter.Writer{}
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)
	if !flagIsSet(c, noHeaderFlag) {
		fmt.Fprintln(tw, "UUID\tURL\tAlias\tPrimary\tSmap\tTargets\tUptime")
	}
	for _, ra := range all.A {
		uptime := "n/a"
		bp := api.BaseParams{
			Client: defaultHTTPClient,
			URL:    ra.URL,
			Token:  loggedUserToken,
			UA:     ua,
		}
		if clutime, _, err := api.HealthUptime(bp); err == nil {
			ns, _ := strconv.ParseInt(clutime, 10, 64)
			uptime = time.Duration(ns).String()
		}
		if ra.Smap != nil {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\tv%d\t%d\t%s\n",
				ra.UUID, ra.URL, ra.Alias, ra.Smap.Primary, ra.Smap.Version, ra.Smap.CountTargets(), uptime)
		} else {
			url := ra.URL
			if url[0] == '[' {
				url = strings.Replace(url, "[", "<", 1)
				url = strings.Replace(url, "]", ">", 1)
			}
			fmt.Fprintf(tw, "<%s>\t%s\t%s\t%s\t%s\t%s\t%s\n", ra.UUID, url, ra.Alias, "n/a", "n/a", "n/a", uptime)
		}
	}
	tw.Flush()

	if flagIsSet(c, verboseFlag) {
		for _, ra := range all.A {
			fmt.Fprintln(c.App.Writer)
			actionCptn(c, ra.Alias+"["+ra.UUID+"]", " cluster map:")
			err := smapFromNode(c, ra.Smap, "" /*daemonID*/, flagIsSet(c, jsonFlag))
			if err != nil {
				actionWarn(c, err.Error())
			}
		}
	}
	return nil
}

func showMpathHandler(c *cli.Context) error {
	var (
		smap            *cluster.Smap
		nodes           []*cluster.Snode
		sid, sname, err = argNode(c)
	)
	if err != nil {
		return err
	}
	smap, err = getClusterMap(c)
	if err != nil {
		return err
	}

	setLongRunParams(c)

	if sid != "" {
		node := smap.GetNode(sid)
		if node.IsProxy() {
			return fmt.Errorf("node %s is a proxy (expecting target)", sname)
		}
		nodes = []*cluster.Snode{node}
	} else {
		nodes = make(cluster.Nodes, 0, len(smap.Tmap))
		for _, tgt := range smap.Tmap {
			nodes = append(nodes, tgt)
		}
	}

	var (
		l    = len(nodes)
		wg   = cos.NewLimitedWaitGroup(sys.NumCPU(), l)
		mpCh = make(chan *targetMpath, l)
		erCh = make(chan error, l)
	)
	for _, node := range nodes {
		wg.Add(1)
		go func(node *cluster.Snode) {
			mpl, err := api.GetMountpaths(apiBP, node)
			if err != nil {
				erCh <- err
			} else {
				mpCh <- &targetMpath{
					DaemonID: node.ID(),
					Mpl:      mpl,
				}
			}
			wg.Done()
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
	usejs := flagIsSet(c, jsonFlag)
	return tmpls.Print(mpls, c.App.Writer, tmpls.TargetMpathListTmpl, nil, usejs)
}

func fmtStatValue(name string, value int64, human bool) string {
	if human {
		return formatStatHuman(name, value)
	}
	return fmt.Sprintf("%v", value)
}

func appendStatToProps(props nvpairList, name string, value int64, prefix string, regex *regexp.Regexp, human bool) nvpairList {
	name = prefix + name
	if regex != nil && !regex.MatchString(name) {
		return props
	}
	return append(props, nvpair{Name: name, Value: fmtStatValue(name, value, human)})
}

func showStatsHandler(c *cli.Context) (err error) {
	var (
		sid  string
		node *cluster.Snode
		smap *cluster.Smap
	)
	if c.NArg() > 0 {
		sid, _, err = argNode(c)
		if err != nil {
			return err
		}
		smap, err = getClusterMap(c)
		debug.AssertNoErr(err)
		node = smap.GetNode(sid)
	}
	var (
		regexStr    = parseStrFlag(c, regexFlag)
		regex       *regexp.Regexp
		refresh     = flagIsSet(c, refreshFlag)
		sleep       = calcRefreshRate(c)
		averageOver = cos.MinDuration(cos.MaxDuration(sleep/2, 2*time.Second), 10*time.Second)
	)
	if regexStr != "" {
		regex, err = regexp.Compile(regexStr)
		if err != nil {
			return
		}
	}
	sleep = cos.MaxDuration(10*time.Millisecond, sleep-averageOver)
	for {
		if node != nil {
			err = showNodeStats(c, node, averageOver, regex)
		} else {
			err = showAggregatedStats(c, averageOver, regex)
		}
		if err != nil || !refresh {
			return err
		}

		time.Sleep(sleep)
	}
}

func showNodeStats(c *cli.Context, node *cluster.Snode, averageOver time.Duration, regex *regexp.Regexp) error {
	stats, err := api.GetDaemonStats(apiBP, node)
	if err != nil {
		return err
	}

	// throughput (Bps)
	daemonBps(node, stats, averageOver)

	// TODO: extract regex-matching, if defined
	if flagIsSet(c, jsonFlag) {
		return tmpls.Print(stats, c.App.Writer, tmpls.ConfigTmpl, nil, true)
	}

	var (
		human = !flagIsSet(c, rawFlag)
		props = make(nvpairList, 0, len(stats.Tracker))
	)
	for k, v := range stats.Tracker {
		props = appendStatToProps(props, k, v.Value, "", regex, human)
	}
	sort.Slice(props, func(i, j int) bool {
		return props[i].Name < props[j].Name
	})

	if node.IsProxy() {
		return tmpls.Print(props, c.App.Writer, tmpls.ConfigTmpl, nil, false)
	}

	// target
	var (
		idx         int
		mpathSorted = make([]string, 0, len(stats.MPCap)) // sort
	)
	for mpath := range stats.MPCap {
		mpathSorted = append(mpathSorted, mpath)
	}
	sort.Strings(mpathSorted)
	for _, mpath := range mpathSorted {
		var (
			mstat  = stats.MPCap[mpath]
			prefix = fmt.Sprintf("mountpath.%d.", idx)
		)
		idx++
		if regex != nil && !regex.MatchString(prefix) {
			continue
		}
		props = append(props,
			nvpair{Name: prefix + "path", Value: mpath},
			nvpair{Name: prefix + "used", Value: fmtStatValue(".size", int64(mstat.Used), human)},
			nvpair{Name: prefix + "avail", Value: fmtStatValue(".size", int64(mstat.Avail), human)},
			nvpair{Name: prefix + "%used", Value: fmt.Sprintf("%d", mstat.PctUsed)})
	}
	return tmpls.Print(props, c.App.Writer, tmpls.ConfigTmpl, nil, false)
}

func showAggregatedStats(c *cli.Context, averageOver time.Duration, regex *regexp.Regexp) error {
	st, err := api.GetClusterStats(apiBP)
	if err != nil {
		return err
	}

	clusterBps(st, averageOver)

	usejs := flagIsSet(c, jsonFlag)
	if usejs {
		return tmpls.Print(st, c.App.Writer, tmpls.TargetMpathListTmpl, nil, usejs)
	}

	human := !flagIsSet(c, rawFlag)
	props := make(nvpairList, 0, len(st.Proxy.Tracker))
	for k, v := range st.Proxy.Tracker {
		props = appendStatToProps(props, k, v.Value, "proxy.", regex, human)
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
		props = appendStatToProps(props, k, v, "target.", regex, human)
	}

	sort.Slice(props, func(i, j int) bool {
		return props[i].Name < props[j].Name
	})

	return tmpls.Print(props, c.App.Writer, tmpls.ConfigTmpl, nil, false)
}
