// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains implementation of the top-level `show` command.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
)

const showJobUsage = "Show running and/or finished jobs,\n" +
	indent1 + "\te.g.:\n" +
	indent1 + "\t- show job tco-cysbohAGL\t- show a given (multi-object copy/transform) job identified by its unique ID;\n" +
	indent1 + "\t- show job copy-listrange\t- show all running multi-object copies;\n" +
	indent1 + "\t- show job copy-objects\t- same as above (using display name);\n" +
	indent1 + "\t- show job copy\t- show all copying jobs including both bucket-to-bucket and multi-object;\n" +
	indent1 + "\t- show job copy-objects --all\t- show both running and already finished (or stopped) multi-object copies;\n" +
	indent1 + "\t- show job list\t- show all running list-objects jobs;\n" +
	indent1 + "\t- show job ls\t- same as above;\n" +
	indent1 + "\t- show job ls --refresh 10\t- same as above with periodic _refreshing_ every 10 seconds;\n" +
	indent1 + "\t- show job ls --refresh 10 --count 4\t- same as above but only for the first four 10-seconds intervals;\n" +
	indent1 + "\t- show job prefetch-listrange\t- show all running prefetch jobs;\n" +
	indent1 + "\t- show job prefetch\t- same as above;\n" +
	indent1 + "\t- show job prefetch --refresh 1m\t- show all running prefetch jobs at 1 minute intervals (until Ctrl-C);\n" +
	indent1 + "\t- show job evict\t- all running bucket and/or data evicting jobs;\n" +
	indent1 + "\t- show job --all\t- show absolutely all jobs, running and finished."

type (
	nodeSnaps struct {
		DaemonID  string
		XactSnaps []*core.Snap
	}

	targetMpath struct {
		DaemonID string
		Mpl      *apc.MountpathList
		Tcdf     fs.Tcdf
	}
)

var (
	showCmdsFlags = map[string][]cli.Flag{
		commandJob: append(
			longRunFlags,
			jsonFlag,
			allJobsFlag,
			regexJobsFlag,
			noHeaderFlag,
			verboseJobFlag,
			unitsFlag,
			dateTimeFlag,
			// download and dsort only
			progressFlag,
			dsortLogFlag,
		),
		cmdObject: {
			objPropsFlag, // --props [list]
			allPropsFlag,
			objNotCachedPropsFlag,
			noHeaderFlag,
			unitsFlag,
			silentFlag,
		},
		cmdCluster: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
			unitsFlag,
			nonverboseFlag,
		),
		cmdSmap: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
		),
		cmdBMD: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
		),
		cmdBucket: {
			jsonFlag,
			compactPropFlag,
			noHeaderFlag,
			addRemoteFlag,
		},
		cmdConfig: {
			jsonFlag,
			noHeaderFlag,
		},
		cmdShowRemoteAIS: {
			noHeaderFlag,
			verboseFlag,
			jsonFlag,
		},
	}

	showCmd = cli.Command{
		Name:  commandShow,
		Usage: "Show configuration, buckets, jobs, etc. - all managed entities in the cluster, and the cluster itself",
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
			showTLS,
		},
	}

	showCmdStorage = cli.Command{
		Name:      commandStorage,
		Usage:     "Show storage usage and utilization, disks and mountpaths",
		ArgsUsage: optionalTargetIDArgument,
		Flags:     sortFlags(storageFlags[commandStorage]),
		Action:    showStorageHandler,
		Subcommands: []cli.Command{
			showCmdDisk,
			showCmdMpath,
			showCmdMpathCapacity,
			showCmdStgSummary,
		},
	}
	showCmdObject = cli.Command{
		Name:         cmdObject,
		Usage:        "Show object properties",
		ArgsUsage:    objectArgument,
		Flags:        sortFlags(showCmdsFlags[cmdObject]),
		Action:       showObjectHandler,
		BashComplete: bucketCompletions(bcmplop{separator: true}),
	}
	showCmdCluster = cli.Command{
		Name:         cmdCluster,
		Usage:        "Main dashboard: show cluster at-a-glance (nodes, software versions, utilization, capacity, memory and more)",
		ArgsUsage:    showClusterArgument,
		Flags:        sortFlags(showCmdsFlags[cmdCluster]),
		Action:       showClusterHandler,
		BashComplete: showClusterCompletions, // NOTE: level 0 hardcoded
		Subcommands: []cli.Command{
			{
				Name:         cmdSmap,
				Usage:        "Show cluster map (Smap)",
				ArgsUsage:    optionalNodeIDArgument,
				Flags:        sortFlags(showCmdsFlags[cmdSmap]),
				Action:       showSmapHandler,
				BashComplete: suggestAllNodes,
			},
			{
				Name:         cmdBMD,
				Usage:        "Show bucket metadata (BMD)",
				ArgsUsage:    optionalNodeIDArgument,
				Flags:        sortFlags(showCmdsFlags[cmdBMD]),
				Action:       showBMDHandler,
				BashComplete: suggestAllNodes,
			},
			{
				Name:      cmdConfig,
				Usage:     "Show cluster and node configuration",
				ArgsUsage: showClusterConfigArgument,
				Flags:     sortFlags(showCmdsFlags[cmdConfig]),
				Action:    showClusterConfigHandler,
			},
			makeAlias(showCmdPeformance, cliName+" "+commandShow+" "+commandPerf, false /*silent*/, cmdShowStats),
		},
	}
	showCmdBucket = cli.Command{
		Name:         cmdBucket,
		Usage:        "Show bucket properties",
		ArgsUsage:    bucketAndPropsArgument,
		Flags:        sortFlags(showCmdsFlags[cmdBucket]),
		Action:       showBckPropsHandler,
		BashComplete: bucketAndPropsCompletions, // bucketCompletions(),
	}
	showCmdConfig = cli.Command{
		Name:         cmdConfig,
		Usage:        "Show CLI, cluster, or node configurations (nodes inherit cluster and have local)",
		ArgsUsage:    showConfigArgument,
		Flags:        sortFlags(showCmdsFlags[cmdConfig]),
		Action:       showAnyConfigHandler,
		BashComplete: showConfigCompletions,
	}
	showCmdRemoteAIS = cli.Command{
		Name:      cmdShowRemoteAIS,
		Usage:     "Show attached AIS clusters",
		ArgsUsage: "",
		Flags:     sortFlags(showCmdsFlags[cmdShowRemoteAIS]),
		Action:    showRemoteAISHandler,
	}

	showCmdJob = cli.Command{
		Name:         commandJob,
		Usage:        showJobUsage,
		ArgsUsage:    jobAnyArg,
		Flags:        sortFlags(showCmdsFlags[commandJob]),
		Action:       showJobsHandler,
		BashComplete: runningJobCompletions,
	}
)

// args [NAME] [JOB_ID] [NODE_ID] [BUCKET] may:
// - be omitted, in part or in total, and may
// - come in arbitrary order
func showJobsHandler(c *cli.Context) error {
	var (
		multimatch                    bool
		l                             int
		name, xid, daemonID, bck, err = jobArgs(c, 0, false /*ignore daemonID*/)
	)
	if err != nil {
		return err
	}
	if name == cmdRebalance {
		return showRebalanceHandler(c)
	}

	if name == "" && xid != "" {
		name, _, multimatch = xid2Name(xid)
	}

	setLongRunParams(c, 72)

	if multimatch {
		var (
			prefix = xid
			cnt    int
		)
		names := xact.ListDisplayNames(false /*only-startable*/)
		sort.Strings(names)
		for _, name = range names {
			if !strings.HasPrefix(name, prefix) { // filter
				continue
			}
			ll, errV := _showJobs(c, name, "" /*xid*/, daemonID, bck, true)
			if errV != nil {
				actionWarn(c, errV.Error())
				err = errV
				cnt++
				if cnt > 1 {
					break
				}
			}
			l += ll
		}
	} else {
		l, err = showJobsDo(c, name, xid, daemonID, bck)
	}

	if err == nil && l == 0 && !flagIsSet(c, allJobsFlag) {
		var (
			what string
			n, h = qflprn(allJobsFlag), qflprn(cli.HelpFlag)
		)
		if name != "" {
			what = " '" + name + "'"
		}
		fmt.Fprintf(c.App.Writer, "No running%s jobs. "+
			"Use %s to show all, %s <TAB-TAB> to select, %s for details.\n", what, n, n, h)
	}
	return err
}

func showJobsDo(c *cli.Context, name, xid, daemonID string, bck cmn.Bck) (int, error) {
	if name != "" || xid != "" {
		return _showJobs(c, name, xid, daemonID, bck, true /*caption*/)
	}

	// special (best-effort)
	if xid != "" {
		switch name {
		case cmdDownload:
			return _showJobs(c, cmdDownload, xid, daemonID, bck, false /*caption, here and elsewhere*/)
		case cmdDsort:
			return _showJobs(c, cmdDsort, xid, daemonID, bck, false)
		case commandETL:
			_, otherID, _ := xid2Name(xid)
			return _showJobs(c, commandETL, otherID /*etl name*/, daemonID, bck, false)
		}
	}

	var (
		ll  int
		cnt int
		err error
	)
	names := xact.ListDisplayNames(false /*only-startable*/)
	sort.Strings(names)
	for _, name = range names {
		l, errV := _showJobs(c, name, "" /*xid*/, daemonID, bck, true)
		if errV != nil {
			actionWarn(c, errV.Error())
			err = errV
			cnt++
			if cnt > 1 {
				break
			}
		}
		ll += l
	}
	return ll, err
}

func _jname(xname, xid string) string { return xname + "[" + xid + "]" }

func jobCptn(c *cli.Context, name, xid, ctlmsg string, onlyActive, byTarget bool) {
	var (
		s, tip string
	)
	if !flagIsSet(c, verboseJobFlag) {
		if _, dtor, err := xact.GetDescriptor(name); err == nil && dtor.ExtendedStats {
			tip = fmt.Sprintf("(tip: use %s to include extended stats)", qflprn(verboseJobFlag))
		}
	}
	if xid != "" {
		jname := _jname(name, xid)
		if ctlmsg != "" {
			actionCptn(c, jname, "(run options:", fcyan(ctlmsg)+")", tip)
		} else {
			actionCptn(c, jname, tip)
		}
		return
	}

	if byTarget {
		s = "by target"
	}
	if onlyActive {
		actionCptn(c, name, "jobs", s, tip)
	} else {
		actionCptn(c, name, "jobs", s, "including finished", tip)
	}
}

func _showJobs(c *cli.Context, name, xid, daemonID string, bck cmn.Bck, caption bool) (int, error) {
	switch name {
	case cmdDownload:
		return showDownloads(c, xid, caption)
	case commandETL:
		return showETLs(c, xid, caption)
	case cmdDsort:
		return showDsorts(c, xid, caption)
	default:
		var (
			// finished or not, always try to show when xid provided
			all         = flagIsSet(c, allJobsFlag) || xact.IsValidUUID(xid)
			onlyActive  = !all
			xactKind, _ = xact.GetKindName(name)
			regexStr    = parseStrFlag(c, regexJobsFlag)
			xargs       = xact.ArgsMsg{
				ID:          xid,
				Kind:        xactKind,
				DaemonID:    daemonID,
				Bck:         bck,
				OnlyRunning: onlyActive,
			}
		)
		if regexStr != "" {
			regex, err := regexp.Compile(regexStr)
			if err != nil {
				actionWarn(c, err.Error())
				regex = nil
			}
			if regex != nil && !regex.MatchString(name) && !regex.MatchString(xactKind) {
				return 0, nil
			}
		}
		return xactList(c, &xargs, caption)
	}
}

func showDownloads(c *cli.Context, id string, caption bool) (int, error) {
	if id == "" { // list all download jobs
		return downloadJobsList(c, parseStrFlag(c, regexJobsFlag), caption)
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
		list, err := api.ListDsort(apiBP, parseStrFlag(c, regexJobsFlag), onlyActive)
		l := len(list)
		if err != nil || l == 0 {
			return l, V(err)
		}
		if caption {
			jobCptn(c, cmdDsort, id, "" /*ctlmsg*/, onlyActive, false)
		}
		return l, dsortJobsList(c, list, usejs)
	}

	return 1, dsortJobStatus(c, id)
}

func showClusterHandler(c *cli.Context) error {
	var (
		what, sid string
		daeType   string
	)
	if c.NArg() > 0 {
		what = c.Args().Get(0)
		if node, _, errV := getNode(c, what); errV == nil {
			sid, what = node.ID(), ""
			daeType = node.Type()
		}
	}
	if c.NArg() > 1 {
		arg := c.Args().Get(1)
		if sid != "" { // already set above, must be the last arg
			if err := errTailArgsContainFlag(c.Args()[1:]); err != nil {
				tip := reorderTailArgs("ais show cluster", c.Args()[1:], sid)
				return fmt.Errorf("%v (tip: try '%s')", err, tip)
			}
			return incorrectUsageMsg(c, "", arg)
		}
		node, _, err := getNode(c, arg)
		if err != nil {
			return err
		}
		sid, daeType = node.ID(), node.Type()
		if err := errTailArgsContainFlag(c.Args()[2:]); err != nil {
			tip := reorderTailArgs("ais show cluster", c.Args()[2:], daeType, sid)
			return fmt.Errorf("%v (tip: try '%s')", err, tip)
		}
	}

	setLongRunParams(c)

	smap, tstatusMap, pstatusMap, err := fillNodeStatusMap(c, daeType)
	if err != nil {
		return err
	}
	cluConfig, err := api.GetClusterConfig(apiBP)
	if err != nil {
		return V(err)
	}

	return cluDaeStatus(c, smap, tstatusMap, pstatusMap, cluConfig, cos.Left(sid, what))
}

func xactList(c *cli.Context, xargs *xact.ArgsMsg, caption bool) (int, error) {
	// override the caller's choice if explicitly identified
	if xargs.ID != "" {
		if !xact.IsValidUUID(xargs.ID) {
			return 0, fmt.Errorf("UUID %q is invalid (typo?)", xargs.ID)
		}
		xargs.OnlyRunning = false
	}

	xs, _, err := queryXactions(xargs, false /*summarize*/)
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

func xlistByKindID(c *cli.Context, xargs *xact.ArgsMsg, caption bool, xs xact.MultiSnap) (int, error) {
	// first, extract snaps for: xargs.ID, Kind
	filteredXs := make(xact.MultiSnap, 8)
	for tid, snaps := range xs {
		for _, snap := range snaps {
			if snap.ID != xargs.ID {
				continue
			}
			debug.Assert(snap.Kind == xargs.Kind)
			if _, ok := filteredXs[tid]; !ok {
				filteredXs[tid] = make([]*core.Snap, 0, 8)
			}
			filteredXs[tid] = append(filteredXs[tid], snap)
		}
	}

	// second, filteredXs => dts templates
	var (
		ctlmsg             string
		fromToBck, haveBck bool
		dts                = make([]nodeSnaps, 0, len(filteredXs))
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

		// a.k.a "run options"
		// try to show more but not too much
		nmsg := snaps[0].CtlMsg
		switch {
		case nmsg == "":
			// do nothing
		case ctlmsg == "":
			ctlmsg = nmsg
		case strings.HasSuffix(ctlmsg, "..."):
			// do nothing
		case strings.Contains(ctlmsg, nmsg):
			// do nothing
		case len(ctlmsg)+len(nmsg) < 60:
			ctlmsg += "; " + nmsg
		default:
			ctlmsg += "; ..."
		}

		dts = append(dts, nodeSnaps{DaemonID: tid, XactSnaps: snaps})
	}
	sort.Slice(dts, func(i, j int) bool {
		return dts[i].DaemonID < dts[j].DaemonID // ascending by node id/name
	})

	_, xname := xact.GetKindName(xargs.Kind)
	if caption {
		jobCptn(c, xname, xargs.ID, ctlmsg, xargs.OnlyRunning, xargs.DaemonID != "")
	}

	l := len(dts)
	var (
		err error

		usejs       = flagIsSet(c, jsonFlag)
		hideHeader  = flagIsSet(c, noHeaderFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
		datedTime   = flagIsSet(c, dateTimeFlag)
	)
	if errU != nil {
		actionWarn(c, errU.Error())
		units = ""
	}
	opts := teb.Opts{AltMap: teb.FuncMapUnits(units, datedTime), UseJSON: usejs}
	switch xargs.Kind {
	case apc.ActECGet:
		if hideHeader {
			err = teb.Print(dts, teb.XactECGetNoHdrTmpl, opts)
		} else {
			err = teb.Print(dts, teb.XactECGetTmpl, opts)
		}
	case apc.ActECPut:
		if hideHeader {
			err = teb.Print(dts, teb.XactECPutNoHdrTmpl, opts)
		} else {
			err = teb.Print(dts, teb.XactECPutTmpl, opts)
		}
	default:
		switch {
		case fromToBck && hideHeader:
			err = teb.Print(dts, teb.XactNoHdrFromToTmpl, opts)
		case fromToBck:
			err = teb.Print(dts, teb.XactFromToTmpl, opts)
		case haveBck && hideHeader:
			err = teb.Print(dts, teb.XactNoHdrBucketTmpl, opts)
		case haveBck:
			err = teb.Print(dts, teb.XactBucketTmpl, opts)
		default:
			if hideHeader {
				err = teb.Print(dts, teb.XactNoHdrNoBucketTmpl, opts)
			} else {
				err = teb.Print(dts, teb.XactNoBucketTmpl, opts)
			}
		}
	}
	if err != nil || !flagIsSet(c, verboseJobFlag) {
		return l, err
	}

	// show in/out stats when verbose
	for _, di := range dts {
		if len(dts[0].XactSnaps) == 0 {
			continue
		}
		var (
			props   = flattenXactStats(di.XactSnaps[0], units)
			_, name = xact.GetKindName(di.XactSnaps[0].Kind)
			err     error
		)
		debug.Assert(name != "", di.XactSnaps[0].Kind)
		actionCptn(c, meta.Tname(di.DaemonID)+":", fmt.Sprintf("%s[%s] stats", name, di.XactSnaps[0].ID))

		if hideHeader {
			err = teb.Print(props, teb.PropValTmplNoHdr, teb.Jopts(usejs))
		} else {
			err = teb.Print(props, teb.PropValTmpl, teb.Jopts(usejs))
		}
		if err != nil {
			return l, err
		}
	}
	return l, nil
}

func showObjectHandler(c *cli.Context) error {
	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in the form "+objectArgument)
	}
	fullObjName := c.Args().Get(0)
	bck, object, err := parseBckObjURI(c, fullObjName, false)
	if err != nil {
		return err
	}
	if _, err := headBucket(bck, true /* don't add */); err != nil {
		return err
	}
	_, err = showObjProps(c, bck, object, false /*silent*/)
	return err
}

func showBckPropsHandler(c *cli.Context) error {
	return showBucketProps(c)
}

func showSmapHandler(c *cli.Context) error {
	var (
		sid              string
		node, sname, err = arg0Node(c)
		smap             *meta.Smap
	)
	if err != nil {
		return err
	}

	setLongRunParams(c)

	if node != nil {
		var out any
		sid = node.ID()
		actionCptn(c, "Cluster map from:", sname)
		out, err = api.GetNodeMeta(apiBP, sid, apc.WhatSmap)
		if err == nil {
			smap = out.(*meta.Smap)
		}
	} else {
		smap, err = getClusterMap(c)
	}
	if err != nil {
		return err // cannot happen
	}
	return smapFromNode(c, smap, sid, flagIsSet(c, jsonFlag))
}

func showBMDHandler(c *cli.Context) error {
	var (
		bmd              *meta.BMD
		sid              string
		node, sname, err = arg0Node(c)
	)
	if err != nil {
		return err
	}

	setLongRunParams(c)

	if node != nil {
		var out any
		sid = node.ID()
		actionCptn(c, "BMD from:", sname)
		out, err = api.GetNodeMeta(apiBP, sid, apc.WhatBMD)
		if err == nil {
			bmd = out.(*meta.BMD)
		}
	} else {
		bmd, err = api.GetBMD(apiBP)
	}
	if err != nil {
		return V(err)
	}

	if bmd.IsEmpty() {
		msg := bmd.StringEx() + " - is empty"
		actionDone(c, msg)
		return nil
	}

	usejs := flagIsSet(c, jsonFlag)
	if usejs {
		return teb.Print(bmd, "", teb.Jopts(usejs))
	}

	tw := &tabwriter.Writer{}
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)
	if !flagIsSet(c, noHeaderFlag) {
		fmt.Fprintln(tw, "PROVIDER\tNAMESPACE\tNAME\tBACKEND\tCOPIES\tEC\tCREATED")
	}
	for provider, namespaces := range bmd.Providers {
		for nsUname, buckets := range namespaces {
			ns := cmn.ParseNsUname(nsUname)
			for bucket, props := range buckets {
				var copies, ec string
				if props.Mirror.Enabled {
					copies = strconv.Itoa(int(props.Mirror.Copies))
				}
				if props.EC.Enabled {
					if props.EC.ObjSizeLimit == cmn.ObjSizeToAlwaysReplicate {
						// no EC - always producing %d total replicas
						ec = fmt.Sprintf("%d-way replication", props.EC.ParitySlices+1)
					} else {
						ec = fmt.Sprintf("D=%d, P=%d (size limit %s)", props.EC.DataSlices,
							props.EC.ParitySlices, cos.ToSizeIEC(props.EC.ObjSizeLimit, 0))
					}
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					provider, ns, bucket, props.BackendBck.String(), copies, ec,
					cos.FormatNanoTime(props.Created, ""))
			}
		}
	}
	tw.Flush()
	fmt.Fprintln(c.App.Writer)
	fmt.Fprintf(c.App.Writer, "Version:\t%d\n", bmd.Version)
	fmt.Fprintf(c.App.Writer, "UUID:\t\t%s\n", bmd.UUID)
	return nil
}

func showClusterConfigHandler(c *cli.Context) error {
	return showClusterConfig(c, c.Args().Get(0))
}

func showAnyConfigHandler(c *cli.Context) error {
	switch {
	case c.NArg() == 0:
		return incorrectUsageMsg(c, "missing arguments (hint: "+tabtab+")")
	case c.Args().Get(0) == cmdCLI:
		return showCfgCLI(c)
	case c.Args().Get(0) == cmdCluster:
		return showClusterConfig(c, c.Args().Get(1))
	default:
		return showNodeConfig(c)
	}
}

// TODO: prune config.ClusterConfig - hide deprecated "non_electable"
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
		return teb.Print(cluConfig, "", teb.Jopts(usejs))
	}

	var flat nvpairList
	if section != "backend" {
		flat = flattenJSON(cluConfig, section)
	} else {
		backends, err := api.GetConfiguredBackends(apiBP)
		if err != nil {
			return V(err)
		}
		flat = flattenBackends(backends)
	}

	// compare w/ headBckTable using the same generic template for bucket props
	if flagIsSet(c, noHeaderFlag) {
		err = teb.Print(flat, teb.PropValTmplNoHdr)
	} else {
		err = teb.Print(flat, teb.PropValTmpl)
	}
	if err != nil {
		return err
	}
	if section == "" {
		msg := fmt.Sprintf("(Tip: use '[SECTION] %s' to show config section(s), see %s for details)",
			flprn(jsonFlag), qflprn(cli.HelpFlag))
		actionDone(c, msg)
		return nil
	}

	// feature flags: show all w/ descriptions
	if section == featureFlagsJname {
		err = printFeatVerbose(c, cluConfig.Features, false /*bucket scope*/)
	}

	return err
}

func showNodeConfig(c *cli.Context) error {
	var (
		section string
		scope   string
		usejs   = flagIsSet(c, jsonFlag)
	)
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	node, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	config, err := api.GetDaemonConfig(apiBP, node)
	if err != nil {
		return V(err)
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

	if section == "backend" {
		// NOTE compare with showClusterConfig above (ref 080235)
		usejs = true
	}
	if usejs {
		opts := teb.Jopts(true)
		warn := "option " + qflprn(jsonFlag) + " won't show node <=> cluster configuration differences, if any."
		switch scope {
		case cfgScopeLocal:
			if section == "" {
				return teb.Print(&config.LocalConfig, "", opts)
			}
			if !printSectionJSON(c, &config.LocalConfig, section) {
				fmt.Fprintln(c.App.Writer)
			}
			return nil
		case cfgScopeInherited:
			actionWarn(c, warn)
			if section == "" {
				return teb.Print(&config.ClusterConfig, "", opts)
			}
			if !printSectionJSON(c, &config.ClusterConfig, section) {
				fmt.Fprintln(c.App.Writer)
			}
			return nil
		default: // cfgScopeAll
			if section == "" {
				actionCptn(c, sname, "local config:")
				if err := teb.Print(&config.LocalConfig, "", opts); err != nil {
					return err
				}
				fmt.Fprintln(c.App.Writer)
				actionCptn(c, sname, "inherited config:")
				actionWarn(c, warn)
				return teb.Print(&config.ClusterConfig, "", opts)
			}
			// fall through on purpose
		}
	}

	usejs = false

	// fill-in `data`
	switch scope {
	case cfgScopeLocal:
		data.LocalConfigPairs = flattenJSON(config.LocalConfig, section)
	default: // cfgScopeInherited | cfgScopeAll
		cluConf, err := api.GetClusterConfig(apiBP)
		if err != nil {
			return V(err)
		}
		// diff cluster <=> this node
		flatNode := flattenJSON(config.ClusterConfig, section)
		flatCluster := flattenJSON(cluConf, section)
		data.ClusterConfigDiff = diffConfigs(flatNode, flatCluster)
		if scope == cfgScopeAll {
			data.LocalConfigPairs = flattenJSON(config.LocalConfig, section)
		}
	}
	// show "flat" diff-s
	if len(data.LocalConfigPairs) == 0 && len(data.ClusterConfigDiff) == 0 {
		fmt.Fprintf(c.App.Writer, "PROPERTY\t VALUE\n\n")
		return nil
	}
	err = teb.Print(data, teb.DaemonConfigTmpl, teb.Jopts(usejs))

	if err == nil && section == "" {
		msg := fmt.Sprintf("(Tip: to show specific section(s), use 'inherited [SECTION]' or 'all [SECTION]' with or without %s)",
			flprn(jsonFlag))
		actionDone(c, msg)
	}
	return err
}

// TODO -- FIXME: check backend.conf <new JSON formatted value>
func showRemoteAISHandler(c *cli.Context) error {
	const (
		warnRemAisOffline = `remote ais cluster at %s is currently unreachable.
Run 'ais config cluster backend.conf --json' - to show the respective configuration;
    'ais config cluster backend.conf <new JSON formatted value>' - to reconfigure or remove.
For details and usage examples, see: docs/cli/config.md`
	)

	all, err := api.GetRemoteAIS(apiBP)
	if err != nil {
		return V(err)
	}
	tw := &tabwriter.Writer{}
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)
	if !flagIsSet(c, noHeaderFlag) {
		fmt.Fprintln(tw, "UUID\tURL\tAlias\tPrimary\tSmap\tTargets\tUptime")
	}
	for _, ra := range all.A {
		uptime := teb.UnknownStatusVal
		bp := api.BaseParams{
			URL:   ra.URL,
			Token: loggedUserToken,
			UA:    ua,
		}
		if cos.IsHTTPS(bp.URL) {
			// NOTE: alternatively, cmn.NewClientTLS(..., TLSArgs{SkipVerify: true})
			bp.Client = clientTLS
		} else {
			bp.Client = clientH
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
			if url != "" && url[0] == '[' && !strings.Contains(url, " ") {
				url = strings.Replace(url, "[", "", 1)
				url = strings.Replace(url, "]", "", 1)
			}
			fmt.Fprintf(tw, "<%s>\t%s\t%s\t%s\t%s\t%s\t%s\n", ra.UUID, url, ra.Alias,
				teb.UnknownStatusVal, teb.UnknownStatusVal, teb.UnknownStatusVal, uptime)

			warn := fmt.Sprintf(warnRemAisOffline, url)

			if len(all.A) == 1 {
				tw.Flush()
				fmt.Fprintln(c.App.Writer)
				return errors.New(warn)
			}
			actionWarn(c, warn+"\n")
		}
	}
	tw.Flush()

	if flagIsSet(c, verboseFlag) {
		for _, ra := range all.A {
			if ra.Smap == nil {
				continue
			}
			fmt.Fprintln(c.App.Writer)
			actionCptn(c, ra.Alias+"["+ra.UUID+"]", "cluster map:")
			err := smapFromNode(c, ra.Smap, "" /*daemonID*/, flagIsSet(c, jsonFlag))
			if err != nil {
				actionWarn(c, err.Error())
			}
		}
	}
	return nil
}
