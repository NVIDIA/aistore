// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

type bsummCtx struct {
	c       *cli.Context
	units   string
	xid     string
	qbck    cmn.QueryBcks
	msg     apc.BsummCtrlMsg
	args    api.BsummArgs
	started int64
	l       int
	n       int
	res     cmn.AllBsummResults
	// waiting
	longWaitTime   time.Duration
	longWaitPrompt string
	dontWait       bool
}

var (
	mpathCmdsFlags = map[string][]cli.Flag{
		cmdMpathAttach: {
			forceFlag,
		},
		cmdMpathEnable: {},
		cmdMpathDetach: {
			noResilverFlag,
		},
		cmdMpathDisable: {
			noResilverFlag,
		},
	}

	mpathCmd = cli.Command{
		Name:   cmdMountpath,
		Usage:  "show and attach/detach target mountpaths",
		Action: showMpathHandler,
		Subcommands: []cli.Command{
			makeAlias(showCmdMpath, "", true, commandShow), // alias for `ais show`
			{
				Name:         cmdMpathAttach,
				Usage:        "attach mountpath (i.e., formatted disk or RAID) to a target node",
				ArgsUsage:    nodeMountpathPairArgument,
				Flags:        mpathCmdsFlags[cmdMpathAttach],
				Action:       mpathAttachHandler,
				BashComplete: suggestTargets,
			},
			{
				Name:         cmdMpathEnable,
				Usage:        "(re)enable target's mountpath",
				ArgsUsage:    nodeMountpathPairArgument,
				Flags:        mpathCmdsFlags[cmdMpathEnable],
				Action:       mpathEnableHandler,
				BashComplete: func(c *cli.Context) { suggestTargetMpath(c, cmdMpathEnable) },
			},
			{
				Name:         cmdMpathDetach,
				Usage:        "detach mountpath (i.e., formatted disk or RAID) from a target node",
				ArgsUsage:    nodeMountpathPairArgument,
				Flags:        mpathCmdsFlags[cmdMpathDetach],
				Action:       mpathDetachHandler,
				BashComplete: func(c *cli.Context) { suggestTargetMpath(c, cmdMpathDetach) },
			},
			{
				Name:         cmdMpathDisable,
				Usage:        "disable mountpath (deactivate but keep in a target's volume)",
				ArgsUsage:    nodeMountpathPairArgument,
				Flags:        mpathCmdsFlags[cmdMpathDisable],
				Action:       mpathDisableHandler,
				BashComplete: func(c *cli.Context) { suggestTargetMpath(c, cmdMpathDisable) },
			},
		},
	}
)

var (
	cleanupFlags = []cli.Flag{
		waitFlag,
		waitJobXactFinishedFlag,
	}
	cleanupCmd = cli.Command{
		Name:         cmdStgCleanup,
		Usage:        "perform storage cleanup: remove deleted objects and old/obsolete workfiles",
		ArgsUsage:    listAnyCommandArgument,
		Flags:        cleanupFlags,
		Action:       cleanupStorageHandler,
		BashComplete: bucketCompletions(bcmplop{}),
	}
)

var (
	storageSummFlags = append(
		longRunFlags,
		bsummPrefixFlag,
		listObjCachedFlag,
		unitsFlag,
		verboseFlag,
		dontWaitFlag,
		noHeaderFlag,
	)
	storageFlags = map[string][]cli.Flag{
		commandStorage: append(
			longRunFlags,
			jsonFlag,
		),
		cmdShowDisk: append(
			longRunFlags,
			noHeaderFlag,
			unitsFlag,
			regexColsFlag,
			diskSummaryFlag,
		),
		cmdMountpath: append(
			longRunFlags,
			jsonFlag,
		),
		cmdStgValidate: append(
			longRunFlags,
			waitJobXactFinishedFlag,
		),
	}

	//
	// `show storage` sub-commands
	//
	showCmdDisk = cli.Command{
		Name:         cmdShowDisk,
		Usage:        "show disk utilization and read/write statistics",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        storageFlags[cmdShowDisk],
		Action:       showDisksHandler,
		BashComplete: suggestTargets,
	}
	showCmdStgSummary = cli.Command{
		Name:         cmdSummary,
		Usage:        "show bucket sizes and %% of used capacity on a per-bucket basis",
		ArgsUsage:    listAnyCommandArgument,
		Flags:        storageSummFlags,
		Action:       summaryStorageHandler,
		BashComplete: bucketCompletions(bcmplop{}),
	}
	showCmdMpath = cli.Command{
		Name:         cmdMountpath,
		Usage:        "show target mountpaths",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        storageFlags[cmdMountpath],
		Action:       showMpathHandler,
		BashComplete: suggestTargets,
	}

	storageCmd = cli.Command{
		Name:  commandStorage,
		Usage: "monitor and manage clustered storage",
		Subcommands: []cli.Command{
			makeAlias(showCmdStorage, "", true, commandShow), // alias for `ais show`
			showCmdStgSummary,
			{
				Name:         cmdStgValidate,
				Usage:        "check buckets for misplaced objects and objects that have insufficient numbers of copies or EC slices",
				ArgsUsage:    listAnyCommandArgument,
				Flags:        storageFlags[cmdStgValidate],
				Action:       showMisplacedAndMore,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			mpathCmd,
			showCmdDisk,
			cleanupCmd,
		},
	}
)

func showStorageHandler(c *cli.Context) (err error) {
	return showDiskStats(c, "") // all targets, all disks
}

//
// cleanup
//

func cleanupStorageHandler(c *cli.Context) (err error) {
	var (
		bck cmn.Bck
		id  string
	)
	if c.NArg() != 0 {
		bck, err = parseBckURI(c, c.Args().Get(0), false)
		if err != nil {
			return
		}
		if _, err = headBucket(bck, true /* don't add */); err != nil {
			return
		}
	}
	xargs := xact.ArgsMsg{Kind: apc.ActStoreCleanup, Bck: bck}
	if id, err = api.StartXaction(apiBP, &xargs); err != nil {
		return
	}

	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		if id != "" {
			fmt.Fprintf(c.App.Writer, "Started storage cleanup %q. %s\n", id, toMonitorMsg(c, id, ""))
		} else {
			fmt.Fprintf(c.App.Writer, "Started storage cleanup\n")
		}
		return
	}

	fmt.Fprintf(c.App.Writer, "Started storage cleanup %s...\n", id)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		xargs.Timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	if err := waitXact(apiBP, &xargs); err != nil {
		return err
	}
	fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	return nil
}

//
// disk
//

func showDisksHandler(c *cli.Context) error {
	var (
		tid             string
		tsi, sname, err = arg0Node(c)
	)
	if err != nil {
		return err
	}
	if tsi != nil {
		if tsi.IsProxy() {
			const s = "(AIS gateways do not store user data and do not have any data drives)"
			return fmt.Errorf("%s is a 'proxy' aka gateway %s", sname, s)
		}
		tid = tsi.ID()
	}
	return showDiskStats(c, tid)
}

func showDiskStats(c *cli.Context, tid string) error {
	var (
		regex       *regexp.Regexp
		regexStr    = parseStrFlag(c, regexColsFlag)
		hideHeader  = flagIsSet(c, noHeaderFlag)
		summary     = flagIsSet(c, diskSummaryFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}
	setLongRunParams(c, 72)

	smap, err := getClusterMap(c)
	if err != nil {
		return err
	}
	numTs := smap.CountActiveTs()
	if numTs == 0 {
		return cmn.NewErrNoNodes(apc.Target, smap.CountTargets())
	}
	if tid != "" {
		numTs = 1
	}
	if regexStr != "" {
		regex, err = regexp.Compile(regexStr)
		if err != nil {
			return err
		}
	}

	dsh, err := getDiskStats(smap, tid)
	if err != nil {
		return err
	}

	// collapse target disks
	if summary {
		collapseDisks(dsh, numTs)
	}

	// tally up
	// TODO: check config.TestingEnv (or DeploymentType == apc.DeploymentDev)
	var totalsHdr string
	if l := int64(len(dsh)); l > 1 {
		totalsHdr = cluTotal
		if tid != "" {
			totalsHdr = tgtTotal
		}
		tally := teb.DiskStatsHelper{TargetID: totalsHdr}
		for _, ds := range dsh {
			tally.Stat.RBps += ds.Stat.RBps
			tally.Stat.Ravg += ds.Stat.Ravg
			tally.Stat.WBps += ds.Stat.WBps
			tally.Stat.Wavg += ds.Stat.Wavg
			tally.Stat.Util += ds.Stat.Util
		}
		tally.Stat.Ravg = cos.DivRound(tally.Stat.Ravg, l)
		tally.Stat.Wavg = cos.DivRound(tally.Stat.Wavg, l)
		tally.Stat.Util = cos.DivRound(tally.Stat.Util, l)

		dsh = append(dsh, tally)
	}

	table := teb.NewDiskTab(dsh, smap, regex, units, totalsHdr)
	out := table.Template(hideHeader)
	return teb.Print(dsh, out)
}

//
// summary (compare with `listBckTableWithSummary` - fast)
//

func summaryStorageHandler(c *cli.Context) error {
	uri := c.Args().Get(0)
	qbck, errV := parseQueryBckURI(c, uri)
	if errV != nil {
		return errV
	}

	units, errU := parseUnitsFlag(c, unitsFlag)
	if errU != nil {
		return errU
	}

	// TODO: remote buckets not in BMD - see ais ls --summary and `listBckTableWithSummary`
	dontWait := flagIsSet(c, dontWaitFlag)
	ctx := newBsummContext(c, units, qbck, true /*bckPresent*/, dontWait)

	setLongRunParams(c)

	var news = true
	if xid := c.Args().Get(1); xid != "" && cos.IsValidUUID(xid) {
		ctx.msg.UUID = xid
		news = false
	}
	xid, summaries, err := ctx.slow() // execute

	f := func() string {
		verb := "has started"
		if !news {
			verb = "is running"
		}
		return fmt.Sprintf("Job %s[%s] %s. To monitor, run 'ais storage summary %s %s %s' or 'ais show job %s';\n"+
			"see %s for more options",
			cmdSummary, xid, verb, uri, xid, flprn(dontWaitFlag), xid, qflprn(cli.HelpFlag))
	}
	if err == nil && dontWait && len(summaries) == 0 {
		actionDone(c, f())
		return nil
	}

	var status int
	if err != nil {
		if herr, ok := err.(*cmn.ErrHTTP); ok {
			status = herr.Status
		}
		if dontWait && status == http.StatusAccepted {
			actionDone(c, f())
			return nil
		}
		if dontWait && status == http.StatusPartialContent {
			msg := fmt.Sprintf("%s[%s] is still running - showing partial results:", cmdSummary, ctx.msg.UUID)
			actionNote(c, msg)
			err = nil
		}
	}
	if err != nil {
		return err
	}

	altMap := teb.FuncMapUnits(units)
	opts := teb.Opts{AltMap: altMap}
	hideHeader := flagIsSet(c, noHeaderFlag)
	if hideHeader {
		return teb.Print(summaries, teb.BucketsSummariesBody, opts)
	}
	return teb.Print(summaries, teb.BucketsSummariesTmpl, opts)
}

func newBsummContext(c *cli.Context, units string, qbck cmn.QueryBcks, bckPresent, dontWait bool) *bsummCtx {
	ctx := &bsummCtx{
		c:        c,
		units:    units,
		qbck:     qbck,
		started:  mono.NanoTime(),
		dontWait: dontWait,
	}
	ctx.msg.Prefix = parseStrFlag(c, bsummPrefixFlag)
	ctx.msg.ObjCached = flagIsSet(c, listObjCachedFlag)
	ctx.msg.BckPresent = bckPresent

	ctx.args.DontWait = dontWait

	if flagIsSet(c, refreshFlag) {
		ctx.args.CallAfter = parseDurationFlag(c, refreshFlag)
		ctx.args.Callback = ctx.progress
		ctx.longWaitTime = 0 // have refresh callback
	} else if !dontWait {
		ctx.longWaitTime = listObjectsWaitTime
		if ctx.msg.UUID != "" {
			ctx.longWaitPrompt = "Please wait, the operation may take some time.\n" +
				"To monitor, run 'ais storage summary " + ctx.msg.UUID // TODO -- FIXME
		}
	}
	return ctx
}

// "slow" version of the bucket-summary (compare with `listBuckets` => `listBckTableWithSummary`)
func (ctx *bsummCtx) slow() (xid string, res cmn.AllBsummResults, err error) {
	if ctx.longWaitTime > 0 {
		err = waitForFunc(ctx.get, ctx.longWaitTime, ctx.longWaitPrompt)
	} else {
		err = ctx.get()
	}
	xid, res = ctx.xid, ctx.res
	return
}

func (ctx *bsummCtx) get() (err error) {
	ctx.xid, ctx.res, err = api.GetBucketSummary(apiBP, ctx.qbck, &ctx.msg, ctx.args)
	return
}

// re-print line per bucket
func (ctx *bsummCtx) progress(summaries *cmn.AllBsummResults, done bool) {
	if done {
		if ctx.n > 0 {
			fmt.Fprintln(ctx.c.App.Writer)
		}
		return
	}
	if summaries == nil {
		return
	}
	results := *summaries
	if len(results) == 0 {
		return
	}
	ctx.n++

	// format out
	elapsed := mono.SinceNano(ctx.started)
	for i, res := range results {
		s := res.Bck.Cname("") + ": "
		if res.ObjCount.Present == 0 && res.ObjCount.Remote == 0 {
			s += "is empty"
			goto emit
		}
		if res.Bck.IsAIS() {
			debug.Assert(res.ObjCount.Remote == 0 && res.ObjCount.Present != 0)
			s += fmt.Sprintf("(%s, size=%s)", cos.FormatBigNum(int(res.ObjCount.Present)),
				teb.FmtSize(int64(res.TotalSize.PresentObjs), ctx.units, 2))
			goto emit
		}

		// cloud bucket
		if res.ObjCount.Present == 0 {
			s += "[cluster: none"
		} else {
			s += fmt.Sprintf("[cluster: (%s, size=%s)",
				cos.FormatBigNum(int(res.ObjCount.Present)), teb.FmtSize(int64(res.TotalSize.PresentObjs), ctx.units, 2))
		}
		if res.ObjCount.Remote == 0 {
			s += "]"
		} else {
			s += fmt.Sprintf(", remote: (%s, size=%s)]",
				cos.FormatBigNum(int(res.ObjCount.Remote)), teb.FmtSize(int64(res.TotalSize.RemoteObjs), ctx.units, 2))
		}
		s += ", " + teb.FmtDuration(elapsed, ctx.units)

	emit:
		if ctx.l < len(s) {
			ctx.l = len(s) + 4
		}
		s += strings.Repeat(" ", ctx.l-len(s))
		fmt.Fprintf(ctx.c.App.Writer, "\r%s", s)

		if len(results) > 1 {
			if i < len(results)-1 {
				time.Sleep(3 * time.Second)
			}
		}
	}
}

//
// mountpath
//

func showMpathHandler(c *cli.Context) error {
	var (
		nodes           []*meta.Snode
		tsi, sname, err = arg0Node(c)
	)
	if err != nil {
		return err
	}
	if tsi != nil {
		if tsi.IsProxy() {
			return fmt.Errorf("node %s is a proxy (expecting target)", sname)
		}
	}
	setLongRunParams(c)

	smap, tstatusMap, _, err := fillNodeStatusMap(c, apc.Target)
	if err != nil {
		return err
	}
	if tsi != nil {
		nodes = []*meta.Snode{tsi}
	} else {
		nodes = make(meta.Nodes, 0, len(smap.Tmap))
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
		go func(node *meta.Snode) {
			mpl, err := api.GetMountpaths(apiBP, node)
			if err != nil {
				erCh <- err
			} else {
				mpCh <- &targetMpath{
					DaemonID:  node.ID(),
					Mpl:       mpl,
					TargetCDF: tstatusMap[node.ID()].TargetCDF,
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
	return teb.Print(mpls, teb.MpathListTmpl, teb.Jopts(usejs))
}

func mpathAttachHandler(c *cli.Context) (err error)  { return mpathAction(c, apc.ActMountpathAttach) }
func mpathEnableHandler(c *cli.Context) (err error)  { return mpathAction(c, apc.ActMountpathEnable) }
func mpathDetachHandler(c *cli.Context) (err error)  { return mpathAction(c, apc.ActMountpathDetach) }
func mpathDisableHandler(c *cli.Context) (err error) { return mpathAction(c, apc.ActMountpathDisable) }

func mpathAction(c *cli.Context, action string) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	smap, err := getClusterMap(c)
	if err != nil {
		return err
	}
	kvs, err := makePairs(c.Args())
	if err != nil {
		// check whether user typed target ID with no mountpath
		first, tail, nodeID := c.Args().Get(0), c.Args().Tail(), ""
		if len(tail) == 0 {
			nodeID = first
		} else {
			nodeID = tail[len(tail)-1]
		}
		nodeID = meta.N2ID(nodeID)
		if nodeID != "" && smap.GetTarget(nodeID) != nil {
			return fmt.Errorf("target %s: missing mountpath to %s", first, action)
		}
		return err
	}
	for nodeID, mountpath := range kvs {
		var (
			err   error
			acted string
		)
		nodeID = meta.N2ID(nodeID)
		si := smap.GetTarget(nodeID)
		if si == nil {
			si = smap.GetProxy(nodeID)
			if si == nil {
				return &errDoesNotExist{what: "node", name: nodeID}
			}
			return fmt.Errorf("node %q is a proxy "+
				"(hint: press <TAB-TAB> or run \"ais show cluster target\" to select a target)", nodeID)
		}
		switch action {
		case apc.ActMountpathAttach:
			acted = "attached"
			err = api.AttachMountpath(apiBP, si, mountpath, flagIsSet(c, forceFlag))
		case apc.ActMountpathEnable:
			acted = "enabled"
			err = api.EnableMountpath(apiBP, si, mountpath)
		case apc.ActMountpathDetach:
			acted = "detached"
			err = api.DetachMountpath(apiBP, si, mountpath, flagIsSet(c, noResilverFlag))
		case apc.ActMountpathDisable:
			acted = "disabled"
			err = api.DisableMountpath(apiBP, si, mountpath, flagIsSet(c, noResilverFlag))
		default:
			return incorrectUsageMsg(c, "invalid mountpath action %q", action)
		}
		if err != nil {
			return err
		}
		fmt.Fprintf(c.App.Writer, "Node %q %s mountpath %q\n", si.ID(), acted, mountpath)
	}
	return nil
}
