// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

// top-level job command
var (
	jobCmd = cli.Command{
		Name:        commandJob,
		Usage:       "monitor, query, start/stop and manage jobs and eXtended actions (xactions)",
		Subcommands: jobSub,
	}
	// NOTE: `appendJobSub` (below) expects jobSub[0] to be the `jobStartSub`
	jobSub = []cli.Command{
		jobStartSub,
		jobStopSub,
		jobWaitSub,
		jobRemoveSub,
		makeAlias(showCmdJob, "", true, commandShow), // alias for `ais show`
	}
)

// job start
var (
	startCommonFlags = []cli.Flag{
		waitFlag,
		waitJobXactFinishedFlag,
	}
	startSpecialFlags = map[string][]cli.Flag{
		cmdDownload: {
			timeoutFlag,
			descJobFlag,
			limitConnectionsFlag,
			objectsListFlag,
			downloadProgressFlag,
			progressFlag,
			waitFlag,
			waitJobXactFinishedFlag,
			limitBytesPerHourFlag,
			syncFlag,
		},
		cmdDsort: {
			dsortSpecFlag,
		},
		commandPrefetch: append(
			listrangeFlags,
			dryRunFlag,
		),
		cmdLRU: {
			listBucketsFlag,
			forceFlag,
		},
	}

	jobStartResilver = cli.Command{
		Name: commandResilver,
		Usage: "resilver user data on a given target (or all targets in the cluster): fix data redundancy\n" +
			argsUsageIndent + "with respect to bucket configuration, remove migrated objects and old/obsolete workfiles",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        startCommonFlags,
		Action:       startResilverHandler,
		BashComplete: suggestTargetNodes,
	}
	jobStartSub = cli.Command{
		Name:  commandStart,
		Usage: "run batch job",
		Subcommands: []cli.Command{
			{
				Name:         commandPrefetch,
				Usage:        "prefetch objects from remote buckets",
				ArgsUsage:    bucketArgument,
				Flags:        startSpecialFlags[commandPrefetch],
				Action:       startPrefetchHandler,
				BashComplete: bucketCompletions(bcmplop{multiple: true}),
			},
			{
				Name:      cmdDownload,
				Usage:     "download files and objects from remote sources",
				ArgsUsage: startDownloadArgument,
				Flags:     startSpecialFlags[cmdDownload],
				Action:    startDownloadHandler,
			},
			{
				Name:      cmdDsort,
				Usage:     "start " + dsort.DSortName + " job",
				ArgsUsage: jsonSpecArgument,
				Flags:     startSpecialFlags[cmdDsort],
				Action:    startDsortHandler,
			},
			{
				Name:         cmdLRU,
				Usage:        "run LRU eviction",
				Flags:        startSpecialFlags[cmdLRU],
				Action:       startLRUHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			{
				Name:         cmdStgCleanup,
				Usage:        "cleanup storage: remove migrated and deleted objects, old/obsolete workfiles",
				ArgsUsage:    listAnyCommandArgument,
				Flags:        storageCmdFlags[cmdStgCleanup],
				Action:       cleanupStorageHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			{
				Name:  commandETL,
				Usage: "start ETL",
				Subcommands: []cli.Command{
					initCmdETL,
					objCmdETL,
					bckCmdETL,
				},
			},
			{
				Name:   commandRebalance,
				Usage:  "rebalance ais cluster",
				Flags:  clusterCmdsFlags[commandStart],
				Action: startClusterRebalanceHandler,
			},
			jobStartResilver,
			// NOTE: append all `startableXactions`
		},
	}
)

// ais stop
var (
	stopCmdsFlags = []cli.Flag{
		allRunningJobsFlag,
		regexJobsFlag,
	}
	jobStopSub = cli.Command{
		Name:         commandStop,
		Usage:        "terminate a single batch job or multiple jobs (" + tabHelpOpt + ")",
		ArgsUsage:    jobShowStopWaitArgument,
		Flags:        stopCmdsFlags,
		Action:       stopJobHandler,
		BashComplete: runningJobCompletions,
	}
)

// ais wait
var (
	waitCmdsFlags = []cli.Flag{
		refreshFlag,
		progressFlag,
		waitJobXactFinishedFlag,
	}
	jobWaitSub = cli.Command{
		Name:         commandWait,
		Usage:        "wait for a specific batch job to complete (" + tabHelpOpt + ")",
		ArgsUsage:    jobShowStopWaitArgument,
		Flags:        waitCmdsFlags,
		Action:       waitJobHandler,
		BashComplete: runningJobCompletions,
	}
)

// ais job remove
var (
	removeCmdsFlags = []cli.Flag{
		allFinishedJobsFlag,
		regexJobsFlag,
	}
	jobRemoveSub = cli.Command{
		Name:  commandRemove,
		Usage: "cleanup finished jobs",
		Subcommands: []cli.Command{
			{
				Name:         cmdDownload,
				Usage:        "remove finished download job(s)",
				ArgsUsage:    optionalJobIDArgument,
				Flags:        removeCmdsFlags,
				Action:       removeDownloadHandler,
				BashComplete: downloadIDFinishedCompletions,
			},
			{
				Name:         cmdDsort,
				Usage:        "remove finished dsort job(s)",
				ArgsUsage:    optionalJobIDArgument,
				Flags:        removeCmdsFlags,
				Action:       removeDsortHandler,
				BashComplete: dsortIDFinishedCompletions,
			},
		},
	}
)

func appendJobSub(jobcmd *cli.Command) {
	debug.Assert(jobcmd.Subcommands[0].Name == commandStart)

	// append to `jobStartSub`, add to `ais start` (== `ais job start`)
	cmds := jobcmd.Subcommands[0].Subcommands
	cmds = append(cmds, storageSvcCmds...)
	cmds = append(cmds, startableXactions(cmds)...)
	jobcmd.Subcommands[0].Subcommands = cmds
}

func startableXactions(explDefinedCmds cli.Commands) cli.Commands {
	var (
		cmds      = make(cli.Commands, 0, 8)
		startable = xact.ListDisplayNames(true /*onlyStartable*/)
	)
outer:
	for _, xname := range startable {
		for i := range explDefinedCmds {
			cmd := &explDefinedCmds[i]
			kind, _ := xact.GetKindName(xname)
			// CLI is allowed to make it even shorter..
			if strings.HasPrefix(xname, cmd.Name) || strings.HasPrefix(kind, cmd.Name) {
				continue outer
			}
		}
		cmd := cli.Command{
			Name:   xname,
			Usage:  fmt.Sprintf("start %s", xname),
			Flags:  startCommonFlags,
			Action: startXactionHandler,
		}
		if xact.IsSameScope(xname, xact.ScopeB) { // with a single arg: bucket
			cmd.ArgsUsage = bucketArgument
			cmd.BashComplete = bucketCompletions(bcmplop{})
		} else if xact.IsSameScope(xname, xact.ScopeGB) { // with a single optional arg: bucket
			cmd.ArgsUsage = optionalBucketArgument
			cmd.BashComplete = bucketCompletions(bcmplop{})
		}
		cmds = append(cmds, cmd)
	}

	// and in addition:
	cmd := bucketCmdCopy
	cmd.Name, _ = xact.GetKindName(apc.ActCopyBck)
	cmds = append(cmds, cmd)

	cmd = bucketCmdRename
	cmd.Name, _ = xact.GetKindName(apc.ActMoveBck)
	cmds = append(cmds, cmd)

	return cmds
}

//
// job start
//

// via `startableXactions()`
func startXactionHandler(c *cli.Context) (err error) {
	xname := c.Command.Name
	return startXactionKind(c, xname)
}

func startXactionKind(c *cli.Context, xname string) (err error) {
	var bck cmn.Bck
	if c.NArg() == 0 && xact.IsSameScope(xname, xact.ScopeB) {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if c.NArg() > 0 && xact.IsSameScope(xname, xact.ScopeB, xact.ScopeGB) {
		bck, err = parseBckURI(c, c.Args().First(), true /*require provider*/)
		if err != nil {
			return err
		}
	}
	return startXaction(c, xname, bck, "" /*sid*/)
}

func startResilverHandler(c *cli.Context) (err error) {
	var sid string
	if c.NArg() > 0 {
		sid, _, err = getNodeIDName(c, c.Args().First())
		if err != nil {
			return
		}
	}
	return startXaction(c, apc.ActResilver, cmn.Bck{}, sid)
}

func startXaction(c *cli.Context, xname string, bck cmn.Bck, sid string) error {
	if !bck.IsQuery() {
		if _, err := headBucket(bck, false /* don't add */); err != nil {
			return err
		}
	} else if xact.IsSameScope(xname, xact.ScopeB) {
		return fmt.Errorf("%q requires bucket to run", xname)
	}

	xargs := xact.ArgsMsg{Kind: xname, Bck: bck, DaemonID: sid}
	xid, err := api.StartXaction(apiBP, xargs)
	if err != nil {
		return err
	}
	if xid == "" {
		warn := fmt.Sprintf("The operation returned an empty UUID (a no-op?). %s\n",
			toShowMsg(c, "", "To investigate", false))
		actionWarn(c, warn)
		return nil
	}

	debug.Assert(xact.IsValidUUID(xid), xid)
	msg := fmt.Sprintf("Started %s[%s]. %s", xname, xid, toMonitorMsg(c, xid, ""))
	actionDone(c, msg)

	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		return nil
	}
	return waitJob(c, xname, xid, bck)
}

func startDownloadHandler(c *cli.Context) error {
	var (
		description      = parseStrFlag(c, descJobFlag)
		timeout          = parseStrFlag(c, timeoutFlag)
		objectsListPath  = parseStrFlag(c, objectsListFlag)
		progressInterval = parseStrFlag(c, downloadProgressFlag)
		id               string
	)
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if c.NArg() == 1 {
		return missingArgumentsError(c, "destination")
	}
	if c.NArg() > 2 {
		const q = "For range download, enclose source in quotation marks, e.g.: \"gs://imagenet/train-{00..99}.tgz\""
		s := fmt.Sprintf("too many arguments - expected 2, got %d.\n%s", len(c.Args()), q)
		return &errUsage{
			context:      c,
			message:      s,
			helpData:     c.Command,
			helpTemplate: cli.CommandHelpTemplate,
		}
	}

	src, dst := c.Args().Get(0), c.Args().Get(1)
	source, err := parseSource(src)
	if err != nil {
		return err
	}
	bck, pathSuffix, err := parseDest(c, dst)
	if err != nil {
		return err
	}

	limitBPH, err := parseHumanSizeFlag(c, limitBytesPerHourFlag)
	if err != nil {
		return err
	}

	if _, err := time.ParseDuration(progressInterval); err != nil {
		return err
	}

	basePayload := dload.Base{
		Bck:              bck,
		Timeout:          timeout,
		Description:      description,
		ProgressInterval: progressInterval,
		Limits: dload.Limits{
			Connections:  parseIntFlag(c, limitConnectionsFlag),
			BytesPerHour: int(limitBPH),
		},
	}

	if basePayload.Bck.Props, err = api.HeadBucket(apiBP, basePayload.Bck, true /* don't add */); err != nil {
		if !cmn.IsStatusNotFound(err) {
			return err
		}
		warn := fmt.Sprintf("destination bucket %s doesn't exist. Bucket with default properties will be created.",
			basePayload.Bck.DisplayName())
		actionWarn(c, warn)
	}

	// Heuristics to determine the download type.
	var dlType dload.Type
	if objectsListPath != "" {
		dlType = dload.TypeMulti
	} else if strings.Contains(source.link, "{") && strings.Contains(source.link, "}") {
		dlType = dload.TypeRange
	} else if source.backend.bck.IsEmpty() {
		dlType = dload.TypeSingle
	} else {
		cfg, err := getRandTargetConfig(c)
		if err != nil {
			return err
		}
		if _, ok := cfg.Backend.Providers[source.backend.bck.Provider]; ok { // backend is configured
			dlType = dload.TypeBackend

			p, err := api.HeadBucket(apiBP, basePayload.Bck, false /* don't add */)
			if err != nil {
				return err
			}
			if !p.BackendBck.Equal(&source.backend.bck) {
				warn := fmt.Sprintf("%s does not have Cloud bucket %s as its *backend* - proceeding to download anyway.",
					basePayload.Bck, source.backend.bck)
				actionWarn(c, warn)
				dlType = dload.TypeSingle
			}
		} else if source.backend.prefix == "" {
			return fmt.Errorf(
				"cluster is not configured with %q provider: cannot download remote bucket",
				source.backend.bck.Provider,
			)
		} else {
			if source.link == "" {
				return fmt.Errorf(
					"cluster is not configured with %q provider: cannot download bucket's objects",
					source.backend.bck.Provider,
				)
			}
			// If `prefix` is not empty then possibly it is just a single object
			// which we can download without cloud to be configured (web link).
			dlType = dload.TypeSingle
		}
	}

	switch dlType {
	case dload.TypeSingle:
		payload := dload.SingleBody{
			Base: basePayload,
			SingleObj: dload.SingleObj{
				Link:    source.link,
				ObjName: pathSuffix, // in this case pathSuffix is a full name of the object
			},
		}
		id, err = api.DownloadWithParam(apiBP, dlType, payload)
	case dload.TypeMulti:
		var objects []string
		{
			file, err := os.Open(objectsListPath)
			if err != nil {
				return err
			}
			if err := jsoniter.NewDecoder(file).Decode(&objects); err != nil {
				return fmt.Errorf("file %q doesn't seem to contain JSON array of strings: %v", objectsListPath, err)
			}
		}
		for i, object := range objects {
			objects[i] = source.link + "/" + object
		}
		payload := dload.MultiBody{
			Base:           basePayload,
			ObjectsPayload: objects,
		}
		id, err = api.DownloadWithParam(apiBP, dlType, payload)
	case dload.TypeRange:
		payload := dload.RangeBody{
			Base:     basePayload,
			Subdir:   pathSuffix, // in this case pathSuffix is a subdirectory in which the objects are to be saved
			Template: source.link,
		}
		id, err = api.DownloadWithParam(apiBP, dlType, payload)
	case dload.TypeBackend:
		payload := dload.BackendBody{
			Base:   basePayload,
			Sync:   flagIsSet(c, syncFlag),
			Prefix: source.backend.prefix,
		}
		id, err = api.DownloadWithParam(apiBP, dlType, payload)
	default:
		debug.Assert(false)
	}

	if err != nil {
		return err
	}

	fmt.Fprintf(c.App.Writer, "Started download job %s\n", id)

	if flagIsSet(c, progressFlag) {
		return pbDownload(c, id)
	}

	if flagIsSet(c, waitFlag) || flagIsSet(c, waitJobXactFinishedFlag) {
		return wtDownload(c, id)
	}

	return bgDownload(c, id)
}

func pbDownload(c *cli.Context, id string) (err error) {
	refreshRate := _refreshRate(c)
	downloadingResult, err := newDownloaderPB(apiBP, id, refreshRate).run()
	if err != nil {
		return err
	}

	fmt.Fprintln(c.App.Writer, downloadingResult)
	return nil
}

func wtDownload(c *cli.Context, id string) error {
	if err := waitDownload(c, id); err != nil {
		return err
	}
	resp, err := api.DownloadStatus(apiBP, id, true /*only active*/)
	if err != nil {
		return err
	}
	if resp.ErrorCnt != 0 {
		msg := toShowMsg(c, id, "For details", true)
		warn := fmt.Sprintf("%d of %d download jobs failed. %s", resp.ErrorCnt, resp.ScheduledCnt, msg)
		actionWarn(c, warn)
	} else {
		actionDownloaded(c, resp.FinishedCnt)
	}
	return nil
}

func actionDownloaded(c *cli.Context, cnt int) {
	var msg string
	if cnt == 1 {
		msg = "File successfully downloaded"
	} else {
		msg = fmt.Sprintf("All %d files successfully downloaded", cnt)
	}
	actionDone(c, msg)
}

func bgDownload(c *cli.Context, id string) (err error) {
	var (
		resp           *dload.StatusResp
		startedTimeout = cos.MaxDuration(5*time.Second, refreshRateMinDur*2)
	)

	// In a non-interactive mode, allow downloader to start before checking
	for elapsed := time.Duration(0); elapsed < startedTimeout; elapsed += refreshRateMinDur {
		time.Sleep(refreshRateMinDur)
		resp, err = api.DownloadStatus(apiBP, id, true /*only active*/)
		if err != nil {
			return err
		}
		if resp.ErrorCnt != 0 || resp.FinishedCnt != 0 || resp.FinishedTime.UnixNano() != 0 {
			break
		}
	}

	if resp.ErrorCnt != 0 {
		msg := toShowMsg(c, id, "For details", true)
		warn := fmt.Sprintf("%d of %d download jobs failed. %sBarFlag)s", resp.ErrorCnt, resp.ScheduledCnt, msg)
		actionWarn(c, warn)
	} else if resp.FinishedTime.UnixNano() != 0 {
		actionDownloaded(c, resp.FinishedCnt)
	} else {
		msg := toMonitorMsg(c, id, flprn(progressFlag)+"'")
		actionDone(c, msg)
	}
	return err
}

func waitDownload(c *cli.Context, id string) (err error) {
	var (
		elapsed, timeout time.Duration
		refreshRate      = _refreshRate(c)
		qn               = fmt.Sprintf("%s[%s]", cmdDownload, id)
		aborted          bool
	)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	for {
		resp, err := api.DownloadStatus(apiBP, id, true)
		if err != nil {
			return err
		}

		aborted = resp.Aborted
		if aborted || resp.JobFinished() {
			break
		}
		time.Sleep(refreshRate)
		elapsed += refreshRate
		if timeout != 0 && elapsed > timeout {
			return fmt.Errorf("timed out waiting for %s", qn)
		}
	}
	if aborted {
		return fmt.Errorf("download job %s was aborted", id)
	}
	return nil
}

func startDsortHandler(c *cli.Context) (err error) {
	var (
		id       string
		specPath = parseStrFlag(c, dsortSpecFlag)
	)
	if c.NArg() == 0 && specPath == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	} else if c.NArg() > 0 && specPath != "" {
		return &errUsage{
			context:      c,
			message:      "multiple job specifications provided, expected one",
			helpData:     c.Command,
			helpTemplate: cli.CommandHelpTemplate,
		}
	}

	var specBytes []byte
	if specPath == "" {
		// Specification provided as an argument.
		specBytes = []byte(c.Args().First())
	} else {
		// Specification provided as path to the file (flag).
		var r io.Reader
		if specPath == fileStdIO {
			r = os.Stdin
		} else {
			f, err := os.Open(specPath)
			if err != nil {
				return err
			}
			defer f.Close()
			r = f
		}

		var b bytes.Buffer
		// Read at most 1MB so we don't blow up when reading a malicious file.
		if _, err := io.CopyN(&b, r, cos.MiB); err == nil {
			return errors.New("file too big")
		} else if err != io.EOF {
			return err
		}
		specBytes = b.Bytes()
	}

	var rs dsort.RequestSpec
	if errj := jsoniter.Unmarshal(specBytes, &rs); errj != nil {
		if erry := yaml.Unmarshal(specBytes, &rs); erry != nil {
			return fmt.Errorf(
				"failed to determine the type of the job specification, errs: (%v, %v)",
				errj, erry,
			)
		}
	}

	if id, err = api.StartDSort(apiBP, rs); err != nil {
		return
	}

	fmt.Fprintln(c.App.Writer, id)
	return
}

func startLRUHandler(c *cli.Context) (err error) {
	if !flagIsSet(c, listBucketsFlag) {
		return startXactionHandler(c)
	}

	if flagIsSet(c, forceFlag) {
		warn := fmt.Sprintf("LRU eviction with %s option will evict buckets _ignoring_ their respective `lru.enabled` properties.",
			qflprn(forceFlag))
		if ok := confirm(c, "Would you like to continue?", warn); !ok {
			return
		}
	}

	s := parseStrFlag(c, listBucketsFlag)
	bckArgs := splitCsv(s)
	buckets := make([]cmn.Bck, len(bckArgs))
	for idx, bckArg := range bckArgs {
		bck, err := parseBckURI(c, bckArg, true /*require provider*/)
		if err != nil {
			return err
		}
		buckets[idx] = bck
	}

	var (
		id    string
		xargs = xact.ArgsMsg{Kind: apc.ActLRU, Buckets: buckets, Force: flagIsSet(c, forceFlag)}
	)
	if id, err = api.StartXaction(apiBP, xargs); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Started %s %s. %s\n", apc.ActLRU, id, toMonitorMsg(c, id, ""))
	return
}

func startPrefetchHandler(c *cli.Context) (err error) {
	printDryRunHeader(c)

	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}
	bck, err := parseBckURI(c, c.Args().First(), true /*require provider*/)
	if err != nil {
		return
	}
	if bck.IsAIS() {
		return fmt.Errorf("cannot prefetch from ais buckets (the operation applies to remote buckets only)")
	}
	if _, err = headBucket(bck, false /* don't add */); err != nil {
		return
	}

	if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
		return listrange(c, bck)
	}
	return missingArgumentsError(c, "object list or range")
}

//
// job stop
//

func stopJobHandler(c *cli.Context) error {
	var shift int
	if c.Args().Get(0) == commandJob {
		shift = 1
	}
	name, xid, daemonID, bck, err := jobArgs(c, shift, true /*ignore daemonID*/)
	if err != nil {
		return err
	}
	if name == "" && xid == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if daemonID != "" {
		warn := fmt.Sprintf("node ID %q will be ignored (stopping job on a given node not supported)\n",
			daemonID)
		actionWarn(c, warn)
	}

	regex := parseStrFlag(c, regexJobsFlag)

	if xid != "" && (flagIsSet(c, allRunningJobsFlag) || regex != "") {
		warn := fmt.Sprintf("in presence of %s argument ('%s') flags %s and %s will be ignored",
			jobIDArgument, xid, qflprn(allRunningJobsFlag), qflprn(regexJobsFlag))
		actionWarn(c, warn)
	} else if xid == "" && (flagIsSet(c, allRunningJobsFlag) || regex != "") {
		switch name {
		case cmdDownload, cmdDsort:
			// regex supported
		case commandRebalance:
			warn := fmt.Sprintf("global rebalance is global (ignoring %s and %s flags)",
				qflprn(allRunningJobsFlag), qflprn(regexJobsFlag))
			actionWarn(c, warn)
		default:
			if regex != "" {
				warn := fmt.Sprintf("ignoring flag %s - not implemented yet", qflprn(regexJobsFlag))
				actionWarn(c, warn)
			}
		}
	}

	var otherID string
	if name == "" && xid != "" {
		name, otherID = xid2Name(xid)
	}

	// specialized stop
	switch name {
	case cmdDownload:
		if xid == "" {
			if flagIsSet(c, allRunningJobsFlag) || regex != "" {
				return stopDownloadRegex(c, regex)
			}
			return missingArgumentsError(c, jobIDArgument)
		}
		return stopDownloadHandler(c, xid)
	case cmdDsort:
		if xid == "" {
			if flagIsSet(c, allRunningJobsFlag) || regex != "" {
				return stopDsortRegex(c, regex)
			}
			return missingArgumentsError(c, jobIDArgument)
		}
		return stopDsortHandler(c, xid)
	case commandETL:
		return stopETLs(c, otherID /*etl name*/)
	case commandRebalance:
		return stopClusterRebalanceHandler(c)
	}

	// common stop
	var (
		xactID          = xid
		xname, xactKind string
	)
	if name != "" {
		xactKind, xname = xact.GetKindName(name)
		if xactKind == "" {
			return incorrectUsageMsg(c, "unrecognized or misplaced option '%s'", name)
		}
	}

	if xactID == "" {
		// NOTE: differentiate global (cluster-wide) xactions from non-global
		// (the latter require --all to confirm stopping potentially many...)
		var isGlobal bool
		if _, dtor, err := xact.GetDescriptor(xactKind); err == nil {
			isGlobal = dtor.Scope == xact.ScopeG || dtor.Scope == xact.ScopeGB
		}
		if !isGlobal && !flagIsSet(c, allRunningJobsFlag) {
			msg := fmt.Sprintf("Expecting either %s argument or %s option (with or without regular expression)",
				jobIDArgument, qflprn(allRunningJobsFlag))
			return cannotExecuteError(c, errors.New("missing "+jobIDArgument), msg)
		}

		// stop all xactions of a given kind (TODO: bck)
		return stopXactionKind(c, xactKind, xname, bck)
	}

	// query
	msg := formatXactMsg(xactID, xname, bck)
	xargs := xact.ArgsMsg{ID: xactID, Kind: xactKind}
	snap, err := getXactSnap(xargs)
	if err != nil {
		return fmt.Errorf("cannot stop %s: %v", msg, err)
	}
	if snap == nil {
		actionWarn(c, msg+" not found, nothing to do")
		return nil
	}

	// reformat
	if xactID == "" {
		xactID = snap.ID
		debug.Assert(xactKind == snap.Kind)
		msg = formatXactMsg(xactID, xname, snap.Bck)
	} else {
		debug.Assert(xactID == snap.ID)
		msg = formatXactMsg(xactID, xname, snap.Bck)
	}

	// abort?
	var s = "already finished"
	if snap.IsAborted() {
		s = "aborted"
	}
	if snap.IsAborted() || snap.Finished() {
		fmt.Fprintf(c.App.Writer, "%s is %s, nothing to do\n", msg, s)
		return nil
	}

	// abort
	args := xact.ArgsMsg{ID: xactID, Kind: xactKind, Bck: snap.Bck}
	if err := api.AbortXaction(apiBP, args); err != nil {
		return err
	}
	actionDone(c, fmt.Sprintf("Stopped %s\n", msg))
	return nil
}

// TODO: `bck` must provide additional filtering (NIY)
func stopXactionKind(c *cli.Context, xactKind, xname string, bck cmn.Bck) error {
	xactIDs, err := api.GetAllRunningXactions(apiBP, xactKind)
	if err != nil {
		return err
	}
	if len(xactIDs) == 0 {
		actionDone(c, fmt.Sprintf("No running '%s' jobs, nothing to do", xname))
		return nil
	}
	for _, ki := range xactIDs {
		var (
			i      = strings.IndexByte(ki, xact.LeftID[0])
			xactID = ki[i+1 : len(ki)-1] // extract UUID from "name[UUID]"
			args   = xact.ArgsMsg{ID: xactID, Kind: xactKind, Bck: bck}
			msg    = formatXactMsg(xactID, xname, bck)
		)
		if err := api.AbortXaction(apiBP, args); err != nil {
			actionWarn(c, fmt.Sprintf("failed to stop %s: %v", msg, err))
		} else {
			actionDone(c, "Stopped "+msg)
		}
	}
	return nil
}

func formatXactMsg(xactID, xactKind string, bck cmn.Bck) string {
	var sb string
	if !bck.IsQuery() {
		sb = fmt.Sprintf(", %s", bck.DisplayName())
	}
	switch {
	case xactKind != "" && xactID != "":
		return fmt.Sprintf("%s[%s%s]", xactKind, xactID, sb)
	case xactKind != "" && sb != "":
		return fmt.Sprintf("%s[%s]", xactKind, sb)
	case xactKind != "":
		return xactKind
	default:
		return fmt.Sprintf("job[%s%s]", xactID, sb)
	}
}

func stopDownloadRegex(c *cli.Context, regex string) error {
	dlList, err := api.DownloadGetList(apiBP, regex, true /*onlyActive*/)
	if err != nil {
		return err
	}

	var cnt int
	for _, dl := range dlList {
		if err = api.AbortDownload(apiBP, dl.ID); err == nil {
			actionDone(c, fmt.Sprintf("Stopped download job %s", dl.ID))
			cnt++
		} else {
			actionWarn(c, fmt.Sprintf("failed to stop download job %q: %v", dl.ID, err))
		}
	}
	if err != nil {
		return err
	}
	if cnt == 0 {
		actionDone(c, "No running download jobs, nothing to do")
	}
	return nil
}

func stopDownloadHandler(c *cli.Context, id string) (err error) {
	if err = api.AbortDownload(apiBP, id); err != nil {
		return
	}
	actionDone(c, fmt.Sprintf("Stopped download job %s\n", id))
	return
}

func stopDsortRegex(c *cli.Context, regex string) error {
	dsortLst, err := api.ListDSort(apiBP, regex, true /*onlyActive*/)
	if err != nil {
		return err
	}

	var cnt int
	for _, dsort := range dsortLst {
		if err = api.AbortDSort(apiBP, dsort.ID); err == nil {
			actionDone(c, fmt.Sprintf("Stopped dsort job %s", dsort.ID))
			cnt++
		} else {
			actionWarn(c, fmt.Sprintf("failed to stop dsort job %q: %v", dsort.ID, err))
		}
	}
	if err != nil {
		return err
	}
	if cnt == 0 {
		actionDone(c, "No running dsort jobs, nothing to do")
	}
	return nil
}

func stopDsortHandler(c *cli.Context, id string) (err error) {
	if err = api.AbortDSort(apiBP, id); err != nil {
		return
	}
	actionDone(c, fmt.Sprintf("Stopped dsort job %s\n", id))
	return
}

//
// `job wait`
//

const wasFast = 5 * time.Second // start printing "."(s)

func waitJobHandler(c *cli.Context) error {
	var shift int
	if c.Args().Get(0) == commandJob {
		shift = 1
	}

	name, xid, daemonID, bck, err := jobArgs(c, shift, true /*ignore daemonID*/)
	if err != nil {
		return err
	}
	if name == "" && xid == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if daemonID != "" {
		actionWarn(c, fmt.Sprintf("node ID %q will be ignored (waiting for a single target not supported)\n", daemonID))
	}

	if name == "" && xid != "" {
		name, _ = xid2Name(xid) // TODO: add waitETL
	}

	return waitJob(c, name, xid, bck)
}

func waitJob(c *cli.Context, name, xid string, bck cmn.Bck) error {
	// special wait
	switch name {
	case cmdDownload:
		if xid == "" {
			return missingArgumentsError(c, jobIDArgument)
		}
		return waitDownloadHandler(c, xid /*job ID*/)
	case cmdDsort:
		if xid == "" {
			return missingArgumentsError(c, jobIDArgument)
		}
		return waitDsortHandler(c, xid /*job ID*/)
	}

	// common wait
	var (
		xactID          = xid
		xname, xactKind string
	)
	if name != "" {
		xactKind, xname = xact.GetKindName(name)
		if xactKind == "" {
			return incorrectUsageMsg(c, "unrecognized or misplaced option '%s'", name)
		}
	}
	var (
		msg         = formatXactMsg(xactID, xname, bck)
		xargs       = xact.ArgsMsg{ID: xactID, Kind: xactKind}
		refreshRate = _refreshRate(c)
		total       time.Duration
		timeout     time.Duration
		prompted    bool
	)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	for {
		status, err := api.GetOneXactionStatus(apiBP, xargs)
		if err != nil {
			if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
				actionWarn(c, msg+" not found")
				return nil
			}
			return err
		}
		xactID = status.UUID
		msg = formatXactMsg(xactID, xname, bck)
		if status.Aborted() {
			if total > wasFast {
				fmt.Fprintln(c.App.Writer)
			}
			return fmt.Errorf("%s was aborted", msg)
		}
		if status.Finished() {
			break
		}
		if !prompted {
			fmt.Fprintf(c.App.Writer, "Waiting for %s ", msg)
			prompted = true
		}
		time.Sleep(refreshRate)
		total += refreshRate
		if total > wasFast {
			fmt.Fprint(c.App.Writer, ".")
		}
		if timeout != 0 && total > timeout {
			return fmt.Errorf("timed out waiting for %s", msg)
		}
	}
	actionDone(c, "\n"+msg+" done.")
	return nil
}

func waitDownloadHandler(c *cli.Context, id string) error {
	refreshRate := _refreshRate(c)
	if flagIsSet(c, progressFlag) {
		downloadingResult, err := newDownloaderPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}
		actionDone(c, downloadingResult.String())
		return nil
	}

	// poll at a refresh rate
	var (
		total   time.Duration
		timeout time.Duration
		qn      = fmt.Sprintf("%s[%s]", cmdDownload, id)
	)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	for {
		resp, err := api.DownloadStatus(apiBP, id, true /*onlyActive*/)
		if err != nil {
			return err
		}
		if resp.Aborted {
			if total > wasFast {
				fmt.Fprintln(c.App.Writer)
			}
			return fmt.Errorf("%s was aborted", qn)
		}
		if resp.JobFinished() {
			break
		}
		time.Sleep(refreshRate)
		total += refreshRate
		if timeout != 0 && total > timeout {
			return fmt.Errorf("timed out waiting for %s", qn)
		}
	}
	actionDone(c, "\n"+qn+" finished")
	return nil
}

func waitDsortHandler(c *cli.Context, id string) error {
	refreshRate := _refreshRate(c)
	if flagIsSet(c, progressFlag) {
		dsortResult, err := newDSortPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}
		actionDone(c, dsortResult.String())
		return nil
	}

	// poll at refresh rate
	var (
		total   time.Duration
		timeout time.Duration
		qn      = fmt.Sprintf("%s[%s]", cmdDsort, id)
	)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	for {
		resp, err := api.MetricsDSort(apiBP, id)
		if err != nil {
			return err
		}
		finished := true
		for _, targetMetrics := range resp {
			if targetMetrics.Aborted.Load() {
				if total > wasFast {
					fmt.Fprintln(c.App.Writer)
				}
				return fmt.Errorf("%s was aborted", qn)
			}
			finished = finished && targetMetrics.Creation.Finished
		}
		if finished {
			break
		}
		time.Sleep(refreshRate)
		total += refreshRate
		if timeout != 0 && total > timeout {
			return fmt.Errorf("timed out waiting for %s", qn)
		}
	}
	if total > wasFast {
		fmt.Fprintln(c.App.Writer)
	}
	actionDone(c, qn+" finished")
	return nil
}

//
// job remove
//

func removeDownloadHandler(c *cli.Context) error {
	regex := parseStrFlag(c, regexJobsFlag)
	if flagIsSet(c, allFinishedJobsFlag) || regex != "" {
		return removeDownloadRegex(c, regex)
	}

	// by job ID
	if c.NArg() < 1 {
		msg := fmt.Sprintf("Expecting either %s argument or %s option (with or without regular expression)",
			jobIDArgument, qflprn(allFinishedJobsFlag))
		return cannotExecuteError(c, errors.New("missing "+jobIDArgument), msg)
	}
	id := c.Args().First()
	if err := api.RemoveDownload(apiBP, id); err != nil {
		return err
	}
	actionDone(c, fmt.Sprintf("Removed finished download job %q", id))
	return nil
}

func removeDownloadRegex(c *cli.Context, regex string) error {
	dlList, err := api.DownloadGetList(apiBP, regex, false /*onlyActive*/)
	if err != nil {
		return err
	}

	var cnt int
	for _, dl := range dlList {
		if !dl.JobFinished() {
			continue
		}
		err = api.RemoveDownload(apiBP, dl.ID)
		if err == nil {
			actionDone(c, fmt.Sprintf("Removed download job %q", dl.ID))
			cnt++
		} else {
			actionWarn(c, fmt.Sprintf("failed to remove download job %q: %v", dl.ID, err))
		}
	}
	if err != nil {
		return err
	}
	if cnt == 0 {
		actionDone(c, "No finished download jobs, nothing to do")
	}
	return nil
}

func removeDsortHandler(c *cli.Context) error {
	regex := parseStrFlag(c, regexJobsFlag)
	if flagIsSet(c, allFinishedJobsFlag) || regex != "" {
		return removeDsortRegex(c, regex)
	}

	// by job ID
	if c.NArg() < 1 {
		msg := fmt.Sprintf("Expecting either %s argument or %s option (with or without regular expression)",
			jobIDArgument, qflprn(allFinishedJobsFlag))
		return cannotExecuteError(c, errors.New("missing "+jobIDArgument), msg)
	}
	id := c.Args().First()
	if err := api.RemoveDSort(apiBP, id); err != nil {
		return err
	}

	actionDone(c, fmt.Sprintf("Removed finished dsort job %q", id))
	return nil
}

func removeDsortRegex(c *cli.Context, regex string) error {
	dsortLst, err := api.ListDSort(apiBP, regex, false /*onlyActive*/)
	if err != nil {
		return err
	}

	var cnt int
	for _, dsort := range dsortLst {
		if dsort.IsRunning() {
			continue
		}
		err = api.RemoveDSort(apiBP, dsort.ID)
		if err == nil {
			actionDone(c, fmt.Sprintf("Removed dsort job %q", dsort.ID))
			cnt++
		} else {
			actionWarn(c, fmt.Sprintf("failed to remove dsort job %q: %v", dsort.ID, err))
		}
	}
	if err != nil {
		return err
	}
	if cnt == 0 {
		actionDone(c, "No finished dsort jobs, nothing to do")
	}
	return nil
}

//
// utility functions
//

// facilitate flexibility and common-sense argument omissions, do away with rigid name -> id -> ... ordering
func jobArgs(c *cli.Context, shift int, ignoreDaemonID bool) (name, xid, daemonID string, bck cmn.Bck, err error) {
	// prelim. assignments
	name = c.Args().Get(shift)
	xid = c.Args().Get(shift + 1)
	daemonID = c.Args().Get(shift + 2)

	// validate and reassign
	if name != "" && name != dsort.DSortName {
		if xactKind, _ := xact.GetKindName(name); xactKind == "" {
			daemonID = xid
			xid = name
			name = ""
		}
	}
	if xid != "" {
		var errV error
		if bck, errV = parseBckURI(c, xid, true /*require provider*/); errV == nil {
			xid = "" // arg #1 is a bucket
		}
	}
	if daemonID != "" && bck.IsEmpty() {
		var errV error
		if bck, errV = parseBckURI(c, daemonID, true); errV == nil {
			daemonID = "" // ditto arg #2
		}
	}
	if xid != "" && daemonID == "" {
		if sid, _, errV := getNodeIDName(c, xid); errV == nil {
			daemonID, xid = sid, ""
			return
		}
	}
	if ignoreDaemonID {
		return
	}
	// sname => sid
	if daemonID != "" {
		daemonID, _, err = getNodeIDName(c, daemonID)
	}
	return
}

// [best effort] try to disambiguate download/dsort/etl job ID vs xaction UUID
func xid2Name(xid string) (name, otherID string) {
	switch {
	case strings.HasPrefix(xid, dload.PrefixJobID):
		if _, err := api.DownloadStatus(apiBP, xid, false /*onlyActive*/); err == nil {
			name = cmdDownload
		}
	case strings.HasPrefix(xid, dsort.PrefixJobID):
		if _, err := api.MetricsDSort(apiBP, xid); err == nil {
			name = cmdDsort
		}
	// NOTE: not to confuse ETL xaction ID with its name (`etl-name`)
	case strings.HasPrefix(xid, etl.PrefixXactID):
		if l := findETL("", xid); l != nil {
			name = commandETL
			otherID = l.Name
		}
	}
	return
}
