// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
)

const prefetchUsage = "prefetch one remote bucket, multiple remote buckets, or\n" +
	indent1 + "selected objects in a given remote bucket or buckets, e.g.:\n" +
	indent1 + "\t- 'prefetch gs://abc'\t- prefetch entire bucket (all gs://abc objects that are _not_ in-cluster);\n" +
	indent1 + "\t- 'prefetch gs://abc --num-workers 32'\t- same as above with 32 concurrent (prefetching) workers;\n" +
	indent1 + "\t- 'prefetch gs:'\t- prefetch all visible/accessible GCP buckets;\n" +
	indent1 + "\t- 'prefetch gs: --num-workers=48'\t- same as above employing 48 workers;\n" +
	indent1 + "\t- 'prefetch gs://abc --prefix images/'\t- prefetch all objects from the virtual subdirectory \"images\";\n" +
	indent1 + "\t- 'prefetch gs://abc --template images/'\t- same as above;\n" +
	indent1 + "\t- 'prefetch gs://abc/images/'\t- same as above;\n" +
	indent1 + "\t- 'prefetch gs://abc --template \"shard-{0000..9999}.tar.lz4\"'\t- prefetch the matching range (prefix + brace expansion);\n" +
	indent1 + "\t- 'prefetch \"gs://abc/shard-{0000..9999}.tar.lz4\"'\t- same as above (notice double quotes)"

const blobDownloadUsage = "run a job to download large object(s) from remote storage to aistore cluster, e.g.:\n" +
	indent1 + "\t- 'blob-download s3://ab/largefile --chunk-size=2mb --progress'\t- download one blob at a given chunk size\n" +
	indent1 + "\t- 'blob-download s3://ab --list \"f1, f2\" --num-workers=4 --progress'\t- run 4 concurrent readers to download 2 (listed) blobs\n" +
	indent1 + "When _not_ using '--progress' option, run 'ais show job' to monitor."

const resilverUsage = "resilver user data on a given target (or all targets in the cluster); entails:\n" +
	indent1 + "\t- fix data redundancy with respect to bucket configuration;\n" +
	indent1 + "\t- remove migrated objects and old/obsolete workfiles."

const stopUsage = "terminate a single batch job or multiple jobs, e.g.:\n" +
	indent1 + "\t- 'stop tco-cysbohAGL'\t- terminate a given job identified by its unique ID;\n" +
	indent1 + "\t- 'stop copy-listrange'\t- terminate all multi-object copies;\n" +
	indent1 + "\t- 'stop copy-objects'\t- same as above (using display name);\n" +
	indent1 + "\t- 'stop --all'\t- terminate all running jobs\n" +
	indent1 + tabHelpOpt + "."

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
		nonverboseFlag,
	}
	startSpecialFlags = map[string][]cli.Flag{
		cmdDownload: {
			dloadTimeoutFlag,
			descJobFlag,
			limitConnectionsFlag,
			objectsListFlag,
			dloadProgressFlag,
			progressFlag,
			waitFlag,
			waitJobXactFinishedFlag,
			limitBytesPerHourFlag,
			syncFlag,
			unitsFlag,
		},
		cmdDsort: {
			dsortSpecFlag,
			verboseFlag,
		},
		commandPrefetch: append(
			listRangeProgressWaitFlags,
			dryRunFlag,
			verbObjPrefixFlag, // to disambiguate bucket/prefix vs bucket/objName
			latestVerFlag,
			blobThresholdFlag,
			numListRangeWorkersFlag,
		),
		cmdBlobDownload: {
			refreshFlag,
			progressFlag,
			listFlag,
			chunkSizeFlag,
			numBlobWorkersFlag,
			waitFlag,
			waitJobXactFinishedFlag,
			latestVerFlag,
			nonverboseFlag,
		},
		cmdLRU: {
			lruBucketsFlag,
			forceFlag,
			nonverboseFlag,
		},
	}

	jobStartResilver = cli.Command{
		Name:         commandResilver,
		Usage:        resilverUsage,
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        startCommonFlags,
		Action:       startResilverHandler,
		BashComplete: suggestTargets,
	}

	prefetchStartCmd = cli.Command{
		Name:         commandPrefetch,
		Usage:        prefetchUsage,
		ArgsUsage:    bucketObjectOrTemplateMultiArg,
		Flags:        startSpecialFlags[commandPrefetch],
		Action:       startPrefetchHandler,
		BashComplete: bucketCompletions(bcmplop{multiple: true}),
	}
	blobDownloadCmd = cli.Command{
		Name:         cmdBlobDownload,
		Usage:        blobDownloadUsage,
		ArgsUsage:    objectArgument,
		Flags:        startSpecialFlags[cmdBlobDownload],
		Action:       blobDownloadHandler,
		BashComplete: remoteBucketCompletions(bcmplop{multiple: true}),
	}

	jobStartSub = cli.Command{
		Name:  commandStart,
		Usage: "run batch job",
		Subcommands: []cli.Command{
			prefetchStartCmd,
			blobDownloadCmd,
			{
				Name:      cmdDownload,
				Usage:     "download files and objects from remote sources",
				ArgsUsage: startDownloadArgument,
				Flags:     startSpecialFlags[cmdDownload],
				Action:    startDownloadHandler,
			},
			dsortStartCmd,
			{
				Name:         cmdLRU,
				Usage:        "run LRU eviction",
				Flags:        startSpecialFlags[cmdLRU],
				Action:       startLRUHandler,
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
			cleanupCmd,
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
		yesFlag,
	}
	jobStopSub = cli.Command{
		Name:         commandStop,
		Usage:        stopUsage,
		ArgsUsage:    jobAnyArg,
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
		ArgsUsage:    jobAnyArg,
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

func jobName(xname, xid string) string { return xname + "[" + xid + "]" }

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
				// the following x-s, even through startable, have their own custom CLI handlers:
				// - cleanup-store
				// - ec-encode
				// - lru
				// - make-n-copies
				// - prefetch-listrange
				// - blob-download
				// - rebalance
				// - resilver
				continue outer
			}
		}
		// add generic start handler on the fly
		cmd := cli.Command{
			Name:   xname,
			Usage:  "start " + xname,
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
	var (
		bck   cmn.Bck
		extra string
	)
	if c.NArg() == 0 && xact.IsSameScope(xname, xact.ScopeB) {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if c.NArg() > 0 && xact.IsSameScope(xname, xact.ScopeB, xact.ScopeGB) {
		uri := c.Args().Get(0)
		if xname == apc.ActBlobDl {
			bck, extra /*objName*/, err = parseBckObjURI(c, uri, false)
		} else {
			bck, err = parseBckURI(c, uri, false)
		}
		if err != nil {
			return err
		}
	}
	xargs := xact.ArgsMsg{Kind: xname, Bck: bck}
	return startXaction(c, &xargs, extra)
}

func startResilverHandler(c *cli.Context) error {
	var tid string
	if c.NArg() > 0 {
		tsi, _, err := getNode(c, c.Args().Get(0))
		if err != nil {
			return err
		}
		tid = tsi.ID()
	}
	xargs := xact.ArgsMsg{Kind: apc.ActResilver, DaemonID: tid}
	return startXaction(c, &xargs, "")
}

func startXaction(c *cli.Context, xargs *xact.ArgsMsg, extra string) error {
	if !xargs.Bck.IsQuery() {
		if _, err := headBucket(xargs.Bck, false /* don't add */); err != nil {
			return err
		}
	} else if xact.IsSameScope(xargs.Kind, xact.ScopeB) {
		return fmt.Errorf("%q requires bucket to run", xargs.Kind)
	}

	xid, err := api.StartXaction(apiBP, xargs, extra)
	if err != nil {
		return V(err)
	}
	if xid == "" {
		warn := fmt.Sprintf("The operation returned an empty UUID (a no-op?). %s\n",
			toShowMsg(c, "", "To investigate", false))
		actionWarn(c, warn)
		return nil
	}

	debug.Assert(xact.IsValidUUID(xid), xid)

	if xargs.Kind == apc.ActRebalance {
		actionDone(c, "Started global rebalance. To monitor the progress, run 'ais show rebalance'")
	} else {
		actionX(c, xargs, "")
	}

	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		return nil
	}
	return waitJob(c, xargs.Kind, xid, xargs.Bck)
}

func startDownloadHandler(c *cli.Context) error {
	var (
		description      = parseStrFlag(c, descJobFlag)
		timeout          = parseStrFlag(c, dloadTimeoutFlag)
		objectsListPath  = parseStrFlag(c, objectsListFlag)
		progressInterval = parseStrFlag(c, dloadProgressFlag)
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

	limitBPH, err := parseSizeFlag(c, limitBytesPerHourFlag)
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
			basePayload.Bck.Cname(""))
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
		backends, err := api.GetConfiguredBackends(apiBP)
		if err != nil {
			return err
		}
		if cos.StringInSlice(source.backend.bck.Provider, backends) {
			dlType = dload.TypeBackend

			p, err := api.HeadBucket(apiBP, basePayload.Bck, false /* don't add */)
			if err != nil {
				return V(err)
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
		return V(err)
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
		startedTimeout = max(dloadStartedTime, refreshRateMinDur*2)
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
		warn := fmt.Sprintf("%d of %d download jobs failed. %s", resp.ErrorCnt, resp.ScheduledCnt, msg)
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
		qn               = xact.Cname(cmdDownload, id)
		aborted          bool
	)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	for {
		resp, err := api.DownloadStatus(apiBP, id, true)
		if err != nil {
			return V(err)
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

func startLRUHandler(c *cli.Context) (err error) {
	if !flagIsSet(c, lruBucketsFlag) {
		return startXactionHandler(c)
	}

	if flagIsSet(c, forceFlag) {
		warn := fmt.Sprintf("LRU eviction with %s option will evict buckets _ignoring_ their respective `lru.enabled` properties.",
			qflprn(forceFlag))
		if ok := confirm(c, "Would you like to continue?", warn); !ok {
			return
		}
	}

	s := parseStrFlag(c, lruBucketsFlag)
	bckArgs := splitCsv(s)
	buckets := make([]cmn.Bck, len(bckArgs))
	for idx, bckArg := range bckArgs {
		bck, err := parseBckURI(c, bckArg, false)
		if err != nil {
			return err
		}
		buckets[idx] = bck
	}

	var (
		id    string
		xargs = xact.ArgsMsg{Kind: apc.ActLRU, Buckets: buckets, Force: flagIsSet(c, forceFlag)}
	)
	if id, err = api.StartXaction(apiBP, &xargs, ""); err != nil {
		return
	}

	actionX(c, &xact.ArgsMsg{Kind: apc.ActLRU, ID: id}, "")
	return
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
	if name == "" && xid == "" && !flagIsSet(c, allRunningJobsFlag) {
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
				warn := fmt.Sprintf("ignoring flag %s - "+NIY, qflprn(regexJobsFlag))
				actionWarn(c, warn)
			}
		}
	}

	var (
		otherID         string
		xactID          = xid
		xname, xactKind string
	)
	if name == "" && xid != "" {
		name, otherID = xid2Name(xid)
	}
	if name != "" {
		xactKind, xname = xact.GetKindName(name)
		if xactKind == "" {
			return incorrectUsageMsg(c, "unrecognized or misplaced option '%s'", name)
		}
	}

	// confirm unless
	if xactID == "" && name != "" {
		if name != commandRebalance && !flagIsSet(c, yesFlag) && !flagIsSet(c, allRunningJobsFlag) {
			prompt := fmt.Sprintf("Stop all '%s' jobs?", name)
			if ok := confirm(c, prompt); !ok {
				return nil
			}
		}
	}

	// specialized stop
	switch name {
	case cmdDownload:
		if xid == "" {
			return stopDownloadRegex(c, regex)
		}
		return stopDownloadHandler(c, xid)
	case cmdDsort:
		if xid == "" {
			return stopDsortRegex(c, regex)
		}
		return stopDsortHandler(c, xid)
	case commandETL:
		return stopETLs(c, otherID /*etl name*/)
	case commandRebalance:
		return stopClusterRebalanceHandler(c)
	}

	// generic xstop

	if xactID == "" {
		return stopXactionKindOrAll(c, xactKind, xname, bck)
	}

	// query
	msg := formatXactMsg(xactID, xname, bck)
	xargs := xact.ArgsMsg{ID: xactID, Kind: xactKind}
	xs, cms, err := queryXactions(&xargs, true /*summarize*/)
	if err != nil {
		return fmt.Errorf("cannot stop %s: %v", msg, err)
	}
	if len(xs) == 0 {
		actionWarn(c, msg+" not found, nothing to do")
		return nil
	}

	// reformat
	msg = formatXactMsg(xid, xname, bck)

	// nothing to do?
	if cms.aborted {
		fmt.Fprintf(c.App.Writer, "%s is aborted, nothing to do\n", msg)
		return nil
	}
	if !cms.running {
		fmt.Fprintf(c.App.Writer, "%s already finished, nothing to do\n", msg)
		return nil
	}

	// call to abort
	args := xact.ArgsMsg{ID: xactID, Kind: xactKind, Bck: bck}
	if err := api.AbortXaction(apiBP, &args); err != nil {
		return V(err)
	}
	actionDone(c, fmt.Sprintf("Stopped %s\n", msg))
	return nil
}

// NOTE: the '--all' case when both (xactKind == "" && xname == "") - is also handled here
// TODO: aistore supports `bck` for additional filtering (NIY)
func stopXactionKindOrAll(c *cli.Context, xactKind, xname string, bck cmn.Bck) error {
	cnames, err := api.GetAllRunningXactions(apiBP, xactKind)
	if err != nil {
		return V(err)
	}
	if len(cnames) == 0 {
		var what string
		if xname != "" {
			what = " '" + xname + "'"
		}
		actionDone(c, fmt.Sprintf("No running%s jobs, nothing to do", what))
		return nil
	}
	for _, cname := range cnames {
		xactKind, xactID, err := xact.ParseCname(cname)
		if err != nil {
			debug.AssertNoErr(err)
			continue
		}
		args := xact.ArgsMsg{ID: xactID, Kind: xactKind, Bck: bck}
		if err := api.AbortXaction(apiBP, &args); err != nil {
			actionWarn(c, fmt.Sprintf("failed to stop %s: %v", cname, err))
		} else {
			actionDone(c, "Stopped "+cname)
		}
	}
	return nil
}

func formatXactMsg(xactID, xactKind string, bck cmn.Bck) string {
	var sb string
	if !bck.IsQuery() {
		sb = ", " + bck.Cname("")
	}
	switch {
	case xactKind != "" && xactID != "":
		return fmt.Sprintf("%s%s", xact.Cname(xactKind, xactID), sb)
	case xactKind != "" && sb != "":
		return fmt.Sprintf("%s%s", xactKind, sb)
	case xactKind != "":
		return xactKind
	default:
		return fmt.Sprintf("[%s]%s", xactID, sb)
	}
}

func stopDownloadRegex(c *cli.Context, regex string) error {
	dlList, err := api.DownloadGetList(apiBP, regex, true /*onlyActive*/)
	if err != nil {
		return V(err)
	}

	var cnt int
	for _, dl := range dlList {
		if err = api.AbortDownload(apiBP, dl.ID); err == nil {
			actionDone(c, "Stopped download job "+dl.ID)
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
	dsortLst, err := api.ListDsort(apiBP, regex, true /*onlyActive*/)
	if err != nil {
		return V(err)
	}

	var cnt int
	for _, dsort := range dsortLst {
		if err = api.AbortDsort(apiBP, dsort.ID); err == nil {
			actionDone(c, "Stopped dsort job "+dsort.ID)
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
	if err = api.AbortDsort(apiBP, id); err != nil {
		return
	}
	actionDone(c, fmt.Sprintf("Stopped dsort job %s\n", id))
	return
}

//
// `job wait`
//

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
	// TODO: niy
	if flagIsSet(c, refreshFlag) {
		warn := fmt.Sprintf("ignoring flag %s  - not fully implemented yet", qflprn(refreshFlag))
		actionWarn(c, warn)
	} else if flagIsSet(c, progressFlag) {
		warn := fmt.Sprintf("ignoring flag %s  - not fully implemented yet", qflprn(progressFlag))
		actionWarn(c, warn)
	}
	// x-wait
	var (
		xactID, xname = xid, name
		xactKind      string
	)
	if name != "" {
		xactKind, xname = xact.GetKindName(name)
		if xactKind == "" {
			return incorrectUsageMsg(c, "unrecognized or misplaced option '%s'", name)
		}
	}
	xargs := xact.ArgsMsg{ID: xactID, Kind: xactKind}
	if flagIsSet(c, waitJobXactFinishedFlag) {
		xargs.Timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}

	// find out the kind if not provided
	if xargs.Kind == "" {
		_, snap, err := getAnyXactSnap(&xargs)
		if err != nil {
			return err
		}
		xargs.Kind = snap.Kind
		_, xname = xact.GetKindName(xargs.Kind)
	}

	msg := formatXactMsg(xactID, xname, bck)
	fmt.Fprintln(c.App.Writer, "Waiting for "+msg+" ...")
	err := waitXact(&xargs)
	if err == nil {
		actionDone(c, "Done.")
	}
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
		qn      = xact.Cname(cmdDownload, id)
	)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	for {
		resp, err := api.DownloadStatus(apiBP, id, true /*onlyActive*/)
		if err != nil {
			return V(err)
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
		dsortResult, err := newDsortPB(apiBP, id, refreshRate).run()
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
		qn      = xact.Cname(cmdDsort, id)
	)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	for {
		resp, err := api.MetricsDsort(apiBP, id)
		if err != nil {
			return V(err)
		}
		finished := true
		for _, targetMetrics := range resp {
			if targetMetrics.Metrics.Aborted.Load() {
				if total > wasFast {
					fmt.Fprintln(c.App.Writer)
				}
				return fmt.Errorf("%s was aborted", qn)
			}
			finished = finished && targetMetrics.Metrics.Creation.Finished
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
	id := c.Args().Get(0)
	if err := api.RemoveDownload(apiBP, id); err != nil {
		return V(err)
	}
	actionDone(c, fmt.Sprintf("Removed finished download job %q", id))
	return nil
}

func removeDownloadRegex(c *cli.Context, regex string) error {
	dlList, err := api.DownloadGetList(apiBP, regex, false /*onlyActive*/)
	if err != nil {
		return V(err)
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
	id := c.Args().Get(0)
	if err := api.RemoveDsort(apiBP, id); err != nil {
		return V(err)
	}

	actionDone(c, fmt.Sprintf("Removed finished dsort job %q", id))
	return nil
}

func removeDsortRegex(c *cli.Context, regex string) error {
	dsortLst, err := api.ListDsort(apiBP, regex, false /*onlyActive*/)
	if err != nil {
		return V(err)
	}

	var cnt int
	for _, dsort := range dsortLst {
		if dsort.IsRunning() {
			continue
		}
		err = api.RemoveDsort(apiBP, dsort.ID)
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
	if name != "" && name != apc.ActDsort {
		if xactKind, _ := xact.GetKindName(name); xactKind == "" {
			daemonID = xid
			xid = name
			name = ""
		}
	}
	if xid != "" {
		var errV error
		if bck, errV = parseBckURI(c, xid, false); errV == nil {
			xid = "" // arg #1 is a bucket
		}
	}
	if daemonID != "" && bck.IsEmpty() {
		var errV error
		if bck, errV = parseBckURI(c, daemonID, false); errV == nil {
			daemonID = "" // ditto arg #2
		}
	}
	if xid != "" && daemonID == "" {
		if node, _, errV := getNode(c, xid); errV == nil {
			daemonID, xid = node.ID(), ""
			return
		}
	}
	if ignoreDaemonID {
		return
	}
	// sname => sid
	if daemonID != "" {
		var node *meta.Snode
		node, _, err = getNode(c, daemonID)
		if err == nil {
			daemonID = node.ID()
		}
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
		if _, err := api.MetricsDsort(apiBP, xid); err == nil {
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
