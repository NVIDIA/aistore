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
	startCmdsFlags = map[string][]cli.Flag{
		subcmdXaction: {
			waitFlag,
		},
		subcmdDownload: {
			timeoutFlag,
			descJobFlag,
			limitConnectionsFlag,
			objectsListFlag,
			progressIntervalFlag,
			progressBarFlag,
			waitFlag,
			limitBytesPerHourFlag,
			syncFlag,
		},
		subcmdDsort: {
			specFileFlag,
		},
		commandPrefetch: append(
			baseLstRngFlags,
			dryRunFlag,
		),
		subcmdLRU: {
			listBucketsFlag,
			forceFlag,
		},
	}

	jobStartResilver = cli.Command{
		Name: commandResilver,
		Usage: "resilver user data on a given target or all targets " +
			"(fix data redundancy with respect to bucket configuration, " +
			"remove migrated objects and old/obsolete workfiles)",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        startCmdsFlags[subcmdXaction],
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
				Flags:        startCmdsFlags[commandPrefetch],
				Action:       startPrefetchHandler,
				BashComplete: bucketCompletions(bcmplop{multiple: true}),
			},
			{
				Name:      subcmdDownload,
				Usage:     "download files and objects from remote sources",
				ArgsUsage: startDownloadArgument,
				Flags:     startCmdsFlags[subcmdDownload],
				Action:    startDownloadHandler,
			},
			{
				Name:      subcmdDsort,
				Usage:     "start " + dsort.DSortName + " job",
				ArgsUsage: jsonSpecArgument,
				Flags:     startCmdsFlags[subcmdDsort],
				Action:    startDsortHandler,
			},
			{
				Name:         subcmdLRU,
				Usage:        "run LRU eviction",
				Flags:        startCmdsFlags[subcmdLRU],
				Action:       startLRUHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			{
				Name:         subcmdStgCleanup,
				Usage:        "cleanup storage: remove migrated and deleted objects, old/obsolete workfiles",
				ArgsUsage:    listAnyCommandArgument,
				Flags:        storageCmdFlags[subcmdStgCleanup],
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
		regexFlag,
	}
	jobStopSub = cli.Command{
		Name:         commandStop,
		Usage:        "stop/abort a single batch job or multiple jobs (use <TAB-TAB> to select)",
		ArgsUsage:    "NAME [JOB_ID] [BUCKET]",
		Flags:        stopCmdsFlags,
		Action:       stopJobHandler,
		BashComplete: runningJobCompletions,
	}
)

// ais wait
var (
	waitCmdsFlags = []cli.Flag{
		refreshFlag,
		progressBarFlag,
	}
	jobWaitSub = cli.Command{
		Name:         commandWait,
		Usage:        "wait for a specific batch job to complete (use <TAB-TAB> to select)",
		ArgsUsage:    "NAME " + jobIDArgument,
		Flags:        waitCmdsFlags,
		Action:       waitJobHandler,
		BashComplete: runningJobCompletions,
	}
)

// ais job remove
var (
	removeCmdsFlags = []cli.Flag{
		allFinishedJobsFlag,
		regexFlag,
	}
	jobRemoveSub = cli.Command{
		Name:  commandRemove,
		Usage: "cleanup finished jobs",
		Subcommands: []cli.Command{
			{
				Name:         subcmdDownload,
				Usage:        "remove finished download job(s)",
				ArgsUsage:    optionalJobIDArgument,
				Flags:        removeCmdsFlags,
				Action:       removeDownloadHandler,
				BashComplete: downloadIDFinishedCompletions,
			},
			{
				Name:         subcmdDsort,
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
			Flags:  startCmdsFlags[subcmdXaction],
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

	xactArgs := api.XactReqArgs{Kind: xname, Bck: bck, DaemonID: sid}
	id, err := api.StartXaction(apiBP, xactArgs)
	if err != nil {
		return err
	}
	if id != "" {
		debug.Assert(cos.IsValidUUID(id), id)
		msg := fmt.Sprintf("Started %s[%s]. %s", xname, id, toMonitorMsg(c, id))
		actionDone(c, msg)
		return nil
	}
	warn := fmt.Sprintf("The operation returned an empty UUID (a no-op?). %s\n",
		toShowMsg(c, "", "To investigate", false))
	actionWarn(c, warn)
	return nil
}

func startDownloadHandler(c *cli.Context) error {
	var (
		description      = parseStrFlag(c, descJobFlag)
		timeout          = parseStrFlag(c, timeoutFlag)
		objectsListPath  = parseStrFlag(c, objectsListFlag)
		progressInterval = parseStrFlag(c, progressIntervalFlag)
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

	limitBPH, err := parseByteFlagToInt(c, limitBytesPerHourFlag)
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

	if flagIsSet(c, progressBarFlag) {
		return pbDownload(c, id)
	}

	if flagIsSet(c, waitFlag) {
		return wtDownload(c, id)
	}

	return bgDownload(c, id)
}

func pbDownload(c *cli.Context, id string) (err error) {
	refreshRate := calcRefreshRate(c)
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
	const (
		checkTimeout  = 5 * time.Second
		checkInterval = time.Second
	)
	var (
		passed time.Duration
		resp   *dload.StatusResp
	)
	// In a non-interactive mode, allow the downloader to start jobs before checking.
	for passed < checkTimeout {
		time.Sleep(checkInterval)
		passed += checkInterval
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
		msg := fmt.Sprintf("To monitor the progress, run '%s %s %s %s %s --%s` ",
			cliName, commandShow, commandJob, subcmdDownload, id, progressBarFlag.Name)
		actionDone(c, msg)
	}
	return err
}

func waitDownload(c *cli.Context, id string) (err error) {
	var (
		aborted     bool
		refreshRate = calcRefreshRate(c)
	)

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
	}

	if aborted {
		return fmt.Errorf("download job %s was aborted", id)
	}
	return nil
}

func startDsortHandler(c *cli.Context) (err error) {
	var (
		id       string
		specPath = parseStrFlag(c, specFileFlag)
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
		warn := fmt.Sprintf("LRU with '--%s' option will evict buckets _ignoring_ their respective `lru.enabled` properties.",
			forceFlag.Name)
		if ok := confirm(c, "Would you like to continue?", warn); !ok {
			return
		}
	}

	bckArgs := makeList(parseStrFlag(c, listBucketsFlag))
	buckets := make([]cmn.Bck, len(bckArgs))
	for idx, bckArg := range bckArgs {
		bck, err := parseBckURI(c, bckArg, true /*require provider*/)
		if err != nil {
			return err
		}
		buckets[idx] = bck
	}

	var (
		id       string
		xactArgs = api.XactReqArgs{Kind: apc.ActLRU, Buckets: buckets, Force: flagIsSet(c, forceFlag)}
	)
	if id, err = api.StartXaction(apiBP, xactArgs); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Started %s %s. %s\n", apc.ActLRU, id, toMonitorMsg(c, id))
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
		return listOrRangeOp(c, bck)
	}

	return missingArgumentsError(c, "object list or range")
}

//
// job stop
//

func stopJobHandler(c *cli.Context) error {
	var (
		name  = c.Args().Get(0)
		id    = c.Args().Get(1)
		regex = parseStrFlag(c, regexFlag)
	)
	if name == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	bck, err := parseBckURI(c, id, true /*require provider*/)
	if err == nil {
		id = "" // arg 1 was in fact bucket
	}

	if id != "" && (flagIsSet(c, allRunningJobsFlag) || regex != "") {
		warn := fmt.Sprintf("in presence of %s argument ('%s') flags '--%s' and '--%s' will be ignored",
			jobIDArgument, id, allRunningJobsFlag.Name, regexFlag.Name)
		actionWarn(c, warn)
	} else if id == "" && (flagIsSet(c, allRunningJobsFlag) || regex != "") {
		switch name {
		case subcmdDownload, subcmdDsort:
			// regex supported
		case commandRebalance:
			warn := fmt.Sprintf("global rebalance is global (ignoring '--%s' and '--%s' flags)",
				allRunningJobsFlag.Name, regexFlag.Name)
			actionWarn(c, warn)
		default:
			if regex != "" {
				warn := fmt.Sprintf("ignoring flag '--%s' - not implemented yet", regexFlag.Name)
				actionWarn(c, warn)
			}
		}
	}

	// specialized stop
	switch name {
	case subcmdDownload:
		if id == "" {
			if flagIsSet(c, allRunningJobsFlag) || regex != "" {
				return stopDownloadRegex(c, regex)
			}
			return missingArgumentsError(c, jobIDArgument)
		}
		return stopDownloadHandler(c, id)
	case subcmdDsort:
		if id == "" {
			if flagIsSet(c, allRunningJobsFlag) || regex != "" {
				return stopDsortRegex(c, regex)
			}
			return missingArgumentsError(c, jobIDArgument)
		}
		return stopDsortHandler(c, id)
	case commandETL:
		return stopETLs(c, id)
	case commandRebalance:
		return stopClusterRebalanceHandler(c)
	}

	// common stop
	xactKind, xname := xact.GetKindName(name)
	if xactKind == "" {
		return incorrectUsageMsg(c, "unrecognized or misplaced option '%s'", name)
	}
	xactID := id

	// stop all xactions of a given kind (TODO: bck)
	if xactID == "" {
		if !flagIsSet(c, allRunningJobsFlag) {
			err := fmt.Errorf("expecting either %s argument or '--%s' option (with or without regular expression)",
				jobIDArgument, allRunningJobsFlag.Name)
			return cannotExecuteError(c, err)
		}
		return stopXactionKind(c, xactKind, xname, bck)
	}

	// query
	msg := formatXactMsg(xactID, xname, bck)
	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind}
	snap, err := getXactSnap(xactArgs)
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
	args := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: snap.Bck}
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
			args   = api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck}
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
		return fmt.Sprintf("xaction[%s%s]", xactID, sb)
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

const wasFast = 60 * time.Second // TODO -- FIXME: reduce and flex

func waitJobHandler(c *cli.Context) error {
	var (
		name = c.Args().Get(0)
		id   = c.Args().Get(1)
	)
	if name == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	bck, err := parseBckURI(c, id, true /*require provider*/)
	if err == nil {
		id = "" // arg 1 was in fact bucket
	}

	// special
	switch name {
	case subcmdDownload:
		if id == "" {
			return missingArgumentsError(c, jobIDArgument)
		}
		return waitDownloadHandler(c, id /* job ID */)
	case subcmdDsort:
		if id == "" {
			return missingArgumentsError(c, jobIDArgument)
		}
		return waitDsortHandler(c, id /* job ID */)
	}

	// common `wait`
	xactKind, xname := xact.GetKindName(name)
	if xactKind == "" {
		return incorrectUsageMsg(c, "unrecognized or misplaced option '%s'", name)
	}

	// all the rest
	var (
		xactID      = id
		msg         = formatXactMsg(xactID, xname, bck)
		xactArgs    = api.XactReqArgs{ID: xactID, Kind: xactKind}
		refreshRate = calcRefreshRate(c)
		total       time.Duration
	)
	for {
		status, err := api.GetOneXactionStatus(apiBP, xactArgs)
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
		time.Sleep(refreshRate)
		if total += refreshRate; total > wasFast {
			fmt.Fprint(c.App.Writer, ".")
		}
	}
	if total > wasFast {
		fmt.Fprintln(c.App.Writer)
	}
	actionDone(c, msg+" finished")
	return nil
}

func waitDownloadHandler(c *cli.Context, id string) error {
	refreshRate := calcRefreshRate(c)
	if flagIsSet(c, progressBarFlag) {
		downloadingResult, err := newDownloaderPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}
		actionDone(c, downloadingResult.String())
		return nil
	}

	// poll at a refresh rate
	var (
		total time.Duration
		qn    = fmt.Sprintf("%s[%s]", subcmdDownload, id)
	)
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
		if total += refreshRate; total > wasFast {
			fmt.Fprint(c.App.Writer, ".")
		}
		time.Sleep(refreshRate)
	}
	if total > wasFast {
		fmt.Fprintln(c.App.Writer)
	}
	actionDone(c, qn+" finished")
	return nil
}

func waitDsortHandler(c *cli.Context, id string) error {
	refreshRate := calcRefreshRate(c)
	if flagIsSet(c, progressBarFlag) {
		dsortResult, err := newDSortPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}
		actionDone(c, dsortResult.String())
		return nil
	}

	// poll at refresh rate
	var (
		qn    = fmt.Sprintf("%s[%s]", subcmdDsort, id)
		total time.Duration
	)
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
		if total += refreshRate; total > wasFast {
			fmt.Fprint(c.App.Writer, ".")
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
	regex := parseStrFlag(c, regexFlag)
	if flagIsSet(c, allFinishedJobsFlag) || regex != "" {
		return removeDownloadRegex(c, regex)
	}

	// by job ID
	if c.NArg() < 1 {
		err := fmt.Errorf("expecting either %s argument or '--%s' option (with or without regular expression)",
			jobIDArgument, allFinishedJobsFlag.Name)
		return cannotExecuteError(c, err)
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
	regex := parseStrFlag(c, regexFlag)
	if flagIsSet(c, allFinishedJobsFlag) || regex != "" {
		return removeDsortRegex(c, regex)
	}

	// by job ID
	if c.NArg() < 1 {
		err := fmt.Errorf("expecting either %s argument or '--%s' option (with or without regular expression)",
			jobIDArgument, allFinishedJobsFlag.Name)
		return cannotExecuteError(c, err)
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
