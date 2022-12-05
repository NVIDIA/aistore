// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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

// top level job cmd
var (
	jobCmd = cli.Command{
		Name:        commandJob,
		Usage:       "monitor, query, start/stop and manage jobs and eXtended actions (xactions)",
		Subcommands: jobSub,
	}
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
		subcmdXaction: {},
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
		},
	}
)

// job stop
var (
	stopCmdsFlags = map[string][]cli.Flag{
		subcmdXaction:  {},
		subcmdDownload: {},
		subcmdDsort:    {},
	}

	jobStopSub = cli.Command{
		Name:  commandStop,
		Usage: "stop batch job",
		Subcommands: []cli.Command{
			{
				Name:         subcmdXaction,
				Usage:        "stop xaction",
				ArgsUsage:    "XACTION | XACTION_ID [BUCKET]",
				Description:  xactionDesc(false),
				Flags:        stopCmdsFlags[subcmdXaction],
				Action:       stopXactionHandler,
				BashComplete: xactCompletions,
			},
			{
				Name:         subcmdDownload,
				Usage:        "stop download",
				ArgsUsage:    jobIDArgument,
				Flags:        stopCmdsFlags[subcmdDownload],
				Action:       stopDownloadHandler,
				BashComplete: downloadIDRunningCompletions,
			},
			{
				Name:         subcmdDsort,
				Usage:        "stop " + dsort.DSortName + " job",
				ArgsUsage:    jobIDArgument,
				Action:       stopDsortHandler,
				BashComplete: dsortIDRunningCompletions,
			},
			{
				Name:   commandRebalance,
				Usage:  "stop rebalancing ais cluster",
				Flags:  clusterCmdsFlags[commandStop],
				Action: stopClusterRebalanceHandler,
			},
			makeAlias(stopCmdETL, "", true, commandETL),
		},
	}
)

// job wait
var (
	waitCmdsFlags = map[string][]cli.Flag{
		subcmdXaction: {
			timeoutFlag,
		},
		subcmdDownload: {
			refreshFlag,
			progressBarFlag,
		},
		subcmdDsort: {
			refreshFlag,
			progressBarFlag,
		},
	}

	jobWaitSub = cli.Command{
		Name:  commandWait,
		Usage: "wait for a specific task",
		Subcommands: []cli.Command{
			{
				Name:         subcmdXaction,
				Usage:        "wait for an xaction to finish",
				ArgsUsage:    "XACTION_ID|XACTION_KIND [BUCKET]",
				Flags:        waitCmdsFlags[subcmdXaction],
				Action:       waitXactionHandler,
				BashComplete: xactCompletions,
			},
			{
				Name:         subcmdDownload,
				Usage:        "wait for download to finish",
				ArgsUsage:    jobIDArgument,
				Flags:        waitCmdsFlags[subcmdDownload],
				Action:       waitDownloadHandler,
				BashComplete: downloadIDRunningCompletions,
			},
			{
				Name:         subcmdDsort,
				Usage:        "wait for " + dsort.DSortName + " job to finish",
				ArgsUsage:    jobIDArgument,
				Flags:        waitCmdsFlags[subcmdDsort],
				Action:       waitDSortHandler,
				BashComplete: dsortIDRunningCompletions,
			},
		},
	}
)

// job remove
var (
	removeCmdsFlags = map[string][]cli.Flag{
		subcmdDownload: {
			allJobsFlag,
		},
		subcmdDsort: {},
	}

	jobRemoveSub = cli.Command{
		Name:  commandRemove,
		Usage: "remove finished jobs",
		Subcommands: []cli.Command{
			{
				Name:         subcmdDownload,
				Usage:        "remove finished download job(s)",
				ArgsUsage:    jobIDArgument,
				Flags:        removeCmdsFlags[subcmdDownload],
				Action:       removeDownloadHandler,
				BashComplete: downloadIDFinishedCompletions,
			},
			{
				Name:         subcmdDsort,
				Usage:        "remove finished " + dsort.DSortName + " job",
				ArgsUsage:    jobIDArgument,
				Flags:        removeCmdsFlags[subcmdDsort],
				Action:       removeDsortHandler,
				BashComplete: dsortIDFinishedCompletions,
			},
		},
	}
)

func initJobSub() {
	// add to `ais job start`
	jobStartSub := jobSub[0].Subcommands
	jobStartSub = append(jobStartSub, storageSvcCmds...)
	jobStartSub = append(jobStartSub, startableXactions(jobStartSub)...)

	jobSub[0].Subcommands = jobStartSub
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
		if xact.IsSameScope(xname, xact.ScopeB) {
			cmd.ArgsUsage = bucketArgument
			cmd.BashComplete = bucketCompletions(bcmplop{})
		} else if xact.IsSameScope(xname, xact.ScopeGB) {
			cmd.ArgsUsage = optionalBucketArgument
			cmd.BashComplete = bucketCompletions(bcmplop{})
		}
		cmds = append(cmds, cmd)
	}
	return cmds
}

//
// job start
//

func startXactionHandler(c *cli.Context) (err error) {
	xname := c.Command.Name
	return startXactionKindHandler(c, xname)
}

func startXactionKindHandler(c *cli.Context, xname string) (err error) {
	var (
		bck cmn.Bck
		sid string
	)
	if xact.IsSameScope(xname, xact.ScopeB, xact.ScopeGB) {
		bck, err = parseBckURI(c, c.Args().First())
		if err != nil {
			return err
		}
	}
	if sid, err = isResilverNode(c, xname); err != nil {
		return err
	}

	return startXaction(c, xname, bck, sid)
}

func startXaction(c *cli.Context, xname string, bck cmn.Bck, sid string) error {
	if !bck.IsEmpty() {
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

func isResilverNode(c *cli.Context, xname string) (sid string, err error) {
	if xname != apc.ActResilver || c.NArg() == 0 {
		return "", nil
	}
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return "", err
	}
	sid = c.Args().First()
	if node := smap.GetTarget(sid); node == nil {
		return "", fmt.Errorf("node %s is not a target. Run 'ais show cluster target' to see a list of all targets", sid)
	}
	return
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
		cfg, err := getRandTargetConfig()
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
		bck, err := parseBckURI(c, bckArg)
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
	bck, err := parseBckURI(c, c.Args().First())
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

func stopXactionHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	// parse, validate
	_, xactID, xactKind, bck, err := parseXactionFromArgs(c)
	if err != nil {
		return err
	}
	msg := formatStoppedMsg(xactID, xactKind, bck)

	// query
	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind}
	snap, err := getXactSnap(xactArgs)
	if err != nil {
		return fmt.Errorf("Cannot stop %s: %v", formatStoppedMsg(xactID, xactKind, bck), err)
	}
	if snap == nil {
		fmt.Fprintf(c.App.Writer, "%s not found, nothing to do\n", msg)
		return nil
	}

	// reformat
	if xactID == "" {
		xactID = snap.ID
	}
	debug.Assertf(xactID == snap.ID, "%q, %q", xactID, snap.ID)
	msg = formatStoppedMsg(xactID, xactKind, bck)

	var s string
	if snap.IsAborted() {
		s = " (aborted)"
	}
	if snap.IsAborted() || snap.Finished() {
		fmt.Fprintf(c.App.Writer, "%s is already finished%s, nothing to do\n", msg, s)
		return nil
	}

	// abort
	args := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck}
	if err := api.AbortXaction(apiBP, args); err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "Stopped %s\n", msg)
	return nil
}

func formatStoppedMsg(xactID, xactKind string, bck cmn.Bck) string {
	var sb string
	if !bck.IsEmpty() {
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

func stopDownloadHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	if err = api.AbortDownload(apiBP, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Stopped download job %s\n", id)
	return
}

func stopDsortHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	if err = api.AbortDSort(apiBP, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Stopped %s job %s\n", dsort.DSortName, id)
	return
}

//
// job wait
//

func waitXactionHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	_, xactID, xactKind, bck, err := parseXactionFromArgs(c)
	if err != nil {
		return err
	}

	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck, Timeout: parseDurationFlag(c, timeoutFlag)}
	status, err := api.WaitForXactionIC(apiBP, xactArgs)
	if err != nil {
		return err
	}
	if status.Aborted() {
		if xactID != "" {
			return fmt.Errorf("xaction %q was aborted", xactID)
		}
		if bck.IsEmpty() {
			return fmt.Errorf("xaction %q was aborted", xactKind)
		}
		return fmt.Errorf("xaction %q (bucket %q) was aborted", xactKind, bck)
	}
	return nil
}

func waitDownloadHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	var (
		aborted     bool
		refreshRate = calcRefreshRate(c)
		id          = c.Args()[0]
	)

	if flagIsSet(c, progressBarFlag) {
		downloadingResult, err := newDownloaderPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}

		fmt.Fprintln(c.App.Writer, downloadingResult)
		return nil
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
	}

	if aborted {
		return fmt.Errorf("download job with id %q was aborted", id)
	}
	return nil
}

func waitDSortHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	var (
		aborted     bool
		refreshRate = calcRefreshRate(c)
		id          = c.Args()[0]
	)

	if flagIsSet(c, progressBarFlag) {
		dsortResult, err := newDSortPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}
		fmt.Fprintln(c.App.Writer, dsortResult)
		return nil
	}

	for {
		resp, err := api.MetricsDSort(apiBP, id)
		if err != nil {
			return err
		}

		finished := true
		for _, targetMetrics := range resp {
			aborted = aborted || targetMetrics.Aborted.Load()
			finished = finished && targetMetrics.Creation.Finished
		}
		if aborted || finished {
			break
		}
		time.Sleep(refreshRate)
	}

	if aborted {
		return fmt.Errorf("dsort job with id %q was aborted", id)
	}
	return nil
}

//
// job remove
//

func removeDownloadHandler(c *cli.Context) (err error) {
	id := c.Args().First()
	if flagIsSet(c, allJobsFlag) {
		return removeDownloadRegex(c)
	}
	if c.NArg() < 1 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if err = api.RemoveDownload(apiBP, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "removed download job %q\n", id)
	return
}

func removeDownloadRegex(c *cli.Context) (err error) {
	var (
		dlList dload.JobInfos
		regex  = ".*"
		cnt    int
		failed bool
	)
	dlList, err = api.DownloadGetList(apiBP, regex, false /*onlyActive*/)
	if err != nil {
		return err
	}
	for _, dl := range dlList {
		if !dl.JobFinished() {
			continue
		}
		if err = api.RemoveDownload(apiBP, dl.ID); err == nil {
			fmt.Fprintf(c.App.Writer, "removed download job %q\n", dl.ID)
			cnt++
		} else {
			fmt.Fprintf(c.App.Writer, "failed to remove download job %q, err: %v\n", dl.ID, err)
			failed = true
		}
	}
	if cnt == 0 && !failed {
		fmt.Fprintf(c.App.Writer, "no finished download jobs, nothing to do\n")
	}
	return
}

func removeDsortHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() < 1 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	if err = api.RemoveDSort(apiBP, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "removed %s job %q\n", dsort.DSortName, id)
	return
}
