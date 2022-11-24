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
	"github.com/NVIDIA/aistore/dloader"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

const (
	dlWarningFmt = "%d of %d download jobs failed, for details run `ais show job download %s -v`"
)

var (
	startCmdsFlags = map[string][]cli.Flag{
		subcmdStartXaction: {},
		subcmdStartDownload: {
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
		subcmdStartDsort: {
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

	jobStartSubcmds = cli.Command{
		Name:  commandStart,
		Usage: "start asynchronous batch job",
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
				Name:      subcmdStartDownload,
				Usage:     "downloads files and objects from external sources",
				ArgsUsage: startDownloadArgument,
				Flags:     startCmdsFlags[subcmdStartDownload],
				Action:    startDownloadHandler,
			},
			{
				Name:      subcmdStartDsort,
				Usage:     fmt.Sprintf("start a new %s job with given specification", dsort.DSortName),
				ArgsUsage: jsonSpecArgument,
				Flags:     startCmdsFlags[subcmdStartDsort],
				Action:    startDsortHandler,
			},
			{
				Name:   subcmdLRU,
				Usage:  fmt.Sprintf("start %q xaction", apc.ActLRU),
				Flags:  startCmdsFlags[subcmdLRU],
				Action: startLRUHandler,
			},
			{
				Name:         subcmdStgCleanup,
				Usage:        "cleanup storage: remove moved or deleted objects, remove old/obsolete workfiles",
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
		},
	}
)

// xaction

func startXactionHandler(c *cli.Context) (err error) {
	xactKind := c.Command.Name
	return startXactionKindHandler(c, xactKind)
}

func startXactionKindHandler(c *cli.Context, xactKind string) (err error) {
	var (
		bck cmn.Bck
		sid string
	)

	if xact.IsBckScope(xactKind) {
		bck, err = parseBckURI(c, c.Args().First())
		if err != nil {
			return err
		}
	}
	if sid, err = isResilverNode(c, xactKind); err != nil {
		return err
	}

	return startXaction(c, xactKind, bck, sid)
}

func startXaction(c *cli.Context, xactKind string, bck cmn.Bck, sid string) (err error) {
	if xact.IsBckScope(xactKind) {
		if _, err = headBucket(bck, false /* don't add */); err != nil {
			return err
		}
	}

	var (
		id       string
		xactArgs = api.XactReqArgs{Kind: xactKind, Bck: bck, DaemonID: sid}
	)

	if id, err = api.StartXaction(apiBP, xactArgs); err != nil {
		return
	}

	if id != "" {
		fmt.Fprintf(c.App.Writer, "Started xaction kind=%s, ID=%s. %s\n", xactKind, id, xactProgressMsg(id))
	} else {
		fmt.Fprintf(c.App.Writer, "Started xaction kind-%s\n", xactKind)
	}

	return
}

func isResilverNode(c *cli.Context, xactKind string) (sid string, err error) {
	if xactKind != apc.ActResilver || c.NArg() == 0 {
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
		return missingArgumentsError(c, "source", "destination")
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

	basePayload := dloader.Base{
		Bck:              bck,
		Timeout:          timeout,
		Description:      description,
		ProgressInterval: progressInterval,
		Limits: dloader.Limits{
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
	var dlType dloader.Type
	if objectsListPath != "" {
		dlType = dloader.TypeMulti
	} else if strings.Contains(source.link, "{") && strings.Contains(source.link, "}") {
		dlType = dloader.TypeRange
	} else if source.backend.bck.IsEmpty() {
		dlType = dloader.TypeSingle
	} else {
		cfg, err := getRandTargetConfig()
		if err != nil {
			return err
		}
		if _, ok := cfg.Backend.Providers[source.backend.bck.Provider]; ok { // backend is configured
			dlType = dloader.TypeBackend

			p, err := api.HeadBucket(apiBP, basePayload.Bck, false /* don't add */)
			if err != nil {
				return err
			}
			if !p.BackendBck.Equal(&source.backend.bck) {
				warn := fmt.Sprintf("%s does not have Cloud bucket %s as its *backend* - proceeding to download anyway.",
					basePayload.Bck, source.backend.bck)
				actionWarn(c, warn)
				dlType = dloader.TypeSingle
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
			dlType = dloader.TypeSingle
		}
	}

	switch dlType {
	case dloader.TypeSingle:
		payload := dloader.SingleBody{
			Base: basePayload,
			SingleObj: dloader.SingleObj{
				Link:    source.link,
				ObjName: pathSuffix, // in this case pathSuffix is a full name of the object
			},
		}
		id, err = api.DownloadWithParam(apiBP, dlType, payload)
	case dloader.TypeMulti:
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
		payload := dloader.MultiBody{
			Base:           basePayload,
			ObjectsPayload: objects,
		}
		id, err = api.DownloadWithParam(apiBP, dlType, payload)
	case dloader.TypeRange:
		payload := dloader.RangeBody{
			Base:     basePayload,
			Subdir:   pathSuffix, // in this case pathSuffix is a subdirectory in which the objects are to be saved
			Template: source.link,
		}
		id, err = api.DownloadWithParam(apiBP, dlType, payload)
	case dloader.TypeBackend:
		payload := dloader.BackendBody{
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
		warn := fmt.Sprintf(dlWarningFmt, resp.ErrorCnt, resp.ScheduledCnt, id)
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
		resp   *dloader.StatusResp
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
		warn := fmt.Sprintf(dlWarningFmt, resp.ErrorCnt, resp.ScheduledCnt, id)
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
		return missingArgumentsError(c, "job specification")
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

	fmt.Fprintf(c.App.Writer, "Started %s %s. %s\n", apc.ActLRU, id, xactProgressMsg(id))
	return
}

func startPrefetchHandler(c *cli.Context) (err error) {
	printDryRunHeader(c)

	if c.NArg() == 0 {
		return incorrectUsageMsg(c, "missing bucket")
	}
	if c.NArg() > 1 {
		return incorrectUsageMsg(c, "too many arguments or unrecognized option '%+v'", c.Args()[1:])
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
		return listOrRangeOp(c, commandPrefetch, bck)
	}

	return missingArgumentsError(c, "object list or range")
}
