// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package commands

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
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/xact"
	"github.com/fatih/color"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

const (
	dlWarningFmt  = "Warning: %d of %d download jobs failed, for details run `ais show job download %s -v`.\n"
	dlProgressFmt = "Run `ais show job download %s --progress` to monitor the progress.\n"
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
		Usage: "start jobs in the cluster",
		Subcommands: []cli.Command{
			{
				Name:         commandPrefetch,
				Usage:        "prefetch objects from remote buckets",
				ArgsUsage:    bucketArgument,
				Flags:        startCmdsFlags[commandPrefetch],
				Action:       startPrefetchHandler,
				BashComplete: bucketCompletions(bckCompletionsOpts{multiple: true}),
			},
			{
				Name:      subcmdStartDownload,
				Usage:     "start a download job (downloads objects from external source)",
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
				Usage:        "perform storage cleanup: remove deleted objects and old/obsolete workfiles",
				ArgsUsage:    listAnyCommandArgument,
				Flags:        storageCmdFlags[subcmdStgCleanup],
				Action:       cleanupStorageHandler,
				BashComplete: bucketCompletions(),
			},
			{
				Name:  commandETL,
				Usage: "start ETL job on the cluster",
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
		if _, err = headBucket(bck); err != nil {
			return err
		}
	}

	var (
		id       string
		xactArgs = api.XactReqArgs{Kind: xactKind, Bck: bck, DaemonID: sid}
	)

	if id, err = api.StartXaction(defaultAPIParams, xactArgs); err != nil {
		return
	}

	if id != "" {
		fmt.Fprintf(c.App.Writer, "Started %s %q, %s\n", xactKind, id, xactProgressMsg(id))
	} else {
		fmt.Fprintf(c.App.Writer, "Started %s\n", xactKind)
	}

	return
}

func isResilverNode(c *cli.Context, xactKind string) (sid string, err error) {
	if xactKind != apc.ActResilver || c.NArg() == 0 {
		return "", nil
	}
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return "", err
	}
	sid = c.Args().First()
	if node := smap.GetTarget(sid); node == nil {
		return "", fmt.Errorf("node %q is not a target. Run 'ais show cluster target' to see a list of all targets", sid)
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

	basePayload := downloader.DlBase{
		Bck:              bck,
		Timeout:          timeout,
		Description:      description,
		ProgressInterval: progressInterval,
		Limits: downloader.DlLimits{
			Connections:  parseIntFlag(c, limitConnectionsFlag),
			BytesPerHour: int(limitBPH),
		},
	}

	if basePayload.Bck.Props, err = api.HeadBucket(defaultAPIParams, basePayload.Bck); err != nil {
		if !cmn.IsStatusNotFound(err) {
			return err
		}
		fmt.Fprintf(c.App.Writer, "Warning: destination bucket %q doesn't exist. A bucket with default properties will be created!\n", basePayload.Bck)
	}

	// Heuristics to determine the download type.
	var dlType downloader.DlType
	if objectsListPath != "" {
		dlType = downloader.DlTypeMulti
	} else if strings.Contains(source.link, "{") && strings.Contains(source.link, "}") {
		dlType = downloader.DlTypeRange
	} else if source.backend.bck.IsEmpty() {
		dlType = downloader.DlTypeSingle
	} else {
		cfg, err := getRandTargetConfig()
		if err != nil {
			return err
		}
		if _, ok := cfg.Backend.Providers[source.backend.bck.Provider]; ok {
			// Cloud is configured to requested backend provider.
			dlType = downloader.DlTypeBackend

			p, err := api.HeadBucket(defaultAPIParams, basePayload.Bck)
			if err != nil {
				return err
			}
			if !p.BackendBck.Equal(&source.backend.bck) {
				color.New(color.FgYellow).Fprintf(c.App.ErrWriter,
					"Warning: bucket %q does not have Cloud bucket %q as its *backend* - proceeding to download anyway\n",
					basePayload.Bck, source.backend.bck,
				)
				dlType = downloader.DlTypeSingle
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
			dlType = downloader.DlTypeSingle
		}
	}

	switch dlType {
	case downloader.DlTypeSingle:
		payload := downloader.DlSingleBody{
			DlBase: basePayload,
			DlSingleObj: downloader.DlSingleObj{
				Link:    source.link,
				ObjName: pathSuffix, // in this case pathSuffix is a full name of the object
			},
		}
		id, err = api.DownloadWithParam(defaultAPIParams, dlType, payload)
	case downloader.DlTypeMulti:
		var objects []string
		{
			file, err := os.Open(objectsListPath)
			if err != nil {
				return err
			}
			if err := jsoniter.NewDecoder(file).Decode(&objects); err != nil {
				return fmt.Errorf("%q file doesn't seem to contain JSON array of strings: %v", objectsListPath, err)
			}
		}
		for i, object := range objects {
			objects[i] = source.link + "/" + object
		}
		payload := downloader.DlMultiBody{
			DlBase:         basePayload,
			ObjectsPayload: objects,
		}
		id, err = api.DownloadWithParam(defaultAPIParams, dlType, payload)
	case downloader.DlTypeRange:
		payload := downloader.DlRangeBody{
			DlBase:   basePayload,
			Subdir:   pathSuffix, // in this case pathSuffix is a subdirectory in which the objects are to be saved
			Template: source.link,
		}
		id, err = api.DownloadWithParam(defaultAPIParams, dlType, payload)
	case downloader.DlTypeBackend:
		payload := downloader.DlBackendBody{
			DlBase: basePayload,
			Sync:   flagIsSet(c, syncFlag),
			Prefix: source.backend.prefix,
		}
		id, err = api.DownloadWithParam(defaultAPIParams, dlType, payload)
	default:
		debug.Assert(false)
	}

	if err != nil {
		return err
	}

	fmt.Fprintln(c.App.Writer, id)

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
	downloadingResult, err := newDownloaderPB(defaultAPIParams, id, refreshRate).run()
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
	resp, err := api.DownloadStatus(defaultAPIParams, id, true /*only active*/)
	if err != nil {
		return err
	}
	if resp.ErrorCnt != 0 {
		fmt.Fprintf(c.App.ErrWriter, dlWarningFmt, resp.ErrorCnt, resp.ScheduledCnt, id)
	} else {
		fmt.Fprintf(c.App.Writer, "All files (%d files) successfully downloaded.\n", resp.FinishedCnt)
	}
	return nil
}

func bgDownload(c *cli.Context, id string) (err error) {
	const (
		checkTimeout  = 5 * time.Second
		checkInterval = time.Second
	)
	var (
		passed time.Duration
		resp   downloader.DlStatusResp
	)
	// In a non-interactive mode, allow the downloader to start jobs before checking.
	for passed < checkTimeout {
		time.Sleep(checkInterval)
		passed += checkInterval
		resp, err = api.DownloadStatus(defaultAPIParams, id, true /*only active*/)
		if err != nil {
			return err
		}
		if resp.ErrorCnt != 0 || resp.FinishedCnt != 0 || resp.FinishedTime.UnixNano() != 0 {
			break
		}
	}

	if resp.ErrorCnt != 0 {
		fmt.Fprintf(c.App.ErrWriter, dlWarningFmt, resp.ErrorCnt, resp.ScheduledCnt, id)
	} else if resp.FinishedTime.UnixNano() != 0 {
		fmt.Fprintf(c.App.Writer, "All files (%d files) successfully downloaded.\n", resp.FinishedCnt)
	} else {
		fmt.Fprintf(c.App.Writer, dlProgressFmt, id)
	}
	return err
}

func waitDownload(c *cli.Context, id string) (err error) {
	var (
		aborted     bool
		refreshRate = calcRefreshRate(c)
	)

	for {
		resp, err := api.DownloadStatus(defaultAPIParams, id, true)
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

	if id, err = api.StartDSort(defaultAPIParams, rs); err != nil {
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
		warning := "forcing LRU will evict any bucket ignoring `lru.enabled` property"
		if ok := confirm(c, "Would you like to continue?", warning); !ok {
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
	if id, err = api.StartXaction(defaultAPIParams, xactArgs); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Started %s %q, %s\n", apc.ActLRU, id, xactProgressMsg(id))
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
	if _, err = headBucket(bck); err != nil {
		return
	}

	if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
		return listOrRangeOp(c, commandPrefetch, bck)
	}

	return missingArgumentsError(c, "object list or range")
}
