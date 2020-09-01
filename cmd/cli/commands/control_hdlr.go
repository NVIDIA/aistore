// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/dsort"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

var (
	startCmdsFlags = map[string][]cli.Flag{
		subcmdStartXaction: {},
		subcmdStartDownload: {
			timeoutFlag,
			descriptionFlag,
			limitConnectionsFlag,
			objectsListFlag,
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

	stopCmdsFlags = map[string][]cli.Flag{
		subcmdStopXaction:  {},
		subcmdStopDownload: {},
		subcmdStopDsort:    {},
	}

	controlCmds = []cli.Command{
		{
			Name:  commandStart,
			Usage: "start jobs in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         commandPrefetch,
					Usage:        "prefetch objects from cloud buckets",
					ArgsUsage:    bucketArgument,
					Flags:        startCmdsFlags[commandPrefetch],
					Action:       prefetchHandler,
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
					Usage:     fmt.Sprintf("start a new %s job with given specification", cmn.DSortName),
					ArgsUsage: jsonSpecArgument,
					Flags:     startCmdsFlags[subcmdStartDsort],
					Action:    startDsortHandler,
				},
				{
					Name:   subcmdLRU,
					Usage:  fmt.Sprintf("start an %q xaction", cmn.ActLRU),
					Flags:  startCmdsFlags[subcmdLRU],
					Action: startLRUHandler,
				},
			},
		},
		{
			Name:  commandStop,
			Usage: "stops jobs running in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdStopXaction,
					Usage:        "stops xactions",
					ArgsUsage:    "XACTION_ID|XACTION_NAME [BUCKET_NAME]",
					Description:  xactionDesc(false),
					Flags:        stopCmdsFlags[subcmdStopXaction],
					Action:       stopXactionHandler,
					BashComplete: xactionCompletions(cmn.ActXactStop),
				},
				{
					Name:         subcmdStopDownload,
					Usage:        "stops a download job with given ID",
					ArgsUsage:    jobIDArgument,
					Flags:        stopCmdsFlags[subcmdStopDownload],
					Action:       stopDownloadHandler,
					BashComplete: downloadIDRunningCompletions,
				},
				{
					Name:         subcmdStopDsort,
					Usage:        fmt.Sprintf("stops a %s job with given ID", cmn.DSortName),
					ArgsUsage:    jobIDArgument,
					Action:       stopDsortHandler,
					BashComplete: dsortIDRunningCompletions,
				},
			},
		},
	}
)

func init() {
	controlCmds[0].Subcommands = append(controlCmds[0].Subcommands, bucketSpecificCmds...)
	controlCmds[0].Subcommands = append(controlCmds[0].Subcommands, xactionCmds()...)
}

func xactionCmds() cli.Commands {
	cmds := make(cli.Commands, 0)

	splCmdKinds := make(cmn.StringSet)
	// Add any xaction which requires a separate handler here.
	splCmdKinds.Add(cmn.ActPrefetch, cmn.ActECEncode, cmn.ActMakeNCopies, cmn.ActLRU)

	startable := listXactions(true)
	for _, xaction := range startable {
		if splCmdKinds.Contains(xaction) {
			continue
		}
		cmd := cli.Command{
			Name:   xaction,
			Usage:  fmt.Sprintf("start %s", xaction),
			Action: startXactionHandler,
		}
		if cmn.IsXactTypeBck(xaction) {
			cmd.ArgsUsage = bucketArgument
			cmd.BashComplete = bucketCompletions()
		}
		cmds = append(cmds, cmd)
	}
	return cmds
}

func startXactionHandler(c *cli.Context) (err error) {
	xactKind := c.Command.Name
	if cmn.IsXactTypeBck(xactKind) && c.NArg() == 0 {
		return missingArgumentsError(c, bucketArgument)
	}

	bck, _, err := cmn.ParseBckObjectURI(c.Args().First())

	if err != nil {
		return err
	}

	var (
		id       string
		xactArgs = api.XactReqArgs{Kind: xactKind, Bck: bck}
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

func stopXactionHandler(c *cli.Context) (err error) {
	var sid string
	if c.NArg() == 0 {
		return missingArgumentsError(c, "xaction name or id")
	}

	xactID, xactKind, bck, err := parseXactionFromArgs(c)
	if err != nil {
		return err
	}

	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck}
	if err = api.AbortXaction(defaultAPIParams, xactArgs); err != nil {
		return
	}

	if xactKind != "" && xactID != "" {
		sid = fmt.Sprintf("%s, ID=%q", xactKind, xactID)
	} else if xactKind != "" {
		sid = xactKind
	} else {
		sid = fmt.Sprintf("xaction ID=%q", xactID)
	}
	if bck.IsEmpty() {
		fmt.Fprintf(c.App.Writer, "Stopped %s\n", sid)
	} else {
		fmt.Fprintf(c.App.Writer, "Stopped %s, bucket=%s\n", sid, bck)
	}
	return
}

func startDownloadHandler(c *cli.Context) error {
	var (
		description     = parseStrFlag(c, descriptionFlag)
		timeout         = parseStrFlag(c, timeoutFlag)
		objectsListPath = parseStrFlag(c, objectsListFlag)
		id              string
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
		return &usageError{
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
	bucket, pathSuffix, err := parseDest(dst)
	if err != nil {
		return err
	}

	limitBPH, err := parseByteFlagToInt(c, limitBytesPerHourFlag)
	if err != nil {
		return err
	}
	basePayload := downloader.DlBase{
		Bck: cmn.Bck{
			Name:     bucket,
			Provider: cmn.ProviderAIS,
			Ns:       cmn.NsGlobal,
		},
		Timeout:     timeout,
		Description: description,
		Limits: downloader.DlLimits{
			Connections:  parseIntFlag(c, limitConnectionsFlag),
			BytesPerHour: int(limitBPH),
		},
	}

	// Heuristics to determine the download type.
	var dlType downloader.DlType
	if objectsListPath != "" {
		dlType = downloader.DlTypeMulti
	} else if strings.Contains(source.link, "{") && strings.Contains(source.link, "}") {
		dlType = downloader.DlTypeRange
	} else if source.cloud.bck.IsEmpty() {
		dlType = downloader.DlTypeSingle
	} else {
		cfg, err := getClusterConfig()
		if err != nil {
			return err
		}
		if _, ok := cfg.Cloud.Providers[source.cloud.bck.Provider]; ok {
			// Cloud is configured to requested bucket provider.
			dlType = downloader.DlTypeCloud

			p, err := api.HeadBucket(defaultAPIParams, basePayload.Bck)
			if err != nil {
				return err
			}
			if !p.BackendBck.Equal(source.cloud.bck) {
				fmt.Fprintf(c.App.Writer,
					"Warning: bucket %q does not have Cloud bucket %q as its *backend* - proceeding to download anyway\n",
					basePayload.Bck, source.cloud.bck,
				)
				dlType = downloader.DlTypeSingle
			}
		} else if source.cloud.prefix == "" {
			return fmt.Errorf(
				"cluster is not configured with %q provider: cannot download whole cloud bucket",
				source.cloud.bck.Provider,
			)
		} else {
			if source.link == "" {
				return fmt.Errorf(
					"cluster is not configured with %q provider: cannot download bucket's objects",
					source.cloud.bck.Provider,
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
	case downloader.DlTypeCloud:
		payload := downloader.DlCloudBody{
			DlBase: basePayload,
			Sync:   flagIsSet(c, syncFlag),
			Prefix: source.cloud.prefix,
		}
		id, err = api.DownloadWithParam(defaultAPIParams, dlType, payload)
	default:
		cmn.Assert(false)
	}

	if err != nil {
		return err
	}

	fmt.Fprintln(c.App.Writer, id)
	fmt.Fprintf(c.App.Writer, "Run `ais show download %s --progress` to monitor the progress of downloading.\n", id)
	return nil
}

func stopDownloadHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() == 0 {
		return missingArgumentsError(c, "download job ID")
	}

	if err = api.DownloadAbort(defaultAPIParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "download job %q successfully stopped\n", id)
	return
}

func startDsortHandler(c *cli.Context) (err error) {
	var (
		id       string
		specPath = parseStrFlag(c, specFileFlag)
	)
	if c.NArg() == 0 && specPath == "" {
		return missingArgumentsError(c, "job specification")
	} else if c.NArg() > 0 && specPath != "" {
		return &usageError{
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
		if _, err := io.CopyN(&b, r, cmn.MiB); err == nil {
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

func stopDsortHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() == 0 {
		return missingArgumentsError(c, cmn.DSortName+" job ID")
	}

	if err = api.AbortDSort(defaultAPIParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%s job %q successfully stopped\n", cmn.DSortName, id)
	return
}

func startLRUHandler(c *cli.Context) (err error) {
	if !flagIsSet(c, listBucketsFlag) {
		return startXactionHandler(c)
	}

	if flagIsSet(c, forceFlag) {
		warning := "Forcing LRU will evict any bucket ignoring `lru.enabled` property"
		if ok := confirm(c, "Would you like to continue?", warning); !ok {
			return
		}
	}

	bckArgs := makeList(parseStrFlag(c, listBucketsFlag), ",")
	buckets := make([]cmn.Bck, len(bckArgs))
	for idx, bckArg := range bckArgs {
		bck, _, err := cmn.ParseBckObjectURI(bckArg)
		if err != nil {
			return err
		}
		buckets[idx] = bck
	}

	var (
		id       string
		xactArgs = api.XactReqArgs{Kind: cmn.ActLRU, Buckets: buckets, Force: flagIsSet(c, forceFlag)}
	)
	if id, err = api.StartXaction(defaultAPIParams, xactArgs); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Started %s %q, %s\n", cmn.ActLRU, id, xactProgressMsg(id))
	return
}
