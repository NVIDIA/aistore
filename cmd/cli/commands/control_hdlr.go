// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
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
		},
		subcmdStartDsort: {
			specFileFlag,
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
					Name:         subcmdStartXaction,
					Usage:        "start an xaction",
					ArgsUsage:    "XACTION_NAME [BUCKET_NAME]",
					Description:  xactionDesc(cmn.ActXactStart),
					Flags:        startCmdsFlags[subcmdStartXaction],
					Action:       startXactionHandler,
					BashComplete: xactionCompletions(cmn.ActXactStart),
				},
				{
					Name:         subcmdStartDownload,
					Usage:        "start a download job (downloads objects from external source)",
					ArgsUsage:    startDownloadArgument,
					Flags:        startCmdsFlags[subcmdStartDownload],
					Action:       startDownloadHandler,
					BashComplete: noSuggestionCompletions(2),
				},
				{
					Name:         subcmdStartDsort,
					Usage:        fmt.Sprintf("start a new %s job with given specification", cmn.DSortName),
					ArgsUsage:    jsonSpecArgument,
					Flags:        startCmdsFlags[subcmdStartDsort],
					Action:       startDsortHandler,
					BashComplete: noSuggestionCompletions(1),
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
					Description:  xactionDesc(cmn.ActXactStop),
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

func startXactionHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "xaction name")
	}

	xactID, xactKind, bck, err := parseXactionFromArgs(c)
	if err != nil {
		return err
	}
	if xactID != "" {
		return fmt.Errorf("%q is not a valid xaction", xactID)
	}
	var (
		id       string
		xactArgs = api.XactReqArgs{Kind: xactKind, Bck: bck}
	)
	if id, err = api.StartXaction(defaultAPIParams, xactArgs); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "Started %q xaction (ID: %s).\n", xactKind, id)
	return
}

func stopXactionHandler(c *cli.Context) (err error) {
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

	if xactID != "" {
		fmt.Fprintf(c.App.Writer, "Stopped xaction with \"id=%s\".\n", xactID)
	} else {
		if bck.IsEmpty() {
			fmt.Fprintf(c.App.Writer, "Stopped xaction with \"kind=%s\".\n", xactKind)
		} else {
			fmt.Fprintf(c.App.Writer, "Stopped xaction with \"kind=%s\" && \"bucket=%s\".\n", xactKind, bck)
		}
	}
	return
}

func startDownloadHandler(c *cli.Context) error {
	var (
		description = parseStrFlag(c, descriptionFlag)
		timeout     = parseStrFlag(c, timeoutFlag)
		id          string
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, "source", "destination")
	}
	if c.NArg() == 1 {
		return missingArgumentsError(c, "destination")
	}
	if c.NArg() > 2 {
		return &usageError{
			context:      c,
			message:      fmt.Sprintf("too many arguments, got %d, expected 2\n\nFor range download put source link in quotation marks", len(c.Args())),
			helpData:     c.Command,
			helpTemplate: cli.CommandHelpTemplate,
		}
	}

	source, dest := c.Args().Get(0), c.Args().Get(1)
	link, err := parseSource(source)
	if err != nil {
		return err
	}
	bucket, pathSuffix, err := parseDest(dest)
	if err != nil {
		return err
	}

	basePayload := downloader.DlBase{
		Bck: cmn.Bck{
			Name:     bucket,
			Provider: cmn.ProviderAIS, // NOTE: currently downloading only to ais buckets is supported
			Ns:       cmn.NsGlobal,
		},
		Timeout:     timeout,
		Description: description,
	}

	if strings.Contains(source, "{") && strings.Contains(source, "}") {
		// Range
		payload := downloader.DlRangeBody{
			DlBase:   basePayload,
			Subdir:   pathSuffix, // in this case pathSuffix is a subdirectory in which the objects are to be saved
			Template: link,
		}
		id, err = api.DownloadRangeWithParam(defaultAPIParams, payload)
		if err != nil {
			return err
		}
	} else {
		// Single
		payload := downloader.DlSingleBody{
			DlBase: basePayload,
			DlObj: downloader.DlObj{
				Link:    link,
				ObjName: pathSuffix, // in this case pathSuffix is a full name of the object
			},
		}
		id, err = api.DownloadSingleWithParam(defaultAPIParams, payload)
		if err != nil {
			return err
		}
	}

	fmt.Fprintln(c.App.Writer, id)
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
