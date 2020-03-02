// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that control running jobs in the cluster.
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
	"github.com/NVIDIA/aistore/dsort"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

var (
	xactKindsMsg = buildXactKindsMsg()

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
					ArgsUsage:    xactionWithOptionalBucketArgument,
					Description:  xactKindsMsg,
					Flags:        startCmdsFlags[subcmdStartXaction],
					Action:       startXactionHandler,
					BashComplete: xactionCompletions,
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
					ArgsUsage:    stopCommandXactionArgument,
					Description:  xactKindsMsg,
					Flags:        stopCmdsFlags[subcmdStopXaction],
					Action:       stopXactionHandler,
					BashComplete: xactionCompletions,
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
	var (
		bck      cmn.Bck
		objName  string
		xactKind = c.Args().First() // empty string if no args given
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, "xaction name")
	}

	if !cmn.IsValidXaction(xactKind) {
		return fmt.Errorf("%q is not a valid xaction", xactKind)
	}

	switch cmn.XactType[xactKind] {
	case cmn.XactTypeGlobal:
		if c.NArg() > 1 {
			fmt.Fprintf(c.App.ErrWriter, "Warning: %s is a global xaction, ignoring bucket name\n", xactKind)
		}
	case cmn.XactTypeBck:
		bck, objName = parseBckObjectURI(c.Args().Get(1))
		if objName != "" {
			return objectNameArgumentNotSupported(c, objName)
		}
		if bck, err = validateBucket(c, bck, "", false); err != nil {
			return
		}
	case cmn.XactTypeTask:
		return errors.New(`cannot start "type=task" xaction`)
	}

	if err = api.ExecXaction(defaultAPIParams, bck, xactKind, commandStart); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "started %q xaction\n", xactKind)
	return
}

func stopXactionHandler(c *cli.Context) (err error) {
	var (
		bck      cmn.Bck
		objName  string
		xactKind = c.Args().First() // empty string if no args given
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, fmt.Sprintf("xaction name or '%s'", allArgument))
	}

	if xactKind == allArgument {
		xactKind = ""
		bck.Name = c.Args().Get(1)
	} else if !cmn.IsValidXaction(xactKind) {
		return fmt.Errorf("%q is not a valid xaction", xactKind)
	} else { // valid xaction
		switch cmn.XactType[xactKind] {
		case cmn.XactTypeGlobal:
			if c.NArg() > 1 {
				fmt.Fprintf(c.App.ErrWriter, "Warning: %s is a task xaction, ignoring bucket name\n", xactKind)
			}
		case cmn.XactTypeBck:
			bck, objName = parseBckObjectURI(c.Args().Get(1))
			if objName != "" {
				return objectNameArgumentNotSupported(c, objName)
			}
			if bck, err = validateBucket(c, bck, "", false); err != nil {
				return
			}
		case cmn.XactTypeTask:
			// TODO: we probably should not ignore bucket...
			if c.NArg() > 1 {
				fmt.Fprintf(c.App.ErrWriter, "Warning: %s is a task xaction, ignoring bucket name\n", xactKind)
			}
		}
	}

	if err = api.ExecXaction(defaultAPIParams, bck, xactKind, commandStop); err != nil {
		return
	}

	if xactKind == "" {
		fmt.Fprintln(c.App.Writer, "stopped all xactions")
	} else {
		fmt.Fprintf(c.App.Writer, "stopped %q xaction\n", xactKind)
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
			message:      fmt.Sprintf("too many arguments. Got %d, expected 2. For range download please put source link in quotation marks", len(c.Args())),
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

	basePayload := cmn.DlBase{
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
		payload := cmn.DlRangeBody{
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
		payload := cmn.DlSingleBody{
			DlBase: basePayload,
			DlObj: cmn.DlObj{
				Link:    link,
				Objname: pathSuffix, // in this case pathSuffix is a full name of the object
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

	fmt.Fprintf(c.App.Writer, "download job %s has been stopped successfully.\n", id)
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

	fmt.Fprintf(c.App.Writer, "%s job %s has been stopped successfully.\n", cmn.DSortName, id)
	return
}

func buildXactKindsMsg() string {
	xactKinds := make([]string, 0, len(cmn.XactType))
	for kind := range cmn.XactType {
		xactKinds = append(xactKinds, kind)
	}
	return fmt.Sprintf("%s can be one of: %s", xactionArgument, strings.Join(xactKinds, ", "))
}
