// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that control running jobs in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/urfave/cli"
)

var (
	startCmdsFlags = map[string][]cli.Flag{
		subcmdStartXaction: {},
		subcmdStartDownload: {
			timeoutFlag,
			descriptionFlag,
		},
		subcmdStartDsort: {},
	}

	stopCmdsFlags = map[string][]cli.Flag{
		subcmdStopXaction:  {},
		subcmdStopDownload: {},
		subcmdStopDsort:    {},
	}

	controlCmds = []cli.Command{
		{
			Name:  commandStart,
			Usage: "starts jobs in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdStartXaction,
					Usage:        "starts an xaction",
					ArgsUsage:    xactionWithOptionalBucketArgumentText,
					Description:  xactKindsMsg,
					Flags:        startCmdsFlags[subcmdStartXaction],
					Action:       startXactionHandler,
					BashComplete: xactStartCompletions,
				},
				{
					Name:         subcmdStartDownload,
					Usage:        "starts a download job (downloads objects from external source)",
					ArgsUsage:    downloadStartArgumentText,
					Flags:        startCmdsFlags[subcmdStartDownload],
					Action:       startDownloadHandler,
					BashComplete: nRequiredArgsCompletions(2),
				},
				{
					Name:         subcmdStartDsort,
					Usage:        fmt.Sprintf("start new %s job with given specification", cmn.DSortName),
					ArgsUsage:    jsonSpecArgumentText,
					Flags:        startCmdsFlags[subcmdStartDsort],
					Action:       startDsortHandler,
					BashComplete: nRequiredArgsCompletions(1),
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
					ArgsUsage:    xactionStopStatsCommandArgumentText,
					Description:  xactKindsMsg,
					Flags:        stopCmdsFlags[subcmdStopXaction],
					Action:       stopXactionHandler,
					BashComplete: xactStopStatsCompletions,
				},
				{
					Name:         subcmdStopDownload,
					Usage:        "stops a download job with given ID",
					ArgsUsage:    idArgumentText,
					Flags:        stopCmdsFlags[subcmdStopDownload],
					Action:       stopDownloadHandler,
					BashComplete: downloadIDListRunning,
				},
				{
					Name:         subcmdStopDsort,
					Usage:        fmt.Sprintf("stops a %s job with given ID", cmn.DSortName),
					ArgsUsage:    idArgumentText,
					Action:       stopDsortHandler,
					BashComplete: dsortIDListRunning,
				},
			},
		},
	}
)

func startXactionHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		xaction    = c.Args().First() // empty string if no args given
		bucket     string
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, "xaction name")
	}

	if _, ok := cmn.ValidXact(xaction); !ok {
		return fmt.Errorf("%q is not a valid xaction", xaction)
	}

	if !bucketXactions.Contains(xaction) { // global xaction
		if c.NArg() > 1 {
			fmt.Fprintf(c.App.ErrWriter, "Warning: %s is a global xaction, ignoring bucket name\n", xaction)
		}
	} else { // bucket related xaction
		bucket = c.Args().Get(1)
		if bucket == "" {
			bucket, _ = os.LookupEnv(aisBucketEnvVar)
			if bucket == "" {
				return missingArgumentsError(c, "bucket name")
			}
		}
	}

	if err = api.ExecXaction(baseParams, xaction, commandStart, bucket); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "started %q xaction\n", xaction)
	return
}

func stopXactionHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		xaction    = c.Args().First() // empty string if no args given
		bucket     string
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, fmt.Sprintf("xaction name or '%s'", allArgumentText))
	}

	if xaction == allArgumentText {
		xaction = ""
		bucket = c.Args().Get(1)
		if bucket == "" {
			bucket, _ = os.LookupEnv(aisBucketEnvVar)
		}
	} else if _, ok := cmn.ValidXact(xaction); !ok {
		return fmt.Errorf("%q is not a valid xaction", xaction)
	} else { // valid xaction
		if bucketXactions.Contains(xaction) {
			bucket = c.Args().Get(1)
			if bucket == "" {
				bucket, _ = os.LookupEnv(aisBucketEnvVar)
				if bucket == "" {
					return missingArgumentsError(c, "bucket name")
				}
			}
		} else if c.NArg() > 1 {
			fmt.Fprintf(c.App.ErrWriter, "Warning: %s is a global xaction, ignoring bucket name\n", xaction)
		}
	}

	if err = api.ExecXaction(baseParams, xaction, commandStop, bucket); err != nil {
		return
	}

	if xaction == "" {
		fmt.Fprintln(c.App.Writer, "stopped all xactions")
	} else {
		fmt.Fprintf(c.App.Writer, "stopped %q xaction\n", xaction)
	}
	return
}

func startDownloadHandler(c *cli.Context) error {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		description = parseStrFlag(c, descriptionFlag)
		timeout     = parseStrFlag(c, timeoutFlag)
		id          string
	)

	basePayload := cmn.DlBase{
		Provider:    cmn.AIS, // NOTE: currently downloading only to ais buckets is supported
		Timeout:     timeout,
		Description: description,
	}

	if c.NArg() == 0 {
		return missingArgumentsError(c, "source", "destination")
	}
	if c.NArg() == 1 {
		return missingArgumentsError(c, "destination")
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
	basePayload.Bucket = bucket

	if strings.Contains(source, "{") && strings.Contains(source, "}") {
		// Range
		payload := cmn.DlRangeBody{
			DlBase:   basePayload,
			Subdir:   pathSuffix, // in this case pathSuffix is a subdirectory in which the objects are to be saved
			Template: link,
		}
		id, err = api.DownloadRangeWithParam(baseParams, payload)
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
		id, err = api.DownloadSingleWithParam(baseParams, payload)
		if err != nil {
			return err
		}
	}

	fmt.Fprintln(c.App.Writer, id)
	return nil
}

func stopDownloadHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         = c.Args().First()
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, "download job ID")
	}

	if err = api.DownloadAbort(baseParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "download job %s has been stopped successfully\n", id)
	return
}

func startDsortHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         string
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, "job specification")
	}

	var rs dsort.RequestSpec
	body := c.Args().First()
	if err := json.Unmarshal([]byte(body), &rs); err != nil {
		return err
	}

	if id, err = api.StartDSort(baseParams, rs); err != nil {
		return
	}

	fmt.Fprintln(c.App.Writer, id)
	return
}

func stopDsortHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         = c.Args().First()
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, cmn.DSortName+" job ID")
	}

	if err = api.AbortDSort(baseParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%s job %s has been stopped successfully\n\n", cmn.DSortName, id)
	return
}

func buildXactKindsMsg() string {
	xactKinds := make([]string, 0, len(cmn.XactKind))

	for kind := range cmn.XactKind {
		xactKinds = append(xactKinds, kind)
	}

	return fmt.Sprintf("%s can be one of: %s", xactionArgumentText, strings.Join(xactKinds, ", "))
}

func bucketXactionNames() cmn.StringSet {
	result := make(cmn.StringSet)

	for name, meta := range cmn.XactKind {
		if !meta.IsGlobal {
			result[name] = struct{}{}
		}
	}

	return result
}
