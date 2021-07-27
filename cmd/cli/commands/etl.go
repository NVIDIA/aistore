// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/etl"
	"github.com/fatih/color"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
)

var etlCmd = cli.Command{
	Name:  commandETL,
	Usage: "execute custom transformations on objects",
	Subcommands: []cli.Command{
		{
			Name: subcmdInit,
			Subcommands: []cli.Command{
				{
					Name:      subcmdSpec,
					Usage:     "start an ETL with YAML Pod specification",
					ArgsUsage: "SPEC_FILE",
					Action:    etlInitSpecHandler,
				},
				{
					Name:  subcmdCode,
					Usage: "start an ETL with transformation source code",
					Flags: []cli.Flag{
						fromFileFlag,
						depsFileFlag,
						runtimeFlag,
						commTypeFlag,
						waitTimeoutFlag,
						etlUUID,
					},
					Action: etlInitCodeHandler,
				},
			},
		},

		{
			Name:   subcmdList,
			Usage:  "list all running ETLs",
			Action: etlListHandler,
		},
		{
			Name:         subcmdLogs,
			Usage:        "retrieve logs produced by an ETL",
			ArgsUsage:    "ETL_ID [TARGET_ID]",
			Action:       etlLogsHandler,
			BashComplete: etlIDCompletions,
		},
		{
			Name:         subcmdStop,
			Usage:        "stop ETL",
			ArgsUsage:    "[ETL_ID...]",
			Action:       etlStopHandler,
			BashComplete: etlIDCompletions,
			Flags:        []cli.Flag{allETLStopFlag},
		},
		{
			Name:         subcmdObject,
			Usage:        "transform an object",
			ArgsUsage:    "ETL_ID BUCKET/OBJECT_NAME OUTPUT",
			Action:       etlObjectHandler,
			BashComplete: etlIDCompletions,
		},
		{
			Name:      subcmdBucket,
			Usage:     "transform bucket and put results into another bucket",
			ArgsUsage: "ETL_ID SRC_BUCKET DST_BUCKET",
			Action:    etlBucketHandler,
			Flags: []cli.Flag{
				etlExtFlag,
				cpBckPrefixFlag,
				cpBckDryRunFlag,
				waitFlag,
				etlBucketRequestTimeout,
			},
			BashComplete: manyBucketsCompletions([]cli.BashCompleteFunc{etlIDCompletions}, 1, 2),
		},
	},
}

func etlIDCompletions(c *cli.Context) {
	if c.NArg() != 0 {
		return
	}

	list, err := api.ETLList(defaultAPIParams)
	if err != nil {
		return
	}

	for _, l := range list {
		fmt.Print(l.ID)
	}
}

func etlExists(uuid string) (err error) {
	// TODO: Replace with a generic API for checking duplicate UUID
	list, err := api.ETLList(defaultAPIParams)
	if err != nil {
		return
	}
	for _, l := range list {
		if l.ID == uuid {
			return fmt.Errorf("ETL %q already exists", uuid)
		}
	}
	return
}

func etlInitSpecHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "SPEC_FILE")
	}
	spec, err := os.ReadFile(c.Args()[0])
	if err != nil {
		return err
	}

	var msg etl.InitSpecMsg
	if msg, err = etl.ValidateSpec(spec); err != nil {
		return err
	}

	// msg.ID is `metadata.name` from podSpec
	if err = etlExists(msg.ID); err != nil {
		return
	}

	id, err := api.ETLInitSpec(defaultAPIParams, spec)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "%s\n", id)
	return nil
}

func etlInitCodeHandler(c *cli.Context) (err error) {
	var msg etl.InitCodeMsg

	fromFile := parseStrFlag(c, fromFileFlag)
	if fromFile == "" {
		return fmt.Errorf("%s flag cannot be empty", fromFileFlag.Name)
	}

	msg.ID = parseStrFlag(c, etlUUID)
	if msg.ID != "" {
		if err = cos.ValidateID(msg.ID); err != nil {
			return
		}
		if err = etlExists(msg.ID); err != nil {
			return
		}
	}

	if msg.Code, err = os.ReadFile(fromFile); err != nil {
		return fmt.Errorf("failed to read file: %q, err: %v", fromFile, err)
	}

	depsFile := parseStrFlag(c, depsFileFlag)
	if depsFile != "" {
		if msg.Deps, err = os.ReadFile(depsFile); err != nil {
			return fmt.Errorf("failed to read file: %q, err: %v", depsFile, err)
		}
	}

	msg.Runtime = parseStrFlag(c, runtimeFlag)
	msg.CommType = parseStrFlag(c, commTypeFlag)
	if msg.CommType != "" {
		// Missing `/` at the end, eg. `hpush:/` (should be `hpush://`)
		if strings.HasSuffix(msg.CommType, ":/") {
			msg.CommType += "/"
		}
		// Missing `://` at the end, eg. `hpush` (should be `hpush://`)
		if !strings.HasSuffix(msg.CommType, "://") {
			msg.CommType += "://"
		}
	}
	msg.WaitTimeout = cos.Duration(parseDurationFlag(c, waitTimeoutFlag))

	if err := msg.Validate(); err != nil {
		return err
	}

	id, err := api.ETLInitCode(defaultAPIParams, msg)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "%s\n", id)
	return nil
}

func etlListHandler(c *cli.Context) (err error) {
	list, err := api.ETLList(defaultAPIParams)
	if err != nil {
		return err
	}
	return templates.DisplayOutput(list, c.App.Writer, templates.TransformListTmpl)
}

func etlLogsHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "ETL_ID")
	}

	var (
		id       = c.Args().Get(0)
		targetID = c.Args().Get(1) // optional
	)

	logs, err := api.ETLLogs(defaultAPIParams, id, targetID)
	if err != nil {
		return err
	}

	if targetID != "" {
		fmt.Fprintln(c.App.Writer, string(logs[0].Logs))
		return nil
	}

	for idx, log := range logs {
		if idx > 0 {
			fmt.Fprintln(c.App.Writer)
		}
		fmt.Fprintf(c.App.Writer, "%s:\n%s\n", log.TargetID, string(log.Logs))
	}

	return nil
}

func etlStopHandler(c *cli.Context) (err error) {
	var etls []string
	if flagIsSet(c, allETLStopFlag) {
		if c.NArg() != 0 {
			return fmt.Errorf("specify either --all flag or ETL IDs")
		}

		res, err := api.ETLList(defaultAPIParams)
		if err != nil {
			return err
		}
		for _, etlInfo := range res {
			etls = append(etls, etlInfo.ID)
		}
	} else {
		if c.NArg() == 0 {
			return fmt.Errorf("either specify --all flag or provide at least one ETL ID")
		}
		etls = c.Args()
	}

	for _, id := range etls {
		if err := api.ETLStop(defaultAPIParams, id); err != nil {
			if httpErr, ok := err.(*cmn.ErrHTTP); ok && httpErr.Status == http.StatusNotFound {
				color.New(color.FgYellow).Fprintf(c.App.Writer, "ETL %q not found", id)
				continue
			}
			return err
		}
		fmt.Fprintf(c.App.Writer, "ETL %q stopped successfully\n", id)
	}

	return nil
}

func etlObjectHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "ETL_ID")
	} else if c.NArg() == 1 {
		return missingArgumentsError(c, "BUCKET/OBJECT_NAME")
	} else if c.NArg() == 2 {
		return missingArgumentsError(c, "OUTPUT")
	}

	var (
		id         = c.Args()[0]
		uri        = c.Args()[1]
		outputDest = c.Args()[2]
	)

	bck, objName, err := parseBckObjectURI(c, uri)
	if err != nil {
		return err
	}

	var w io.Writer
	if outputDest == "-" {
		w = os.Stdout
	} else {
		f, err := os.Create(outputDest)
		if err != nil {
			return err
		}
		w = f
		defer f.Close()
	}

	return handleETLHTTPError(api.ETLObject(defaultAPIParams, id, bck, objName, w), id)
}

func etlBucketHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "ETL_ID")
	} else if c.NArg() == 1 {
		return missingArgumentsError(c, "BUCKET_FROM")
	} else if c.NArg() == 2 {
		return missingArgumentsError(c, "BUCKET_TO")
	}

	id := c.Args()[0]

	fromBck, err := parseBckURI(c, c.Args()[1])
	if err != nil {
		return err
	}
	toBck, err := parseBckURI(c, c.Args()[2])
	if err != nil {
		return err
	}

	if fromBck.Equal(toBck) {
		return fmt.Errorf("cannot ETL bucket %q onto itself", fromBck)
	}

	msg := &cmn.TransCpyBckMsg{
		ID: id,
		CopyBckMsg: cmn.CopyBckMsg{
			Prefix: parseStrFlag(c, cpBckPrefixFlag),
			DryRun: flagIsSet(c, cpBckDryRunFlag),
		},
	}

	if flagIsSet(c, etlExtFlag) {
		mapStr := parseStrFlag(c, etlExtFlag)
		extMap := make(cos.SimpleKVs, 1)
		if err = jsoniter.UnmarshalFromString(mapStr, &extMap); err != nil {
			return fmt.Errorf("couldn't parse ext flag: %s", err.Error())
		}
		msg.Ext = extMap
	}
	if flagIsSet(c, etlBucketRequestTimeout) {
		msg.RequestTimeout = cos.Duration(etlBucketRequestTimeout.Value)
	}

	xactID, err := api.ETLBucket(defaultAPIParams, fromBck, toBck, msg)
	if err := handleETLHTTPError(err, id); err != nil {
		return err
	}

	if !flagIsSet(c, waitFlag) {
		fmt.Fprintln(c.App.Writer, xactID)
		return nil
	}

	if _, err := api.WaitForXaction(defaultAPIParams, api.XactReqArgs{ID: xactID}); err != nil {
		return err
	}
	if !flagIsSet(c, cpBckDryRunFlag) {
		return nil
	}

	stat, err := api.GetXactionStatsByID(defaultAPIParams, xactID)
	if err != nil {
		return err
	}

	fmt.Fprintln(c.App.Writer, dryRunHeader+" "+dryRunExplanation)
	fmt.Fprintf(c.App.Writer, "%d objects (%s) would have been put into bucket %s", stat.ObjCount(), cos.B2S(stat.BytesCount(), 2), toBck.String())
	return nil
}

func handleETLHTTPError(err error, etlID string) error {
	if httpErr, ok := err.(*cmn.ErrHTTP); ok {
		// TODO: How to find out if it's transformation not found, and not object not found?
		if httpErr.Status == http.StatusNotFound && strings.Contains(httpErr.Error(), etlID) {
			return fmt.Errorf("ETL %q not found; try starting new ETL with:\nais %s %s <spec>", etlID, commandETL, subcmdInit)
		}
	}
	return err
}
