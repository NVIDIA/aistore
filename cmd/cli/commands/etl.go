// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/etl"
	"github.com/urfave/cli"
)

var (
	etlCmds = []cli.Command{
		{
			Name:  commandETL,
			Usage: "use ETLs",
			Subcommands: []cli.Command{
				{
					Name:      subcmdInit,
					Usage:     "initialize ETL with yaml spec",
					ArgsUsage: "SPEC_FILE",
					Action:    etlInitHandler,
				},
				{
					Name:  subcmdBuild,
					Usage: "build",
					Flags: []cli.Flag{
						fromFileFlag,
						depsFileFlag,
						runtimeFlag,
						waitTimeoutFlag,
					},
					Action: etlBuildHandler,
				},
				{
					Name:   subcmdList,
					Usage:  "list all ETLs",
					Action: etlListHandler,
				},
				{
					Name:      subcmdStop,
					Usage:     "stop ETL with given id",
					ArgsUsage: "ETL_ID",
					Action:    etlStopHandler,
				},
				{
					Name:      subcmdObject,
					Usage:     "transform object with given ETL",
					ArgsUsage: "ETL_ID BUCKET_NAME/OBJECT_NAME OUTPUT",
					Action:    etlObjectHandler,
				},
				{
					Name:      subcmdBucket,
					Usage:     "offline transform bucket with given ETL",
					ArgsUsage: "ETL_ID BUCKET_FROM BUCKET_TO",
					Action:    etlOfflineHandler,
					Flags: []cli.Flag{
						etlExtFlag,
						cpBckPrefixFlag,
						cpBckDryRunFlag,
					},
				},
			},
		},
	}
)

func etlInitHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "SPEC_FILE")
	}
	spec, err := ioutil.ReadFile(c.Args()[0])
	if err != nil {
		return err
	}

	id, err := api.ETLInit(defaultAPIParams, spec)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "%s\n", id)
	return nil
}

func etlBuildHandler(c *cli.Context) (err error) {
	var msg etl.BuildMsg

	fromFile := parseStrFlag(c, fromFileFlag)
	if fromFile == "" {
		return fmt.Errorf("%s flag cannot be empty", fromFileFlag.Name)
	}
	if msg.Code, err = ioutil.ReadFile(fromFile); err != nil {
		return fmt.Errorf("failed to read file: %q, err: %v", fromFile, err)
	}

	depsFile := parseStrFlag(c, depsFileFlag)
	if depsFile != "" {
		if msg.Deps, err = ioutil.ReadFile(depsFile); err != nil {
			return fmt.Errorf("failed to read file: %q, err: %v", depsFile, err)
		}
	}

	msg.Runtime = parseStrFlag(c, runtimeFlag)
	msg.WaitTimeout = cmn.DurationJSON(parseDurationFlag(c, waitTimeoutFlag))

	if err := msg.Validate(); err != nil {
		return err
	}

	id, err := api.ETLBuild(defaultAPIParams, msg)
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

func etlStopHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "ETL_ID")
	}
	id := c.Args()[0]
	if err := api.ETLStop(defaultAPIParams, id); err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer, "ETL containers stopped successfully.")
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
		objName    = c.Args()[1]
		outputDest = c.Args()[2]
	)

	bck, objName, err := cmn.ParseBckObjectURI(objName)
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

// TODO: add prefix, suffix and ext flags.
func etlOfflineHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "ETL_ID")
	} else if c.NArg() == 1 {
		return missingArgumentsError(c, "BUCKET_FROM")
	} else if c.NArg() == 2 {
		return missingArgumentsError(c, "BUCKET_TO")
	}

	var (
		id = c.Args()[0]
	)

	fromBck, toName, err := cmn.ParseBckObjectURI(c.Args()[1])
	if err != nil {
		return err
	}
	toBck, fromObjName, err := cmn.ParseBckObjectURI(c.Args()[2])
	if err != nil {
		return err
	}
	if fromBck.IsCloud() || toBck.IsCloud() {
		return fmt.Errorf("ETL of cloud buckets not supported")
	}
	if fromBck.IsRemoteAIS() || toBck.IsRemoteAIS() {
		return fmt.Errorf("ETL of remote ais buckets not supported")
	}
	if toName != "" {
		return objectNameArgumentNotSupported(c, toName)
	}
	if fromObjName != "" {
		return objectNameArgumentNotSupported(c, toName)
	}

	xactID, err := api.ETLBucket(defaultAPIParams, fromBck, toBck, &cmn.Bck2BckMsg{
		ID:     id,
		Ext:    parseStrFlag(c, etlExtFlag),
		Prefix: parseStrFlag(c, cpBckPrefixFlag),
		DryRun: flagIsSet(c, cpBckDryRunFlag),
	})

	if err := handleETLHTTPError(err, id); err != nil {
		return err
	}

	if !flagIsSet(c, cpBckDryRunFlag) {
		fmt.Fprintln(c.App.Writer, xactID)
		return nil
	}

	if _, err := api.WaitForXactionV2(defaultAPIParams, api.XactReqArgs{ID: xactID}); err != nil {
		return err
	}

	stat, err := api.GetXactionStatsByID(defaultAPIParams, xactID)
	if err != nil {
		return err
	}

	fmt.Fprintln(c.App.Writer, dryRunHeader+" "+dryRunExplanation)
	fmt.Fprintf(c.App.Writer, "%d objects (%s) would have been put into bucket %s", stat.ObjCount(), cmn.B2S(stat.BytesCount(), 2), toBck.String())
	return nil
}

func handleETLHTTPError(err error, etlID string) error {
	if httpErr, ok := err.(*cmn.HTTPError); ok {
		// TODO: How to find out if it's transformation not found, and not object not found?
		if httpErr.Status == http.StatusNotFound && strings.Contains(httpErr.Error(), etlID) {
			return fmt.Errorf("ETL %q not found; try starting new ETL with:\nais %s %s <spec>", etlID, commandETL, subcmdInit)
		}
	}
	return err
}
