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
					Name:   subcmdList,
					Usage:  "list all ETLs",
					Action: etlListHandler,
				},
				{
					Name:      subcmdStop,
					Usage:     "stop ETL with given id",
					ArgsUsage: "TRANSFORM_ID",
					Action:    etlStopHandler,
				},
				{
					Name:      subcmdObject,
					Usage:     "get transformed object",
					ArgsUsage: "ETL_ID BUCKET_NAME/OBJECT_NAME OUTPUT",
					Action:    etlObjectHandler,
				},
			},
		},
	}
)

func etlInitHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "SPEC_FILE")
	}
	specPath := c.Args()[0]
	f, err := os.Open(specPath)
	if err != nil {
		return err
	}
	spec, err := ioutil.ReadAll(f)
	f.Close()
	if err != nil {
		return err
	}

	id, err := api.TransformInit(defaultAPIParams, spec)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "%s\n", id)
	return nil
}

func etlListHandler(c *cli.Context) (err error) {
	list, err := api.TransformList(defaultAPIParams)
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
	if err := api.TransformStop(defaultAPIParams, id); err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer, "Transformers stopped successfully.")
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

	bck, objName, err := parseBckObjectURI(objName)
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

	err = api.TransformObject(defaultAPIParams, id, bck, objName, w)
	if httpErr, ok := err.(*cmn.HTTPError); ok {
		// TODO: How to find out if it's transformation not found, and not object not found?
		if httpErr.Status == http.StatusNotFound && strings.Contains(httpErr.Error(), id) {
			return fmt.Errorf("transformation %q not found; try starting new transformation with:\nais transform init spec", id)
		}
	}
	return err
}
