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
	"os"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/urfave/cli"
)

var (
	transformCmds = []cli.Command{
		{
			Name:  commandTransform,
			Usage: "use transformations",
			Subcommands: []cli.Command{
				{
					Name:      subcmdInit,
					Usage:     "initialize transformation with yaml spec",
					ArgsUsage: "SPEC_FILE",
					Action:    transformInitHandler,
				},
				{
					Name:   subcmdList,
					Usage:  "list all transformations",
					Action: transformListHandler,
				},
				{
					Name:      subcmdStop,
					Usage:     "stop transformation with given id",
					ArgsUsage: "TRANSFORM_ID",
					Action:    transformStopHandler,
				},
				{
					Name:      subcmdObject,
					Usage:     "get transformed object",
					ArgsUsage: "TRANSFORM_ID BUCKET_NAME/OBJECT_NAME OUTPUT",
					Action:    transformObjectHandler,
				},
			},
		},
	}
)

func transformInitHandler(c *cli.Context) (err error) {
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

func transformListHandler(c *cli.Context) (err error) {
	list, err := api.TransformList(defaultAPIParams)
	if err != nil {
		return err
	}
	return templates.DisplayOutput(list, c.App.Writer, templates.TransformListTmpl)
}

func transformStopHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "TRANSFORM_ID")
	}
	id := c.Args()[0]
	if err := api.TransformStop(defaultAPIParams, id); err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer, "Transformers stopped successfully.")
	return nil
}

func transformObjectHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "TRANSFORM_ID")
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
	return api.TransformObject(defaultAPIParams, id, bck, objName, w)
}
