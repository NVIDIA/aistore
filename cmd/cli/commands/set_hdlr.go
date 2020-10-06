// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides commands that show and update bucket properties and configuration.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	setCmdsFlags = map[string][]cli.Flag{
		subcmdSetConfig: {},
		subcmdSetProps: {
			resetFlag,
		},
		subcmdSetPrimary: {},
	}

	setCmds = []cli.Command{
		{
			Name:  commandSet,
			Usage: "set property or configuration values",
			Subcommands: []cli.Command{
				{
					Name:         subcmdSetConfig,
					Usage:        "update configuration of a single node or the cluster",
					ArgsUsage:    setConfigArgument,
					Flags:        setCmdsFlags[subcmdSetConfig],
					Action:       setConfigHandler,
					BashComplete: setConfigCompletions,
				},
				{
					Name:         subcmdSetProps,
					Usage:        "update/reset bucket properties",
					ArgsUsage:    bucketPropsArgument,
					Flags:        setCmdsFlags[subcmdSetProps],
					Action:       setPropsHandler,
					BashComplete: bucketCompletions(bckCompletionsOpts{additionalCompletions: []cli.BashCompleteFunc{propCompletions}}),
				},
				{
					Name:         subcmdSetPrimary,
					Usage:        "set new primary proxy",
					ArgsUsage:    daemonIDArgument,
					Flags:        setCmdsFlags[subcmdSetPrimary],
					Action:       setPrimaryHandler,
					BashComplete: daemonCompletions(completeProxies),
				},
			},
		},
	}
)

func setConfigHandler(c *cli.Context) (err error) {
	if _, err = fillMap(); err != nil {
		return
	}
	return setConfig(c)
}

func setPropsHandler(c *cli.Context) (err error) {
	var origProps *cmn.BucketProps
	bck, err := parseBckURI(c, c.Args().First())
	if err != nil {
		return
	}

	if bck, origProps, err = validateBucket(c, bck, "", false); err != nil {
		return
	}

	if flagIsSet(c, resetFlag) { // ignores all arguments, just resets bucket origProps
		return resetBucketProps(c, bck)
	}

	updateProps, err := parseBckPropsFromContext(c)
	if err != nil {
		return
	}

	newProps := origProps.Clone()
	newProps.Apply(updateProps)
	if newProps.Equal(origProps) { // Apply props and check for change
		displayPropsEqMsg(c, bck)
		return
	}

	if err = setBucketProps(c, bck, updateProps); err != nil {
		helpMsg := fmt.Sprintf("To show bucket properties, run \"%s %s %s BUCKET_NAME -v\"",
			cliName, commandShow, subcmdShowBckProps)
		return newAdditionalInfoError(err, helpMsg)
	}

	showDiff(c, origProps, newProps)
	return
}

func displayPropsEqMsg(c *cli.Context, bck cmn.Bck) {
	args := c.Args().Tail()
	if len(args) == 1 && !isJSON(args[0]) {
		fmt.Fprintf(c.App.Writer, "Bucket %q: property %q, nothing to do\n", bck, args[0])
		return
	}
	fmt.Fprintf(c.App.Writer, "Bucket %q already has the set props, nothing to do\n", bck)
}

func showDiff(c *cli.Context, origProps, newProps *cmn.BucketProps) {
	origKV, _ := bckPropList(origProps, true)
	newKV, _ := bckPropList(newProps, true)
	for idx, prop := range newKV {
		if origKV[idx].Value != prop.Value {
			fmt.Fprintf(c.App.Writer, "%q set to:%q (was:%q)\n", prop.Name, prop.Value, origKV[idx].Value)
		}
	}
}

func setPrimaryHandler(c *cli.Context) (err error) {
	daemonID := c.Args().First()
	if daemonID == "" {
		return missingArgumentsError(c, "proxy daemon ID")
	}
	primarySmap, err := fillMap()
	if err != nil {
		return
	}
	if _, ok := primarySmap.Pmap[daemonID]; !ok {
		return incorrectUsageMsg(c, "%s: is not a proxy", daemonID)
	}
	err = api.SetPrimaryProxy(defaultAPIParams, daemonID)
	if err == nil {
		fmt.Fprintf(c.App.Writer, "%s has been set as a new primary proxy\n", daemonID)
	}
	return err
}
