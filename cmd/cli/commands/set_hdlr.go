// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides commands that show and update bucket properties and configuration.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/urfave/cli"
)

var (
	setCmdsFlags = map[string][]cli.Flag{
		subcmdSetConfig: {},
		subcmdSetProps: {
			jsonspecFlag,
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
	bck, objName, err := parseBckObjectURI(c.Args().First())
	if err != nil {
		return
	}
	if objName != "" {
		return objectNameArgumentNotSupported(c, objName)
	}
	if bck, _, err = validateBucket(c, bck, "", false); err != nil {
		return
	}

	if flagIsSet(c, resetFlag) { // ignores all arguments, just resets bucket props
		return resetBucketProps(c, bck)
	}

	// TODO: handle new vs existing props (returned by validateBucket() above)

	if err = setBucketProps(c, bck); err != nil {
		helpMsg := fmt.Sprintf("To show bucket properties, run \"%s %s %s BUCKET_NAME -v\"",
			cliName, commandShow, subcmdShowBckProps)
		return newAdditionalInfoError(err, helpMsg)
	}
	return
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
