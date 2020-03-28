// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides commands that show and update bucket properties and configuration.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/urfave/cli"
)

var (
	setCmdsFlags = map[string][]cli.Flag{
		subcmdSetConfig: {},
		subcmdSetProps: {
			jsonspecFlag,
			resetFlag,
		},
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
	if bck, err = validateBucket(c, bck, "", false); err != nil {
		return
	}

	if flagIsSet(c, resetFlag) { // ignores all arguments, just resets bucket props
		return resetBucketProps(c, bck)
	}
	if err = setBucketProps(c, bck); err != nil {
		helpMsg := fmt.Sprintf("To show bucket properties, run \"%s %s %s BUCKET_NAME -v\"", cliName, commandShow, subcmdShowBckProps)
		return newAdditionalInfoError(err, helpMsg)
	}
	return
}
