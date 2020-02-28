// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that update properties/configuration of entities in the cluster.
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
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{propCompletions}, false /* multiple */, false /* separator */),
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
	bck, objName := parseBckObjectURI(c.Args().First())
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
		helpMsg := fmt.Sprintf("To list bucket properties, run: %s %s %s BUCKET_NAME", cliName, commandList, subcmdListBckProps)
		return newAdditionalInfoError(err, helpMsg)
	}
	return
}
