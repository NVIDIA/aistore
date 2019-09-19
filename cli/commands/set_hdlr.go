// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that update properties/configuration of entities in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import "github.com/urfave/cli"

var (
	setCmdsFlags = map[string][]cli.Flag{
		subcmdSetConfig: {},
		subcmdSetProps: {
			providerFlag,
			jsonspecFlag,
			resetFlag,
		},
	}

	setCmds = []cli.Command{
		{
			Name:  commandSet,
			Usage: "sets property or configuration values",
			Subcommands: []cli.Command{
				{
					Name:         subcmdSetConfig,
					Usage:        "updates configuration of a single node or the cluster",
					ArgsUsage:    setConfigArgument,
					Flags:        setCmdsFlags[subcmdSetConfig],
					Action:       setConfigHandler,
					BashComplete: setConfigCompletions,
				},
				{
					Name:         subcmdSetProps,
					Usage:        "updates/resets bucket properties",
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
	if _, err = fillMap(ClusterURL); err != nil {
		return
	}
	return setConfig(c, cliAPIParams(ClusterURL))
}

func setPropsHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
	)

	if flagIsSet(c, resetFlag) { // ignores any arguments, resets bucket props
		return resetBucketProps(c, baseParams)
	}
	return setBucketProps(c, baseParams)
}
