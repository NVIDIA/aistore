// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with configurations of AIS daemons
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	configGet = "get"
	configSet = "set"
)

var (
	errExpectedAtLeastOneKeyValuePair = errors.New("expected at least one key-value pair")

	configFlags = map[string][]cli.Flag{
		configGet: {jsonFlag},
		configSet: {},
	}

	configCmds = []cli.Command{
		{
			Name:  cmn.GetWhatConfig,
			Usage: "interact with daemon configs",
			Flags: []cli.Flag{},
			Subcommands: []cli.Command{
				{
					Name:         configGet,
					Usage:        "displays configuration of a daemon",
					UsageText:    fmt.Sprintf("%s %s %s [DAEMON_ID]", cliName, cmn.GetWhatConfig, configGet),
					Action:       configHandler,
					Flags:        configFlags[configGet],
					BashComplete: daemonList,
				},
				{
					Name:      configSet,
					Usage:     "updates configuration of a single node or the entire cluster",
					UsageText: fmt.Sprintf("%s %s %s [DAEMON_ID] key=value...", cliName, cmn.GetWhatConfig, cmn.ActSetConfig),
					Action:    configHandler,
					Flags:     configFlags[configSet],
					BashComplete: func(c *cli.Context) {
						daemonList(c)
						configPropList(c)
					},
				},
			},
		},
	}
)

func configHandler(c *cli.Context) error {
	if err := fillMap(ClusterURL); err != nil {
		return err
	}

	var (
		err error

		baseParams = cliAPIParams(ClusterURL)
		command    = c.Command.Name
	)

	switch command {
	case configGet:
		err = getConfig(c, baseParams)
	case configSet:
		err = setConfig(c, baseParams)
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}

	return err
}

// Displays the config of a daemon
func getConfig(c *cli.Context, baseParams *api.BaseParams) error {
	var (
		daemonID = c.Args().First()
		useJSON  = flagIsSet(c, jsonFlag)
	)

	newURL, err := daemonDirectURL(daemonID)
	if err != nil {
		return err
	}
	baseParams.URL = newURL
	body, err := api.GetDaemonConfig(baseParams)
	if err != nil {
		return err
	}

	return templates.DisplayOutput(body, templates.ConfigTmpl, useJSON)
}

// Sets config of specific daemon or cluster
func setConfig(c *cli.Context, baseParams *api.BaseParams) error {
	daemonID, nvs, err := extractArguments(c)
	if err != nil {
		return err
	}

	if daemonID == "" {
		if err := api.SetClusterConfig(baseParams, nvs); err != nil {
			return err
		}

		fmt.Println()
		return nil
	}

	daemonURL, err := daemonDirectURL(daemonID)
	if err != nil {
		return err
	}
	baseParams.URL = daemonURL
	if err := api.SetDaemonConfig(baseParams, nvs); err != nil {
		return err
	}

	fmt.Println()
	return nil
}

func extractArguments(c *cli.Context) (daemonID string, nvs cmn.SimpleKVs, err error) {
	if c.NArg() == 0 {
		return "", nil, errExpectedAtLeastOneKeyValuePair
	}

	args := c.Args()
	daemonID = args.First()
	kvs := args.Tail()

	// Case when DAEMON_ID is not provided by the user:
	// 1. Key-value pair separated with '=': `ais set log.level=5`
	// 2. Key-value pair separated with space: `ais set log.level 5`. In this case
	//		it is required that the number of arguments is even and property
	//      name contains a dot
	if strings.Contains(args.First(), keyAndValueSeparator) ||
		(len(args) > 0 && len(args)%2 == 0 && strings.Contains(args.First(), ".")) {
		daemonID = ""
		kvs = args
	}

	if len(kvs) == 0 {
		return "", nil, errExpectedAtLeastOneKeyValuePair
	}

	nvs, err = makePairs(kvs)
	if err != nil {
		return "", nil, err
	}

	return daemonID, nvs, nil
}
