// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"sort"

	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
)

var (
	settingsCmdFlags = map[string][]cli.Flag{
		subcmdCLIShow: {
			cliConfigPathFlag,
			jsonFlag,
		},
		subcmdCLISet: {},
	}

	settingsCmd = cli.Command{
		Name:  subcmdCLI,
		Usage: "display and change AIS CLI configuration",
		Subcommands: []cli.Command{
			{
				Name:   subcmdCLIShow,
				Usage:  "display AIS CLI configuration",
				Flags:  settingsCmdFlags[subcmdCLIShow],
				Action: showCLIConfigHandler,
			},
			{
				Name:         subcmdCLISet,
				Usage:        "change AIS CLI option",
				ArgsUsage:    keyValuePairsArgument,
				Flags:        settingsCmdFlags[subcmdCLISet],
				Action:       setCLIConfigHandler,
				BashComplete: cliPropCompletions,
			},
		},
	}
)

func showCLIConfigHandler(c *cli.Context) (err error) {
	if flagIsSet(c, cliConfigPathFlag) {
		_, err := fmt.Fprintf(c.App.Writer, "%s\n", config.Path())
		return err
	}
	if flagIsSet(c, jsonFlag) {
		out, err := jsoniter.MarshalIndent(cfg, "", "    ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(c.App.Writer, string(out))
		return err
	}

	flat := flattenConfig(cfg, "")
	sort.Slice(flat, func(i, j int) bool {
		return flat[i].Name < flat[j].Name
	})
	return templates.DisplayOutput(flat, c.App.Writer, templates.ConfigTmpl, false)
}

func setCLIConfigHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "attribute name-value pairs")
	}

	var nvs cos.SimpleKVs
	if nvs, err = makePairs(c.Args()); err != nil {
		return err
	}

	flatOld := flattenConfig(cfg, "")
	for k, v := range nvs {
		if err := cmn.UpdateFieldValue(cfg, k, v); err != nil {
			return err
		}
	}

	flatNew := flattenConfig(cfg, "")
	diff := diffConfigs(flatNew, flatOld)
	for _, val := range diff {
		if val.Old == "-" {
			continue
		}
		fmt.Fprintf(c.App.Writer, "%q set to: %q (was: %q)\n", val.Name, val.Current, val.Old)
	}

	return config.Save(cfg)
}
