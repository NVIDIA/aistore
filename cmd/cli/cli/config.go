// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
)

var (
	configCmdsFlags = map[string][]cli.Flag{
		subcmdCluster: {
			transientFlag,
			jsonFlag, // to show
		},
		subcmdNode: {
			transientFlag,
			jsonFlag, // to show
		},
	}

	clicfgCmdFlags = map[string][]cli.Flag{
		subcmdCLIShow: {
			cliConfigPathFlag,
			jsonFlag,
		},
		subcmdCLISet: {},
	}
)

var (
	configCmd = cli.Command{
		Name:  commandConfig,
		Usage: "configure AIS cluster and individual nodes (in the cluster); configure CLI (tool)",
		Subcommands: []cli.Command{
			makeAlias(showCmdConfig, "", true, commandShow), // alias for `ais show`
			{
				Name:         subcmdCluster,
				Usage:        "configure AIS cluster",
				ArgsUsage:    keyValuePairsArgument,
				Flags:        configCmdsFlags[subcmdCluster],
				Action:       setCluConfigHandler,
				BashComplete: setCluConfigCompletions,
			},
			{
				Name:         subcmdNode,
				Usage:        "configure AIS node",
				ArgsUsage:    nodeConfigArgument,
				Flags:        configCmdsFlags[subcmdNode],
				Action:       setNodeConfigHandler,
				BashComplete: setNodeConfigCompletions,
			},
			{
				Name:         subcmdReset,
				Usage:        "reset (cluster | node | CLI) configuration to system defaults",
				ArgsUsage:    optionalDaemonIDArgument,
				Action:       resetConfigHandler,
				BashComplete: showConfigCompletions, // `cli  cluster  p[...]   t[...]`
			},

			// CLI config
			clicfgCmd,
		},
	}

	// cli
	clicfgCmd = cli.Command{
		Name:  subcmdCLI,
		Usage: "display and change AIS CLI configuration",
		Subcommands: []cli.Command{
			{
				Name:   subcmdCLIShow,
				Usage:  "display CLI configuration",
				Flags:  clicfgCmdFlags[subcmdCLIShow],
				Action: showCLIConfigHandler,
			},
			{
				Name:         subcmdCLISet,
				Usage:        "change CLI configuration",
				ArgsUsage:    keyValuePairsArgument,
				Flags:        clicfgCmdFlags[subcmdCLISet],
				Action:       setCLIConfigHandler,
				BashComplete: cliPropCompletions,
			},
			{
				Name:   subcmdCLIReset,
				Usage:  "reset CLI configurations to system defaults",
				Action: resetCLIConfigHandler,
			},
		},
	}
)

func setCluConfigHandler(c *cli.Context) error {
	var (
		nvs      cos.StrKVs
		config   cmn.Config
		propList = make([]string, 0, 48)
		args     = c.Args()
		kvs      = args.Tail()
	)
	err := cmn.IterFields(&config.ClusterConfig, func(tag string, _ cmn.IterField) (err error, b bool) {
		propList = append(propList, tag)
		return
	}, cmn.IterOpts{Allowed: apc.Cluster})
	debug.AssertNoErr(err)
	if cos.StringInSlice(args.First(), propList) || strings.Contains(args.First(), keyAndValueSeparator) {
		kvs = args
	}
	if len(kvs) == 0 {
		return showClusterConfigHandler(c)
	}
	if nvs, err = makePairs(kvs); err != nil {
		if strings.Contains(err.Error(), "key=value pair") {
			return showClusterConfigHandler(c)
		}
		return err
	}
	for k := range nvs {
		if !cos.StringInSlice(k, propList) {
			return fmt.Errorf("invalid property name %q", k)
		}
	}
	for k, v := range nvs {
		if k == feat.FeaturesPropName {
			featfl, err := parseFeatureFlags(v)
			if err != nil {
				return fmt.Errorf("invalid feature flag %q", v)
			}
			nvs[k] = featfl.Value()
		}
	}

	// assorted named fields that require (cluster | node) restart
	// for the change to take an effect
	if name := nvs.ContainsAnyMatch(cmn.ConfigRestartRequired); name != "" {
		warn := fmt.Sprintf("cluster restart required for the change '%s=%s' to take an effect.", name, nvs[name])
		actionWarn(c, warn)
	}
	if err := api.SetClusterConfig(apiBP, nvs, flagIsSet(c, transientFlag)); err != nil {
		return err
	}

	// show
	var listed = make(cos.StrKVs)
	for what := range nvs {
		section := strings.Split(what, ".")[0]
		if listed.Contains(section) {
			continue
		}
		listed[section] = ""
		if err := showClusterConfig(c, section); err != nil {
			fmt.Fprintln(c.App.ErrWriter, redErr(err))
		}
	}
	actionDone(c, "Cluster config updated")
	return nil
}

func setNodeConfigHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingKeyValueError(c)
	}
	var (
		config   cmn.Config
		nvs      cos.StrKVs
		daemonID     = argDaemonID(c)
		args         = c.Args()
		v        any = &config.ClusterConfig
		propList     = make([]string, 0, 48)
	)
	if cos.StringInSlice(cfgScopeLocal, args) {
		v = &config.LocalConfig
	}
	err := cmn.IterFields(v, func(tag string, _ cmn.IterField) (err error, b bool) {
		propList = append(propList, tag)
		return
	}, cmn.IterOpts{Allowed: apc.Cluster})
	debug.AssertNoErr(err)

	kvs := args.Tail()
	if cos.StringInSlice(args.First(), propList) || strings.Contains(args.First(), keyAndValueSeparator) {
		kvs = args
	}
	if len(kvs) == 0 || (len(kvs) == 1 && (kvs[0] == cfgScopeLocal || kvs[0] == cfgScopeInherited)) {
		return showNodeConfig(c)
	}
	if kvs[0] == cfgScopeLocal || kvs[0] == cfgScopeInherited {
		kvs = kvs[1:]
	}
	if kvs[0] == subcmdReset {
		return resetNodeConfigHandler(c)
	}

	if nvs, err = makePairs(kvs); err != nil {
		if strings.Contains(err.Error(), "key=value pair") {
			return showNodeConfig(c)
		}
		return err
	}
	for k := range nvs {
		if !cos.StringInSlice(k, propList) {
			return fmt.Errorf("invalid property name %q", k)
		}
	}

	// assorted named fields that'll require (cluster | node) restart
	// for the change to take an effect
	if name := nvs.ContainsAnyMatch(cmn.ConfigRestartRequired); name != "" {
		warn := fmt.Sprintf("node %q restart required for the change '%s=%s' to take an effect.", daemonID, name, nvs[name])
		actionWarn(c, warn)
	}

	if err := api.SetDaemonConfig(apiBP, daemonID, nvs, flagIsSet(c, transientFlag)); err != nil {
		return err
	}
	s, err := jsoniter.MarshalIndent(nvs, "", "    ")
	debug.AssertNoErr(err)
	fmt.Fprintf(c.App.Writer, "%s\n", string(s))
	fmt.Fprintf(c.App.Writer, "\nnode %q config updated\n", daemonID)
	return nil
}

func resetConfigHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return cli.ShowCommandHelp(c, subcmdReset)
	}
	switch c.Args().First() {
	case subcmdCLI:
		err = resetCLIConfigHandler(c)
		return
	case subcmdCluster:
		err = api.ResetClusterConfig(apiBP)
		if err == nil {
			actionDone(c, "Config globally reset: all nodes reverted to cluster-wide defaults")
		}
	default:
		err = resetNodeConfigHandler(c)
	}
	return
}

func resetNodeConfigHandler(c *cli.Context) (err error) {
	var (
		sname    = c.Args().First()
		daemonID = argDaemonID(c)
	)
	debug.Assert(cluster.N2ID(sname) == daemonID)
	if err := api.ResetDaemonConfig(apiBP, daemonID); err != nil {
		return err
	}
	actionDone(c, sname+": inherited config successfully reset to the current cluster-wide defaults")
	return nil
}

//
// cli config (default location: ~/.config/ais/cli/)
//

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
	return tmpls.Print(flat, c.App.Writer, tmpls.ConfigTmpl, nil, false)
}

func setCLIConfigHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingKeyValueError(c)
	}

	var nvs cos.StrKVs
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

func resetCLIConfigHandler(c *cli.Context) (err error) {
	if err = config.Reset(); err == nil {
		actionDone(c, "CLI config successfully reset to all defaults")
	}
	return
}
