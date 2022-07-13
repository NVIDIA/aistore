// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/fatih/color"
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

	configCmd = cli.Command{
		Name:  commandConfig,
		Usage: "set local/global AIS cluster configurations",
		Subcommands: []cli.Command{
			makeAlias(showCmdConfig, "", true, commandShow), // alias for `ais show`
			{
				Name:         subcmdCluster,
				Usage:        "configure cluster",
				ArgsUsage:    keyValuePairsArgument,
				Flags:        configCmdsFlags[subcmdCluster],
				Action:       setCluConfigHandler,
				BashComplete: setCluConfigCompletions,
			},
			{
				Name:         subcmdNode,
				Usage:        "configure node in the cluster",
				ArgsUsage:    nodeConfigArgument,
				Flags:        configCmdsFlags[subcmdNode],
				Action:       setNodeConfigHandler,
				BashComplete: setConfigCompletions,
			},
			{
				Name:         subcmdReset,
				Usage:        "reset to cluster configuration on all nodes or a specific node",
				ArgsUsage:    optionalDaemonIDArgument,
				Action:       resetConfigHandler,
				BashComplete: daemonCompletions(completeAllDaemons),
			},
			settingsCmd,
		},
	}
)

func setCluConfigHandler(c *cli.Context) error {
	var (
		nvs      cos.SimpleKVs
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

	// assorted named fields that'll require (cluster | node) restart
	// for the change to take an effect
	if name := nvs.ContainsAnyMatch(cmn.ConfigRestartRequired); name != "" {
		cyan := color.New(color.FgHiCyan).SprintFunc()
		msg := fmt.Sprintf("Warning: restart required for the change '%s=%s' to take an effect\n",
			name, nvs[name])
		fmt.Fprintln(c.App.Writer, cyan(msg))
	}
	// TODO -- FIXME: transient doesn't work?
	if err := api.SetClusterConfig(defaultAPIParams, nvs, flagIsSet(c, transientFlag)); err != nil {
		return err
	}
	s, err := jsoniter.MarshalIndent(nvs, "", "    ")
	debug.AssertNoErr(err)
	fmt.Fprintf(c.App.Writer, "%s\n", string(s))
	fmt.Fprintln(c.App.Writer, "\ncluster config updated")
	return nil
}

func setNodeConfigHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingKeyValueError(c)
	}
	var (
		config   cmn.Config
		nvs      cos.SimpleKVs
		daemonID             = argDaemonID(c)
		args                 = c.Args()
		v        interface{} = &config.ClusterConfig
		propList             = make([]string, 0, 48)
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
		cyan := color.New(color.FgHiCyan).SprintFunc()
		msg := fmt.Sprintf("Warning: restart required for the change '%s=%s' to take an effect\n",
			name, nvs[name])
		fmt.Fprintln(c.App.Writer, cyan(msg))
	}

	if err := api.SetDaemonConfig(defaultAPIParams, daemonID, nvs, flagIsSet(c, transientFlag)); err != nil {
		return err
	}
	s, err := jsoniter.MarshalIndent(nvs, "", "    ")
	debug.AssertNoErr(err)
	fmt.Fprintf(c.App.Writer, "%s\n", string(s))
	fmt.Fprintf(c.App.Writer, "\nnode %q config updated\n", daemonID)
	return nil
}

func resetConfigHandler(c *cli.Context) (err error) {
	daemonID := argDaemonID(c)
	if daemonID == "" {
		if err := api.ResetClusterConfig(defaultAPIParams); err != nil {
			return err
		}

		fmt.Fprintf(c.App.Writer, "config successfully reset for all nodes\n")
		return nil
	}

	if err := api.ResetDaemonConfig(defaultAPIParams, daemonID); err != nil {
		return err
	}

	fmt.Fprintf(c.App.Writer, "config for node %q successfully reset\n", daemonID)
	return nil
}
