// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
				BashComplete: setNodeConfigCompletions,
			},
			{
				Name:         subcmdReset,
				Usage:        "reset all nodes to cluster configuration (i.e., discard all local overrides)",
				ArgsUsage:    optionalDaemonIDArgument,
				Action:       resetCluConfigHandler,
				BashComplete: daemonCompletions(completeAllDaemons),
			},
			settingsCmd,
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
		if k == "features" {
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
	// TODO: check that transient works
	if err := api.SetClusterConfig(apiBP, nvs, flagIsSet(c, transientFlag)); err != nil {
		return err
	}
	if err := showClusterConfigHandler(c); err != nil {
		fmt.Fprintln(c.App.ErrWriter, redErr(err))
	} else {
		actionDone(c, "Cluster config updated")
	}
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

func resetCluConfigHandler(c *cli.Context) (err error) {
	if err := api.ResetClusterConfig(apiBP); err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "inherited config successfully reset for all nodes\n")
	return nil
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
	fmt.Fprintf(c.App.Writer, "%s: inherited config successfully reset to the current cluster-wide defaults\n", sname)
	return nil
}
