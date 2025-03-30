// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"

	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
)

var (
	configCmdsFlags = map[string][]cli.Flag{
		cmdCluster: {
			transientFlag,
			jsonFlag, // to show
		},
		cmdNode: {
			transientFlag,
			jsonFlag, // to show
		},
	}

	clicfgCmdFlags = map[string][]cli.Flag{
		cmdCLIShow: {
			cliConfigPathFlag,
			jsonFlag,
		},
		cmdCLISet: {},
	}
)

const examplesCluSetCfg = `
Usage examples:
- ais config cluster checksum.type=xxhash
- ais config cluster checksum.type=md5 checksum.validate_warm_get=true
- ais config cluster checksum --json
For more usage examples, see ` + cmn.GitHubHome + `/blob/main/docs/cli/config.md
`

const examplesNodeSetCfg = `
Usage examples:
- ais config node [NODE_ID] inherited log.level=4
- ais config node [NODE_ID] inherited log
- ais config node [NODE_ID] inherited disk.disk_util_high_wm=93
For more usage examples, see ` + cmn.GitHubHome + `/blob/main/docs/cli/config.md
`

const localNodeCfgErr = `All nodes in a cluster inherit global (cluster) configuration,
with the possibility to locally override most of the inherited values.
In addition, each node has its own local config that only can be viewed but cannot be
updated via CLI. To update local config, lookup it's location, edit the file
(it's a plain text), and restart the node.
`

var (
	configCmd = cli.Command{
		Name:  commandConfig,
		Usage: "configure AIS cluster and individual nodes (in the cluster); configure CLI (tool)",
		Subcommands: []cli.Command{
			makeAlias(showCmdConfig, "", true, commandShow), // alias for `ais show`
			{
				Name:         cmdCluster,
				Usage:        "Configure AIS cluster",
				ArgsUsage:    keyValuePairsArgument,
				Flags:        sortFlags(configCmdsFlags[cmdCluster]),
				Action:       setCluConfigHandler,
				BashComplete: setCluConfigCompletions,
			},
			{
				Name:         cmdNode,
				Usage:        "Configure AIS node",
				ArgsUsage:    nodeConfigArgument,
				Flags:        sortFlags(configCmdsFlags[cmdNode]),
				Action:       setNodeConfigHandler,
				BashComplete: setNodeConfigCompletions,
			},
			{
				Name:         cmdReset,
				Usage:        "Reset (cluster | node | CLI) configuration to system defaults",
				ArgsUsage:    optionalNodeIDArgument,
				Action:       resetConfigHandler,
				BashComplete: showConfigCompletions, // `cli  cluster  p[...]   t[...]`
			},

			// CLI config
			clicfgCmd,
		},
	}

	// cli
	clicfgCmd = cli.Command{
		Name:   cmdCLI,
		Usage:  "Display and change AIS CLI configuration",
		Action: showCfgCLI,
		Flags:  sortFlags(clicfgCmdFlags[cmdCLIShow]),
		Subcommands: []cli.Command{
			{
				Name:   cmdCLIShow,
				Usage:  "Display CLI configuration",
				Flags:  sortFlags(clicfgCmdFlags[cmdCLIShow]),
				Action: showCfgCLI,
			},
			{
				Name:         cmdCLISet,
				Usage:        "Update CLI configuration",
				ArgsUsage:    keyValuePairsArgument,
				Flags:        sortFlags(clicfgCmdFlags[cmdCLISet]),
				Action:       setCfgCLI,
				BashComplete: cliPropCompletions,
			},
			{
				Name:   cmdCLIReset,
				Usage:  "Reset CLI configurations to system defaults",
				Action: resetCfgCLI,
			},
		},
	}
)

// TODO: prune config.ClusterConfig - hide deprecated "non_electable"
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
		if tag == confLogLevel {
			propList = append(propList, confLogModules) // insert to assign separately and combine below (ref 836)
		}
		return
	}, cmn.IterOpts{Allowed: apc.Cluster})
	debug.AssertNoErr(err)

	if cos.StringInSlice(args.First(), propList) || strings.Contains(args.First(), keyAndValueSeparator) {
		kvs = args
	}
	if len(kvs) == 0 {
		return showClusterConfigHandler(c)
	}
	if len(kvs) == 1 && kvs[0] == confLogModules {
		kvs[0] = "log"
	}
	if nvs, err = makePairs(kvs); err != nil {
		if _, ok := err.(*errInvalidNVpair); ok {
			if err = showClusterConfigHandler(c); err != nil {
				err = fmt.Errorf("%v%s", err, examplesCluSetCfg)
			}
		}
		return err
	}
	for k := range nvs {
		if !cos.StringInSlice(k, propList) {
			return fmt.Errorf("invalid property name %q%s", k, examplesCluSetCfg)
		}
	}

	_, useMsg, err := isFmtJSON(nvs)
	if err != nil {
		return err
	}
	if useMsg {
		if err := setcfg(c, nvs); err != nil { // api.SetClusterConfigUsingMsg (vs. api.SetClusterConfig below)
			return fmt.Errorf("%v%s", err, examplesCluSetCfg)
		}
		goto show
	}
	for k, v := range nvs {
		if k == feat.PropName {
			nf, _, err := parseFeatureFlags([]string{v}, 0)
			if err != nil {
				return fmt.Errorf("invalid feature flag %q, err: %v", v, err)
			}

			if cf := config.Features; nf != 0 {
				if nf.IsSet(feat.S3ReverseProxy) && !cf.IsSet(feat.S3ReverseProxy) {
					actionWarn(c, "reverse-proxy mode of operation _may_ (and likely will) degrade scalability and performance!\n")
				}
			}

			nvs[k] = nf.String() // FormatUint
		}
		if k == confLogModules { // (ref 836)
			if nvs[confLogLevel], err = parseLogModules(v); err != nil {
				return err
			}
			delete(nvs, confLogModules)
		}
	}

	// assorted named fields that require (cluster | node) restart
	// for the change to take an effect
	if name := nvs.ContainsAnyMatch(cmn.ConfigRestartRequired[:]); name != "" {
		warn := fmt.Sprintf("cluster restart required for the change '%s=%s' to take effect.", name, nvs[name])
		actionWarn(c, warn)
	}
	if err := api.SetClusterConfig(apiBP, nvs, flagIsSet(c, transientFlag)); err != nil {
		return V(err)
	}

show:
	var listed = make(cos.StrKVs)
	for what := range nvs {
		section := strings.Split(what, cmn.IterFieldNameSepa)[0]
		if listed.Contains(section) {
			continue
		}
		listed[section] = ""
		if err := showClusterConfig(c, section); err != nil {
			fmt.Fprintln(c.App.ErrWriter, redErr(err))
		}
	}
	actionDone(c, "\nCluster config updated")
	return nil
}

// an extra call to get the current (ref 836)
func parseLogModules(v string) (string, error) {
	config, err := api.GetClusterConfig(apiBP)
	if err != nil {
		return "", V(err)
	}
	level, _ := config.Log.Level.Parse()
	if v == "" || v == apc.NilValue {
		config.Log.Level.Set(level, []string{""})
	} else {
		config.Log.Level.Set(level, splitCsv(v))
	}
	return string(config.Log.Level), nil
}

// E.g.:
// $ ais config cluster checksum.type='{"type":"md5"}'
func isFmtJSON(nvs cos.StrKVs) (val string, ans bool, err error) {
	jsonRe := regexp.MustCompile(`^{.*}$`)
	for _, v := range nvs {
		if !jsonRe.MatchString(v) {
			if ans {
				err = fmt.Errorf("cannot have both json-formatted and plain key=value args (%+v) in one command line", nvs)
				return
			}
			continue
		}
		val, ans = v, true
	}
	return
}

// TODO: remove switch w/ assorted hardcoded sections - use reflection
func setcfg(c *cli.Context, nvs cos.StrKVs) error {
	toUpdate := &cmn.ConfigToSet{}
	for k, v := range nvs {
		switch {
		case k == "backend" || strings.HasPrefix(k, "backend."):
			jsoniter.Unmarshal([]byte(v), &toUpdate.Backend)
		case k == "mirror" || strings.HasPrefix(k, "mirror."):
			jsoniter.Unmarshal([]byte(v), &toUpdate.Mirror)
		case k == "ec" || strings.HasPrefix(k, "ec."):
			jsoniter.Unmarshal([]byte(v), &toUpdate.EC)
		case k == "log" || strings.HasPrefix(k, "log."):
			jsoniter.Unmarshal([]byte(v), &toUpdate.Log)
		case k == "checksum" || strings.HasPrefix(k, "checksum."):
			jsoniter.Unmarshal([]byte(v), &toUpdate.Cksum)
		default:
			return fmt.Errorf("cannot update config using JSON-formatted %q - "+NIY, k)
		}
		if err := api.SetClusterConfigUsingMsg(apiBP, toUpdate, flagIsSet(c, transientFlag)); err != nil {
			return V(err)
		}
	}
	return nil
}

//nolint:staticcheck // `localNodeCfgErr` with punctuation for usability
func setNodeConfigHandler(c *cli.Context) error {
	var (
		config   cmn.Config
		nvs      cos.StrKVs
		args         = c.Args()
		v        any = &config.ClusterConfig
		propList     = make([]string, 0, 48)
	)
	if c.NArg() == 0 {
		return missingKeyValueError(c)
	}
	node, sname, err := getNode(c, args.First())
	if err != nil {
		return err
	}
	if cos.StringInSlice(cfgScopeLocal, args) {
		v = &config.LocalConfig
	}
	err = cmn.IterFields(v, func(tag string, _ cmn.IterField) (err error, b bool) {
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
	if kvs[0] == cmdReset {
		return resetNodeConfigHandler(c)
	}

	if nvs, err = makePairs(kvs); err != nil {
		if _, ok := err.(*errInvalidNVpair); ok {
			if err = showNodeConfig(c); err != nil {
				err = fmt.Errorf("%v%s", err, examplesNodeSetCfg)
			}
		}
		return err
	}
	if args.Get(1) == cfgScopeLocal {
		return errors.New(localNodeCfgErr)
	}
	for k, v := range nvs { // (ref 836)
		if k == confLogModules {
			if nvs[confLogLevel], err = parseLogModules(v); err != nil {
				return err
			}
			delete(nvs, confLogModules)
			break
		}
	}
	for k := range nvs {
		if !cos.StringInSlice(k, propList) {
			return fmt.Errorf("invalid property name %q%s", k, examplesNodeSetCfg)
		}
	}

	// assorted named fields that'll require (cluster | node) restart
	// for the change to take an effect
	if name := nvs.ContainsAnyMatch(cmn.ConfigRestartRequired[:]); name != "" {
		warn := fmt.Sprintf("for the change '%s=%s' to take an effect node %q must be restarted.",
			name, nvs[name], sname)
		actionWarn(c, warn)
	}

	jsonval, useMsg, err := isFmtJSON(nvs)
	if err != nil {
		return err
	}
	if useMsg {
		// have api.SetClusterConfigUsingMsg but not "api.SetDaemonConfigUsingMsg"
		return fmt.Errorf("cannot update node configuration using JSON-formatted %q - "+NIY, jsonval)
	}
	if err := api.SetDaemonConfig(apiBP, node.ID(), nvs, flagIsSet(c, transientFlag)); err != nil {
		return V(err)
	}

	// show the update
	var res []byte
	if v, ok := nvs[confLogLevel]; ok { // (ref 836)
		nvs[confLogLevel] = cos.LogLevel(v).String()
	}
	res, err = jsonMarshalIndent(nvs)
	debug.AssertNoErr(err)
	fmt.Fprintf(c.App.Writer, "%s\n", string(res))
	fmt.Fprintf(c.App.Writer, "\nnode %s config updated\n", sname)
	return nil
}

func resetConfigHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return cli.ShowCommandHelp(c, cmdReset)
	}
	switch c.Args().Get(0) {
	case cmdCLI:
		err = resetCfgCLI(c)
		return
	case cmdCluster:
		err = api.ResetClusterConfig(apiBP)
		if err == nil {
			actionDone(c, "Config globally reset: all nodes reverted to cluster-wide defaults")
		}
	default:
		err = resetNodeConfigHandler(c)
	}
	return
}

func resetNodeConfigHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	node, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	if err := api.ResetDaemonConfig(apiBP, node.ID()); err != nil {
		return V(err)
	}
	actionDone(c, sname+": inherited config successfully reset to the current cluster-wide defaults")
	return nil
}

//
// cli config (default location: ~/.config/ais/cli/)
//

func showCfgCLI(c *cli.Context) (err error) {
	if flagIsSet(c, cliConfigPathFlag) {
		fmt.Fprintf(c.App.Writer, "%s\n", config.Path())
		return
	}
	if flagIsSet(c, jsonFlag) {
		out, errV := jsonMarshalIndent(cfg)
		if errV != nil {
			return errV
		}
		fmt.Fprintln(c.App.Writer, string(out))
		return
	}

	flat := flattenJSON(cfg, c.Args().Get(0))
	sort.Slice(flat, func(i, j int) bool {
		return flat[i].Name < flat[j].Name
	})
	if flagIsSet(c, noHeaderFlag) {
		return teb.Print(flat, teb.PropValTmplNoHdr)
	}
	return teb.Print(flat, teb.PropValTmpl)
}

func setCfgCLI(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingKeyValueError(c)
	}

	var nvs cos.StrKVs
	if nvs, err = makePairs(c.Args()); err != nil {
		if _, ok := err.(*errInvalidNVpair); ok {
			return showCfgCLI(c)
		}
		return err
	}

	flatOld := flattenJSON(cfg, "")
	for k, v := range nvs {
		if err := cmn.UpdateFieldValue(cfg, k, v); err != nil {
			return err
		}
	}

	flatNew := flattenJSON(cfg, "")
	diff := diffConfigs(flatNew, flatOld)
	for _, val := range diff {
		if val.Old == "-" {
			continue
		}
		fmt.Fprintf(c.App.Writer, "%q set to: %q (was: %q)\n", val.Name, val.Current, val.Old)
	}

	return config.Save(cfg)
}

func resetCfgCLI(c *cli.Context) (err error) {
	if err = config.Reset(); err == nil {
		actionDone(c, "CLI config successfully reset to all defaults")
	}
	return
}
