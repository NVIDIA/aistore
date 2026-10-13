// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core/meta"

	"github.com/urfave/cli"
)

// note apc.ResetToken usage in examples
const configClusterUsage = "Configure AIS cluster.\n" +
	indent1 + "Examples:\n" +
	indent1 + "\t- 'ais config cluster --json'\t- show entire cluster config in JSON;\n" +
	indent1 + "\t- 'ais config cluster log'\t- show 'log' section;\n" +
	indent1 + "\t- 'ais config cluster log.level 4'\t- set log level to 4;\n" +
	indent1 + "\t- 'ais config cluster log.modules ec xs'\t- elevate verbosity for selected modules;\n" +
	indent1 + "\t- 'ais config cluster features S3-API-via-Root'\t- enable feature flag;\n" +
	indent1 + "\t- 'ais config cluster features none'\t- reset all feature flags;\n" +
	indent1 + "\t- 'ais config cluster log.modules none'\t- reset log modules;\n" +
	indent1 + "\t- 'ais config cluster log.level=4 log.modules=\"ec xs\"'\t- set level and modules in one command;\n" +
	indent1 + "\t- 'ais config cluster space.highwm=90 space.lowwm 80'\t- several values, mixed forms;\n" +
	indent1 + "\t- ais config cluster checksum='{\"type\":\"md5\",\"validate_warm_get\":true}'\t- JSON-formatted section.\n" +
	nvUsageRef

const configCLISetUsage = "Update CLI configuration, e.g.:\n" +
	indent1 + "\t- 'ais config cli set timeout.tcp_timeout=60s'\t- set TCP timeout;\n" +
	indent1 + "\t- 'ais config cli set timeout.tcp_timeout 60s timeout.http_timeout 5m'\t- several values;\n" +
	indent1 + "\t- 'ais config cli'\t- show CLI configuration.\n" +
	nvUsageRef

const configNodeUsage = "Configure AIS node.\n" +
	indent1 + "Each node distributed across the cluster has 'inherited' (from cluster config) and 'local' configuration.\n" +
	indent1 + "Distributed nodes can override inherited defaults with local values; use caution: local changes\n" +
	indent1 + "may cause inconsistent behavior across the cluster. Examples:\n" +
	indent1 + "\t- 'ais config node NODE local --json'\t- show node's local config in JSON;\n" +
	indent1 + "\t- 'ais config node NODE inherited log'\t- show 'log' section (inherited);\n" +
	indent1 + "\t- 'ais config node NODE local host_net --json'\t- show node's network config;\n" +
	indent1 + "\t- 'ais config node NODE log.level 4'\t- set node's log level;\n" +
	indent1 + "\t- 'ais config node NODE log.modules none'\t- reset log modules;\n" +
	indent1 + "\t- 'ais config node NODE disk.iostat_time_long=4s'\t- update disk timing (in re: \"disk utilization smoothing\");\n" +
	indent1 + "\t- 'ais config node NODE log.level=4 log.modules=ec,xs'\t- set node's level and modules in one command.\n" +
	nvUsageRef

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

const (
	confIntraRequestAuth   = "auth.intra_cluster.request_auth"
	confNodeJoinSecretPath = "auth.intra_cluster.node_join_secret_path"
)

var (
	configCmd = cli.Command{
		Name:  commandConfig,
		Usage: "Configure AIS cluster and individual nodes (in the cluster); configure CLI (tool)",
		Subcommands: []cli.Command{
			makeAlias(&showCmdConfig, &mkaliasOpts{newName: commandShow}),
			{
				Name:         cmdCluster,
				ArgsUsage:    nvPairsArgument,
				Flags:        sortFlags(configCmdsFlags[cmdCluster]),
				Action:       setCluConfigHandler,
				Usage:        configClusterUsage,
				BashComplete: setCluConfigCompletions,
			},
			{
				Name:         cmdNode,
				Usage:        configNodeUsage,
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
				Usage:        configCLISetUsage,
				ArgsUsage:    nvPairsArgument,
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

	first := strings.TrimSpace(args.First())
	if slices.Contains(propList, first) || nvEqual(first) >= 0 || (c.NArg() > 1 && isCfgSection(propList, first)) {
		kvs = args
	}
	if len(kvs) == 0 {
		return showClusterConfigHandler(c)
	}
	if len(kvs) == 1 && kvs[0] == confLogModules {
		kvs[0] = "log"
	}
	if nvs, err = makePairs(kvs, propList); err != nil {
		if nvErr, ok := err.(*errInvalidNVpair); ok && nvErr.isLookup(kvs) {
			if err = showClusterConfigHandler(c); err != nil {
				err = fmt.Errorf("%v%s", err, examplesCluSetCfg)
			}
		}
		return err
	}
	for k, v := range nvs {
		if slices.Contains(propList, k) || (nvJSON(v) && isCfgSection(propList, k)) {
			continue
		}
		return fmt.Errorf("invalid property name %q%s", k, examplesCluSetCfg)
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

	if err := setLogModules(nvs, nil); err != nil {
		return err
	}
	if err := setFeatureFlags(c, nvs); err != nil {
		return err
	}

	// assorted named fields that require (cluster | node) restart
	// for the change to take an effect
	if name := nvs.ContainsAnyMatch(cmn.ConfigRestartRequired[:]); name != "" {
		actionWarnf(c, "cluster restart required for the change '%s=%s' to take effect.\n", name, nvs[name])
	}
	if err := api.SetClusterConfig(apiBP, nvs, flagIsSet(c, transientFlag)); err != nil {
		return V(err)
	}

show:
	warnIntraClusterAuthPrestage(c, nvs)

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

func warnIntraClusterAuthPrestage(c *cli.Context, nvs cos.StrKVs) {
	const fmtwarn = "'%s' is accepted and persisted, but has no effect until all nodes are running v5.1 or later"
	var (
		requestAuth, nodeJoinSecretPath string
	)
	if v, ok := nvs[confIntraRequestAuth]; ok {
		if cos.IsParseBool(v) {
			requestAuth = confIntraRequestAuth
		}
	}
	if v, ok := nvs[confNodeJoinSecretPath]; ok {
		if v != "" {
			nodeJoinSecretPath = confNodeJoinSecretPath
		}
	}
	if requestAuth == "" && nodeJoinSecretPath == "" {
		return
	}

	if clusterSupportsAtLeast(c, cmn.MinVerIntraClusterAuth) {
		return
	}

	if requestAuth != "" {
		actionWarnf(c, fmtwarn+"; proxy mediation takes effect immediately", requestAuth)
	}
	if nodeJoinSecretPath != "" {
		actionWarnf(c, fmtwarn, nodeJoinSecretPath)
	}
}

// convert feature flags to their numeric representation; warn when switching to reverse-proxy or restart is required
func setFeatureFlags(c *cli.Context, nvs cos.StrKVs) error {
	v, ok := nvs[feat.PropName]
	if !ok {
		return nil
	}
	nf, err := parseFeatureFlags(v)
	if err != nil {
		return fmt.Errorf("invalid feature flag %q, err: %v", v, err)
	}
	config, err := api.GetClusterConfig(apiBP)
	if err != nil {
		return V(err)
	}
	cf := config.Features
	if nf.IsSet(feat.S3ReverseProxy) && !cf.IsSet(feat.S3ReverseProxy) {
		actionWarn(c, "reverse-proxy mode of operation _may_ (and likely will) degrade scalability and performance!\n")
	}
	if changed := (cf ^ nf) & feat.RestartRequired; changed != 0 {
		actionWarnf(c, "restart required for %v to take effect.\n", changed.Names())
	}
	nvs[feat.PropName] = nf.String() // FormatUint
	return nil
}

// combine log.modules with log level into a single log.level value (ref 836)
// - validate modules before any API call
// - level: log.level from the same command line, if present; otherwise current (cluster or node)
func setLogModules(nvs cos.StrKVs, node *meta.Snode) error {
	v, ok := nvs[confLogModules]
	if !ok {
		return nil
	}
	modules := []string{""} // empty or "none": reset
	if v != "" && v != apc.ResetToken {
		if mod := badLogModule(v); mod != "" {
			return fmt.Errorf("invalid %s value %q (supported: %s; use %s=%s to reset)",
				confLogModules, mod, strings.Join(cos.Mods[:], ", "), confLogModules, apc.ResetToken)
		}
		modules = splitList(v)
	}

	var level int
	if lv, ok := nvs[confLogLevel]; ok {
		if _, err := strconv.Atoi(lv); err != nil {
			return fmt.Errorf("invalid %s value %q: %v", confLogLevel, lv, err)
		}
		if err := cos.LogLevel(lv).Validate(); err != nil {
			return err
		}
		level, _ = cos.LogLevel(lv).Parse()
	} else {
		current, err := currentLogLevel(node)
		if err != nil {
			return err
		}
		level, _ = current.Parse()
	}

	var ll cos.LogLevel
	ll.Set(level, modules)
	nvs[confLogLevel] = string(ll)
	delete(nvs, confLogModules)
	return nil
}

// return current cluster (node == nil) or node log level
func currentLogLevel(node *meta.Snode) (cos.LogLevel, error) {
	if node == nil {
		config, err := api.GetClusterConfig(apiBP)
		if err != nil {
			return "", V(err)
		}
		return config.Log.Level, nil
	}
	config, err := api.GetDaemonConfig(apiBP, node)
	if err != nil {
		return "", V(err)
	}
	return config.Log.Level, nil
}

// e.g.: ais config cluster checksum='{"type":"md5"}'
// - reject mixed JSON-formatted and plain values
func isFmtJSON(nvs cos.StrKVs) (val string, ans bool, err error) {
	var plain bool
	for _, v := range nvs {
		if nvJSON(v) {
			val, ans = v, true
		} else {
			plain = true
		}
	}
	if ans && plain {
		err = fmt.Errorf("cannot have both JSON-formatted and plain name=value pairs (%+v) in one command line", nvs)
	}
	return val, ans, err
}

// set JSON-formatted cluster config sections in a single request
func setcfg(c *cli.Context, nvs cos.StrKVs) error {
	toUpdate, err := cfgToSet(nvs)
	if err != nil {
		return err
	}
	// inspect supplied leaves, including those nested inside a JSON section
	err = cmn.IterFields(toUpdate, func(tag string, field cmn.IterField) (error, bool) {
		value := reflect.ValueOf(field.Value())
		if !value.IsValid() || value.IsZero() {
			return nil, false
		}
		for _, name := range cmn.ConfigRestartRequired {
			if tag == name || strings.HasPrefix(tag, name+".") {
				actionWarnf(c, "cluster restart required for changes to %q to take effect.\n", tag)
				return nil, true
			}
		}
		return nil, false
	})
	debug.AssertNoErr(err)
	return V(api.SetClusterConfigUsingMsg(apiBP, toUpdate, flagIsSet(c, transientFlag)))
}

// convert JSON-formatted values to cmn.ConfigToSet
// - name is a path: place each value at that path (e.g., net.http => {"net":{"http":VALUE}})
// - merge values that share a section; more specific paths override enclosing sections
func cfgToSet(nvs cos.StrKVs) (*cmn.ConfigToSet, error) {
	var (
		toUpdate = &cmn.ConfigToSet{}
		sections = cfgSections()
	)
	for _, k := range slices.Sorted(maps.Keys(nvs)) {
		path := strings.Split(k, ".")
		if !slices.Contains(sections, path[0]) {
			return nil, fmt.Errorf("cannot update config using JSON-formatted %q - "+NIY, k)
		}
		val := nvs[k]
		for i := len(path) - 1; i >= 0; i-- {
			val = `{"` + path[i] + `":` + val + `}`
		}
		if err := cos.JSON.Unmarshal([]byte(val), toUpdate); err != nil {
			return nil, fmt.Errorf("invalid JSON-formatted %q: %v", k, err)
		}
	}
	return toUpdate, nil
}

// return true if name is a config section or sub-section (a prefix of property names)
func isCfgSection(propList []string, name string) bool {
	prefix := name + "."
	for _, prop := range propList {
		if strings.HasPrefix(prop, prefix) {
			return true
		}
	}
	return false
}

// return top-level cmn.ConfigToSet JSON tags
func cfgSections() []string {
	t := reflect.TypeFor[cmn.ConfigToSet]()
	sections := make([]string, 0, t.NumField())
	for f := range t.Fields() {
		tag, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if tag != "" && tag != "-" {
			sections = append(sections, tag)
		}
	}
	return sections
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
		return missingNVError(c)
	}
	node, sname, err := getNode(c, args.First())
	if err != nil {
		return err
	}
	if slices.Contains(args, cfgScopeLocal) {
		v = &config.LocalConfig
	}
	err = cmn.IterFields(v, func(tag string, _ cmn.IterField) (err error, b bool) {
		propList = append(propList, tag)
		return
	}, cmn.IterOpts{Allowed: apc.Cluster})
	debug.AssertNoErr(err)

	kvs := args.Tail()
	if slices.Contains(propList, strings.TrimSpace(args.First())) || nvEqual(args.First()) >= 0 {
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

	if nvs, err = makePairs(kvs, append(slices.Clip(propList), confLogModules)); err != nil {
		if nvErr, ok := err.(*errInvalidNVpair); ok && nvErr.isLookup(kvs) {
			if err = showNodeConfig(c); err != nil {
				err = fmt.Errorf("%v%s", err, examplesNodeSetCfg)
			}
		}
		return err
	}
	if args.Get(1) == cfgScopeLocal {
		return errors.New(localNodeCfgErr)
	}
	if err := setLogModules(nvs, node); err != nil {
		return err
	}
	for k := range nvs {
		if k == feat.PropName {
			return fmt.Errorf("feature flags have cluster scope - cannot change for %s\n(tip: use 'ais config cluster features')", sname)
		}
	}
	for k := range nvs {
		if !slices.Contains(propList, k) {
			return fmt.Errorf("invalid property name %q%s", k, examplesNodeSetCfg)
		}
	}

	// assorted named fields that'll require (cluster | node) restart
	// for the change to take an effect
	if name := nvs.ContainsAnyMatch(cmn.ConfigRestartRequired[:]); name != "" {
		actionWarnf(c, "for the change '%s=%s' to take an effect node %q must be restarted.",
			name, nvs[name], sname)
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
		out, errV := jsonMarshalIndent(gcfg)
		if errV != nil {
			return errV
		}
		fmt.Fprintln(c.App.Writer, string(out))
		return
	}

	flat := flattenJSON(gcfg, c.Args().Get(0))
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
		return missingNVError(c)
	}

	var nvs cos.StrKVs
	if nvs, err = makePairs(c.Args(), nil); err != nil {
		if nvErr, ok := err.(*errInvalidNVpair); ok && nvErr.isLookup(c.Args()) {
			return showCfgCLI(c)
		}
		return err
	}

	flatOld := flattenJSON(gcfg, "")
	for k, v := range nvs {
		if err := cmn.UpdateFieldValue(gcfg, k, v); err != nil {
			return err
		}
	}

	flatNew := flattenJSON(gcfg, "")
	diff := diffConfigs(flatNew, flatOld)
	for _, val := range diff {
		if val.Old == "-" {
			continue
		}
		fmt.Fprintf(c.App.Writer, "%q set to: %q (was: %q)\n", val.Name, val.Current, val.Old)
	}

	return config.Save(gcfg)
}

func resetCfgCLI(c *cli.Context) (err error) {
	if err = config.Reset(); err == nil {
		actionDone(c, "CLI config successfully reset to all defaults")
	}
	return
}
