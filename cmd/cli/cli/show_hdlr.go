// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains implementation of the top-level `show` command.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
)

var (
	showCmdsFlags = map[string][]cli.Flag{
		cmdObject: {
			objPropsFlag, // --props [list]
			allPropsFlag,
			objNotCachedPropsFlag,
			noHeaderFlag,
			unitsFlag,
			silentFlag,
		},
		cmdCluster: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
			unitsFlag,
			verboseFlag,
		),
		cmdSmap: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
		),
		cmdBMD: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
		),
		cmdBucket: {
			jsonFlag,
			compactPropFlag,
			noHeaderFlag,
			addRemoteFlag,
		},
		cmdConfig: {
			jsonFlag,
			noHeaderFlag,
		},
		cmdShowRemoteAIS: {
			noHeaderFlag,
			verboseFlag,
			jsonFlag,
			unitsFlag,
		},
	}

	showCmdDashboard = cli.Command{
		Name:   cmdDashboard,
		Usage:  "Show cluster at-a-glance dashboard: node counts, capacity, performance, health, software version, and more",
		Flags:  sortFlags(showCmdsFlags[cmdCluster]),
		Action: clusterDashboardHandler,
	}

	showCmd = cli.Command{
		Name:  commandShow,
		Usage: "Show configuration, buckets, jobs, etc. - all managed entities in the cluster, and the cluster itself",
		Subcommands: []cli.Command{
			makeAlias(&authCmdShow, &mkaliasOpts{newName: commandAuth}),
			showCmdObject,
			showCmdBucket,
			showCmdCluster,
			showCmdDashboard,
			showCmdPerformance,
			showCmdStorage,
			showCmdRebalance,
			showCmdConfig,
			makeAlias(&showCmdRemoteCluster, &mkaliasOpts{newName: cmdShowRemoteAIS}),
			showCmdRemote,
			showCmdJob,
			showCmdLog,
			showTLS,
			makeAlias(&showCmdETL, &mkaliasOpts{newName: commandETL}),
		},
	}

	showCmdStorage = cli.Command{
		Name:      commandStorage,
		Usage:     "Show storage usage and utilization, disks and mountpaths",
		ArgsUsage: optionalTargetIDArgument,
		Flags:     sortFlags(storageFlags[commandStorage]),
		Action:    showStorageHandler,
		Subcommands: []cli.Command{
			showCmdDisk,
			showCmdMpath,
			showCmdMpathCapacity,
			showCmdStgSummary,
		},
	}
	showCmdObject = cli.Command{
		Name:         cmdObject,
		Usage:        "Show object properties",
		ArgsUsage:    objectArgument,
		Flags:        sortFlags(showCmdsFlags[cmdObject]),
		Action:       showObjectHandler,
		BashComplete: bucketCompletions(bcmplop{separator: true}),
	}
	showCmdCluster = cli.Command{
		Name:         cmdCluster,
		Usage:        "Show cluster: health, version and build, and nodes (including capacity and memory, load averages and alerts)",
		ArgsUsage:    showClusterArgument,
		Flags:        sortFlags(showCmdsFlags[cmdCluster]),
		Action:       showClusterHandler,
		BashComplete: showClusterCompletions, // NOTE: level 0 hardcoded
		Subcommands: []cli.Command{
			{
				Name:         cmdSmap,
				Usage:        "Show cluster map (Smap)",
				ArgsUsage:    optionalNodeIDArgument,
				Flags:        sortFlags(showCmdsFlags[cmdSmap]),
				Action:       showSmapHandler,
				BashComplete: suggestAllNodes,
			},
			{
				Name:         cmdBMD,
				Usage:        "Show bucket metadata (BMD)",
				ArgsUsage:    optionalNodeIDArgument,
				Flags:        sortFlags(showCmdsFlags[cmdBMD]),
				Action:       showBMDHandler,
				BashComplete: suggestAllNodes,
			},
			{
				Name:      cmdConfig,
				Usage:     "Show cluster and node configuration",
				ArgsUsage: showClusterConfigArgument,
				Flags:     sortFlags(showCmdsFlags[cmdConfig]),
				Action:    showClusterConfigHandler,
			},

			makeAlias(&showCmdPerformance, &mkaliasOpts{
				newName:  cmdShowStats,
				aliasFor: joinCommandWords(commandShow, commandPerf),
			}),
		},
	}
	showCmdBucket = cli.Command{
		Name:         cmdBucket,
		Usage:        "Show bucket properties",
		ArgsUsage:    bucketAndPropsArgument,
		Flags:        sortFlags(showCmdsFlags[cmdBucket]),
		Action:       showBckPropsHandler,
		BashComplete: bucketAndPropsCompletions, // bucketCompletions(),
	}
	showCmdConfig = cli.Command{
		Name:         cmdConfig,
		Usage:        "Show CLI, cluster, or node configurations (nodes inherit cluster and have local)",
		ArgsUsage:    showConfigArgument,
		Flags:        sortFlags(showCmdsFlags[cmdConfig]),
		Action:       showAnyConfigHandler,
		BashComplete: showConfigCompletions,
	}

	showCmdRemoteCluster = cli.Command{
		Name:      "cluster",
		Usage:     "Show attached remote AIS clusters",
		ArgsUsage: "",
		Flags:     sortFlags([]cli.Flag{noHeaderFlag}),
		Action:    showRemoteClustersTable,
	}

	showCmdRemote = cli.Command{
		Name:  "remote",
		Usage: "Show remote cluster information and statistics",
		Subcommands: []cli.Command{
			showCmdRemoteCluster,
			{
				Name:         "config",
				Usage:        "Show remote cluster configuration",
				ArgsUsage:    showRemoteConfigArgument,
				Flags:        sortFlags(showCmdsFlags[cmdConfig]),
				Action:       showRemoteConfigHandler,
				BashComplete: showRemoteConfigCompletions,
			},
			{
				Name:      "dashboard",
				Usage:     "Show remote cluster dashboard with performance and health metrics",
				ArgsUsage: "",
				Flags:     sortFlags([]cli.Flag{noHeaderFlag, unitsFlag}),
				Action:    showRemoteDashboardHandler,
			},
		},
	}
)

// part of "Usage:" (via 'ais show job --help')
func formatJobNames() string {
	const (
		maxPerLine = 6
	)
	var (
		sb    strings.Builder
		names = xact.ListDisplayNames(false)
	)
	for i, name := range names {
		if i > 0 && i%maxPerLine == 0 {
			sb.WriteString("\n\t")
		}
		sb.WriteString(name)
		if i != len(names)-1 {
			sb.WriteByte('\t')
		}
	}
	sb.WriteByte('\n')
	return sb.String()
}

func showClusterHandler(c *cli.Context) error {
	var (
		what, sid string
		daeType   string
	)
	if c.NArg() > 0 {
		what = c.Args().Get(0)
		if node, _, errV := getNode(c, what); errV == nil {
			sid, what = node.ID(), ""
			daeType = node.Type()
		}
	}
	if c.NArg() > 1 {
		arg := c.Args().Get(1)
		if sid != "" { // already set above, must be the last arg
			if err := errTailArgsContainFlag(c.Args()[1:]); err != nil {
				tip := reorderTailArgs("ais show cluster", c.Args()[1:], sid)
				return fmt.Errorf("%v (tip: try '%s')", err, tip)
			}
			return incorrectUsageMsg(c, "", arg)
		}
		node, _, err := getNode(c, arg)
		if err != nil {
			return err
		}
		sid, daeType = node.ID(), node.Type()
		if err := errTailArgsContainFlag(c.Args()[2:]); err != nil {
			tip := reorderTailArgs("ais show cluster", c.Args()[2:], daeType, sid)
			return fmt.Errorf("%v (tip: try '%s')", err, tip)
		}
	}

	setLongRunParams(c)

	smap, tstatusMap, pstatusMap, err := fillNodeStatusMap(c, daeType)
	if err != nil {
		return err
	}
	cluConfig, err := api.GetClusterConfig(apiBP)
	if err != nil {
		return V(err)
	}

	return cluDaeStatus(c, smap, tstatusMap, pstatusMap, cluConfig, cos.Left(sid, what), false)
}

func showObjectHandler(c *cli.Context) error {
	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in the form "+objectArgument)
	}
	fullObjName := c.Args().Get(0)
	bck, object, err := parseBckObjURI(c, fullObjName, false)
	if err != nil {
		return err
	}
	if _, err := headBucket(bck, true /* don't add */); err != nil {
		return err
	}
	_, err = showObjProps(c, bck, object, false /*silent*/)
	return err
}

func showBckPropsHandler(c *cli.Context) error {
	return showBucketProps(c)
}

func showSmapHandler(c *cli.Context) error {
	var (
		sid              string
		node, sname, err = arg0Node(c)
		smap             *meta.Smap
	)
	if err != nil {
		return err
	}

	setLongRunParams(c)

	if node != nil {
		var out any
		sid = node.ID()
		actionCptn(c, "Cluster map from:", sname)
		out, err = api.GetNodeMeta(apiBP, sid, apc.WhatSmap)
		if err == nil {
			smap = out.(*meta.Smap)
		}
	} else {
		smap, err = getClusterMap(c)
	}
	if err != nil {
		return err // cannot happen
	}
	return smapFromNode(c, smap, sid, flagIsSet(c, jsonFlag))
}

func showBMDHandler(c *cli.Context) error {
	var (
		bmd              *meta.BMD
		sid              string
		node, sname, err = arg0Node(c)
	)
	if err != nil {
		return err
	}

	setLongRunParams(c)

	if node != nil {
		var out any
		sid = node.ID()
		actionCptn(c, "BMD from:", sname)
		out, err = api.GetNodeMeta(apiBP, sid, apc.WhatBMD)
		if err == nil {
			bmd = out.(*meta.BMD)
		}
	} else {
		bmd, err = api.GetBMD(apiBP)
	}
	if err != nil {
		return V(err)
	}

	if bmd.IsEmpty() {
		msg := bmd.StringEx() + " - is empty"
		actionDone(c, msg)
		return nil
	}

	usejs := flagIsSet(c, jsonFlag)
	if usejs {
		return teb.Print(bmd, "", teb.Jopts(usejs))
	}

	tw := &tabwriter.Writer{}
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)
	if !flagIsSet(c, noHeaderFlag) {
		fmt.Fprintln(tw, "PROVIDER\tNAMESPACE\tNAME\tBACKEND\tCOPIES\tEC\tCREATED")
	}
	for provider, namespaces := range bmd.Providers {
		for nsUname, buckets := range namespaces {
			ns := cmn.ParseNsUname(nsUname)
			for bucket, props := range buckets {
				var copies, ec string
				if props.Mirror.Enabled {
					copies = strconv.Itoa(int(props.Mirror.Copies))
				}
				if props.EC.Enabled {
					if props.EC.ObjSizeLimit == cmn.ObjSizeToAlwaysReplicate {
						// no EC - always producing %d total replicas
						ec = fmt.Sprintf("%d-way replication", props.EC.ParitySlices+1)
					} else {
						ec = fmt.Sprintf("D=%d, P=%d (size limit %s)", props.EC.DataSlices,
							props.EC.ParitySlices, cos.ToSizeIEC(props.EC.ObjSizeLimit, 0))
					}
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					provider, ns, bucket, props.BackendBck.String(), copies, ec,
					cos.FormatNanoTime(props.Created, ""))
			}
		}
	}
	tw.Flush()
	fmt.Fprintln(c.App.Writer)
	fmt.Fprintf(c.App.Writer, "Version:\t%d\n", bmd.Version)
	fmt.Fprintf(c.App.Writer, "UUID:\t\t%s\n", bmd.UUID)
	return nil
}

func showClusterConfigHandler(c *cli.Context) error {
	return showClusterConfig(c, c.Args().Get(0))
}

func showRemoteConfigHandler(c *cli.Context) error {
	if c.NArg() < 1 {
		return incorrectUsageMsg(c, "missing arguments (hint: "+tabtab+")")
	}

	aliasOrUUID := c.Args().Get(0)
	section := c.Args().Get(1)

	clusters, bpMap, err := getRemoteClustersData()
	if err != nil {
		return err
	}

	var targetCluster *meta.RemAis
	for _, ra := range clusters {
		if ra.Alias == aliasOrUUID || ra.UUID == aliasOrUUID {
			targetCluster = ra
			break
		}
	}

	if targetCluster == nil {
		return fmt.Errorf("remote cluster %q not found (alias or UUID)", aliasOrUUID)
	}

	bp := bpMap[targetCluster.UUID]
	hint := fmt.Sprintf(configSectionNotFoundHint, fmt.Sprintf("ais show remote config %s --json", aliasOrUUID))
	return showConfigForBP(c, bp, section, hint)
}

func clusterDashboardHandler(c *cli.Context) error {
	var (
		smap       *meta.Smap
		tstatusMap teb.StstMap
		pstatusMap teb.StstMap
		what, sid  string
	)
	if c.NArg() > 0 {
		what = c.Args().Get(0)
		if node, _, errV := getNode(c, what); errV == nil {
			sid = node.ID()
		} else {
			sid = what
		}
	}

	// Check if longRun is requested
	setLongRunParams(c)

	smap, tstatusMap, pstatusMap, err := fillNodeStatusMap(c, apc.WhatNodeStatsAndStatus)
	if err != nil {
		return err
	}
	cluConfig, err := api.GetClusterConfig(apiBP)
	if err != nil {
		return err
	}

	// Use cluDaeStatus with rich analytics enabled
	return cluDaeStatus(c, smap, tstatusMap, pstatusMap, cluConfig, sid, true)
}

func showAnyConfigHandler(c *cli.Context) error {
	switch {
	case c.NArg() == 0:
		return incorrectUsageMsg(c, "missing arguments (hint: "+tabtab+")")
	case c.Args().Get(0) == cmdCLI:
		return showCfgCLI(c)
	case c.Args().Get(0) == cmdCluster:
		return showClusterConfig(c, c.Args().Get(1))
	default:
		return showNodeConfig(c)
	}
}

// TODO: prune config.ClusterConfig - hide deprecated "non_electable"
// shared printer for cluster configuration, local or remote (via BaseParams)
func showConfigForBP(c *cli.Context, bp api.BaseParams, section, notFoundHint string) error {
	var (
		usejs          = flagIsSet(c, jsonFlag)
		cluConfig, err = api.GetClusterConfig(bp)
	)
	if err != nil {
		return err
	}

	if usejs && section != "" {
		if printSectionJSON(c, cluConfig, section) {
			return nil
		}
		showSectionNotFoundError(c, section, cluConfig, notFoundHint)
		return nil
	}

	if usejs {
		return teb.Print(cluConfig, "", teb.Jopts(usejs))
	}

	var flat nvpairList
	if section != "backend" {
		flat = flattenJSON(cluConfig, section)
	} else {
		backends, err := api.GetConfiguredBackends(bp)
		if err != nil {
			return V(err)
		}
		flat = flattenBackends(backends)
	}

	// Check if section was found (for non-backend sections)
	if section != "" && section != "backend" && len(flat) == 0 {
		showSectionNotFoundError(c, section, cluConfig, notFoundHint)
		return nil
	}

	// compare w/ headBckTable using the same generic template for bucket props
	if flagIsSet(c, noHeaderFlag) {
		err = teb.Print(flat, teb.PropValTmplNoHdr)
	} else {
		err = teb.Print(flat, teb.PropValTmpl)
	}
	if err != nil {
		return err
	}
	if section == "" {
		msg := fmt.Sprintf("(Tip: use '[SECTION] %s' to show config section(s), see %s for details)",
			flprn(jsonFlag), qflprn(cli.HelpFlag))
		actionDone(c, msg)
		return nil
	}

	// feature flags: show all w/ descriptions
	if section == featureFlagsJname {
		err = printFeatVerbose(c, cluConfig.Features, false /*bucket scope*/)
	}

	return err
}

// (remote) reuses the shared printer above
func showClusterConfig(c *cli.Context, section string) error {
	hint := fmt.Sprintf(configSectionNotFoundHint, "ais config cluster --json")
	return showConfigForBP(c, apiBP, section, hint)
}

func showNodeConfig(c *cli.Context) error {
	var (
		section string
		scope   string
		usejs   = flagIsSet(c, jsonFlag)
	)
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	node, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	config, err := api.GetDaemonConfig(apiBP, node)
	if err != nil {
		return V(err)
	}

	data := struct {
		ClusterConfigDiff []propDiff
		LocalConfigPairs  nvpairList
	}{}
	for _, a := range c.Args().Tail() {
		if a == scopeAll || a == cfgScopeInherited || a == cfgScopeLocal {
			if scope != "" {
				return incorrectUsageMsg(c, "... %s %s ...", scope, a)
			}
			scope = a
		} else {
			if scope == "" {
				return incorrectUsageMsg(c, "... %v ...", c.Args().Tail())
			}
			if section != "" {
				return incorrectUsageMsg(c, "... %s %s ...", section, a)
			}
			section = a
			if i := strings.IndexByte(section, '='); i > 0 {
				section = section[:i] // when called to show set-config result (same as above)
			}
		}
	}

	if section == "backend" {
		// NOTE compare with showClusterConfig above (ref 080235)
		usejs = true
	}
	if usejs {
		opts := teb.Jopts(true)
		warn := "option " + qflprn(jsonFlag) + " won't show node <=> cluster configuration differences, if any."
		switch scope {
		case cfgScopeLocal:
			if section == "" {
				return teb.Print(&config.LocalConfig, "", opts)
			}
			if !printSectionJSON(c, &config.LocalConfig, section) {
				showSectionNotFoundError(c, section, &config.LocalConfig,
					"Try 'ais config node [NODE] local --json' to see all sections")
			}
			return nil
		case cfgScopeInherited:
			actionWarn(c, warn)
			if section == "" {
				return teb.Print(&config.ClusterConfig, "", opts)
			}
			if !printSectionJSON(c, &config.ClusterConfig, section) {
				showSectionNotFoundError(c, section, &config.ClusterConfig,
					"Try 'ais config node [NODE] inherited --json' to see all sections")
			}
			return nil
		default: // cfgScopeAll
			if section == "" {
				actionCptn(c, sname, "local config:")
				if err := teb.Print(&config.LocalConfig, "", opts); err != nil {
					return err
				}
				fmt.Fprintln(c.App.Writer)
				actionCptn(c, sname, "inherited config:")
				actionWarn(c, warn)
				return teb.Print(&config.ClusterConfig, "", opts)
			}
			// fall through on purpose
		}
	}

	usejs = false

	// fill-in `data`
	switch scope {
	case cfgScopeLocal:
		data.LocalConfigPairs = flattenJSON(config.LocalConfig, section)
	default: // cfgScopeInherited | cfgScopeAll
		cluConf, err := api.GetClusterConfig(apiBP)
		if err != nil {
			return V(err)
		}
		// diff cluster <=> this node
		flatNode := flattenJSON(config.ClusterConfig, section)
		flatCluster := flattenJSON(cluConf, section)
		data.ClusterConfigDiff = diffConfigs(flatNode, flatCluster)
		if scope == cfgScopeAll {
			data.LocalConfigPairs = flattenJSON(config.LocalConfig, section)
		}
	}
	// show "flat" diff-s
	if len(data.LocalConfigPairs) == 0 && len(data.ClusterConfigDiff) == 0 {
		return nil // No data to show
	}
	if flagIsSet(c, noHeaderFlag) {
		err = teb.Print(data, teb.DaemonConfigTmplNoHdr, teb.Jopts(usejs))
	} else {
		err = teb.Print(data, teb.DaemonConfigTmpl, teb.Jopts(usejs))
	}

	if err == nil && section == "" {
		msg := fmt.Sprintf("(Tip: to show specific section(s), use 'inherited [SECTION]' or 'all [SECTION]' with or without %s)",
			flprn(jsonFlag))
		actionDone(c, msg)
	}
	return err
}

// TODO -- FIXME: check backend.conf <new JSON formatted value>

// Remote cluster methods
// getRemoteClustersData fetches all remote clusters + creates base params for each.
func getRemoteClustersData() ([]*meta.RemAis, map[string]api.BaseParams, error) {
	all, err := api.GetRemoteAIS(apiBP)
	if err != nil {
		return nil, nil, V(err)
	}

	bpMap := make(map[string]api.BaseParams, len(all.A))
	for _, ra := range all.A {
		bpMap[ra.UUID] = createRemoteBaseParams(ra)
	}
	return all.A, bpMap, nil
}

func createRemoteBaseParams(ra *meta.RemAis) api.BaseParams {
	bp := api.BaseParams{
		URL:   ra.URL,
		Token: loggedUserToken,
		UA:    ua,
	}
	cargs := cmn.TransportArgs{
		DialTimeout: gcfg.Timeout.TCPTimeout,
		Timeout:     gcfg.Timeout.HTTPTimeout,
	}
	if cos.IsHTTPS(bp.URL) {
		sargs := cmn.TLSArgs{SkipVerify: true}
		bp.Client = cmn.NewClientTLS(cargs, sargs, false)
	} else {
		if clientH == nil {
			clientH = cmn.NewClient(cargs)
		}
		bp.Client = clientH
	}
	return bp
}

// ais show remote cluster
func showRemoteClustersTable(c *cli.Context) error {
	const (
		warnRemAisOffline = `remote ais cluster at %s is currently unreachable.
Run 'ais config cluster backend.conf --json' - to show the respective configuration;
    'ais config cluster backend.conf <new JSON formatted value>' - to reconfigure or remove.
For details and usage examples, see: docs/cli/config.md`
	)

	clusters, bpMap, err := getRemoteClustersData()
	if err != nil {
		return err
	}

	tw := &tabwriter.Writer{}
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)
	if !flagIsSet(c, noHeaderFlag) {
		fmt.Fprintln(tw, "UUID\tURL\tAlias\tPrimary\tSmap\tTargets\tUptime")
	}

	for _, ra := range clusters {
		uptime := teb.UnknownStatusVal
		bp := bpMap[ra.UUID]

		if clutime, _, err := api.HealthUptime(bp); err == nil {
			ns, _ := strconv.ParseInt(clutime, 10, 64)
			uptime = time.Duration(ns).String()
		}

		if ra.Smap != nil {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\tv%d\t%d\t%s\n",
				ra.UUID, ra.URL, ra.Alias, ra.Smap.Primary, ra.Smap.Version, ra.Smap.CountTargets(), uptime)
		} else {
			url := ra.URL
			if url != "" && url[0] == '[' && !strings.Contains(url, " ") {
				url = strings.Replace(url, "[", "", 1)
				url = strings.Replace(url, "]", "", 1)
			}
			fmt.Fprintf(tw, "<%s>\t%s\t%s\t%s\t%s\t%s\t%s\n", ra.UUID, url, ra.Alias,
				teb.UnknownStatusVal, teb.UnknownStatusVal, teb.UnknownStatusVal, uptime)

			warn := fmt.Sprintf(warnRemAisOffline, url)
			if len(clusters) == 1 {
				tw.Flush()
				fmt.Fprintln(c.App.Writer)
				return errors.New(warn)
			}
			actionWarn(c, warn)
		}
	}
	tw.Flush()
	return nil
}

// ais show remote dashboard
func showRemoteDashboardHandler(c *cli.Context) error {
	clusters, bpMap, err := getRemoteClustersData()
	if err != nil {
		return err
	}

	units, err := parseUnitsFlag(c, unitsFlag)
	if err != nil {
		return err
	}

	for _, ra := range clusters {
		if ra.Smap == nil {
			continue
		}
		bp := bpMap[ra.UUID]
		if dashStr, err := getRemoteClusterDashboard(c, bp, ra.Smap, units); err == nil {
			actionCptn(c, ra.Alias+"["+ra.UUID+"]", "dashboard:")
			fmt.Fprint(c.App.Writer, dashStr)
			fmt.Fprintln(c.App.Writer)
		} else {
			actionWarn(c, fmt.Sprintf("Failed to get dashboard for %s: %v", ra.Alias, err))
		}
	}
	return nil
}

func getRemoteClusterDashboard(c *cli.Context, bp api.BaseParams, smap *meta.Smap, units string) (string, error) {
	tstatusMap := make(teb.StstMap, smap.CountTargets())
	for _, tnode := range smap.Tmap {
		if ds, err := api.GetStatsAndStatus(bp, tnode); err == nil {
			tstatusMap[tnode.ID()] = ds
		}
	}

	if len(tstatusMap) == 0 {
		return "", errors.New("no target stats available")
	}

	// uses ais show dashboard's state, throughput, health metrics, etc.
	// we just pass in remais cluster status map
	return _fmtStorageSummary(c, tstatusMap, units), nil
}
