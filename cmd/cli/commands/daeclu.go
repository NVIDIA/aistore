// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles cluster and daemon operations.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"
)

type targetDiskStats struct {
	stats    map[string]*ios.SelectedDiskStats
	targetID string
}

var (
	proxy  = make(map[string]*stats.DaemonStatus)
	target = make(map[string]*stats.DaemonStatus)
)

// Displays smap of single daemon
func clusterSmap(c *cli.Context, primarySmap *cluster.Smap, daemonID string, useJSON bool) error {
	var (
		smap = primarySmap
		err  error
	)

	if daemonID != "" {
		smap, err = api.GetNodeClusterMap(defaultAPIParams, daemonID)
		if err != nil {
			return err
		}
	}

	extendedURLs := false
	for _, m := range []cluster.NodeMap{smap.Tmap, smap.Pmap} {
		for _, v := range m {
			if v.PublicNet != v.IntraControlNet || v.PublicNet != v.IntraDataNet {
				extendedURLs = true
			}
		}
	}

	body := templates.SmapTemplateHelper{
		Smap:         smap,
		ExtendedURLs: extendedURLs,
	}
	return templates.DisplayOutput(body, c.App.Writer, templates.SmapTmpl, useJSON)
}

// Displays the status of the cluster or daemon
func clusterDaemonStatus(c *cli.Context, smap *cluster.Smap, daemonID string, useJSON, hideHeader bool) error {
	if res, proxyOK := proxy[daemonID]; proxyOK {
		template := chooseTmpl(templates.ProxyInfoSingleBodyTmpl, templates.ProxyInfoSingleTmpl, hideHeader)
		return templates.DisplayOutput(res, c.App.Writer, template, useJSON)
	} else if res, targetOK := target[daemonID]; targetOK {
		template := chooseTmpl(templates.TargetInfoSingleBodyTmpl, templates.TargetInfoSingleTmpl, hideHeader)
		return templates.DisplayOutput(res, c.App.Writer, template, useJSON)
	} else if daemonID == cmn.Proxy {
		template := chooseTmpl(templates.ProxyInfoBodyTmpl, templates.ProxyInfoTmpl, hideHeader)
		return templates.DisplayOutput(proxy, c.App.Writer, template, useJSON)
	} else if daemonID == cmn.Target {
		template := chooseTmpl(templates.TargetInfoBodyTmpl, templates.TargetInfoTmpl, hideHeader)
		return templates.DisplayOutput(target, c.App.Writer, template, useJSON)
	} else if daemonID == "" {
		body := templates.StatusTemplateHelper{
			Smap:   smap,
			Status: proxy,
		}
		if err := templates.DisplayOutput(body, c.App.Writer, templates.AllProxyInfoTmpl, useJSON); err != nil {
			return err
		}
		fmt.Fprintf(c.App.Writer, "\n")
		if err := templates.DisplayOutput(target, c.App.Writer, templates.TargetInfoTmpl, useJSON); err != nil {
			return err
		}
		fmt.Fprintf(c.App.Writer, "\n")
		return templates.DisplayOutput(smap, c.App.Writer, templates.ClusterSummary, useJSON)
	}
	return fmt.Errorf(invalidDaemonMsg, daemonID)
}

// Removes existing node from the cluster.
func clusterRemoveNode(c *cli.Context, daemonID string) (err error) {
	if err := api.UnregisterNode(defaultAPIParams, daemonID); err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "Node with ID %q successfully removed from the cluster\n", daemonID)
	return nil
}

// Displays the disk stats of a target
func daemonDiskStats(c *cli.Context, daemonID string, useJSON, hideHeader bool) error {
	if _, ok := proxy[daemonID]; ok {
		return fmt.Errorf("daemon with ID %q is a proxy, but \"%s %s %s\" works only for targets", daemonID, cliName, commandShow, subcmdShowDisk)
	}
	if _, ok := target[daemonID]; daemonID != "" && !ok {
		return fmt.Errorf("target ID %q invalid - no such target", daemonID)
	}

	targets := map[string]*stats.DaemonStatus{daemonID: {}}
	if daemonID == "" {
		targets = target
	}

	diskStats, err := getDiskStats(targets)
	if err != nil {
		return err
	}

	template := chooseTmpl(templates.DiskStatBodyTmpl, templates.DiskStatsFullTmpl, hideHeader)
	err = templates.DisplayOutput(diskStats, c.App.Writer, template, useJSON)
	if err != nil {
		return err
	}

	return nil
}

func getDiskStats(targets map[string]*stats.DaemonStatus) ([]templates.DiskStatsTemplateHelper, error) {
	var (
		allStats = make([]templates.DiskStatsTemplateHelper, 0, len(targets))
		wg, _    = errgroup.WithContext(context.Background())
		statsCh  = make(chan targetDiskStats, len(targets))
	)

	for targetID := range targets {
		wg.Go(func(targetID string) func() error {
			return func() (err error) {
				diskStats, err := api.GetTargetDiskStats(defaultAPIParams, targetID)
				if err != nil {
					return err
				}

				statsCh <- targetDiskStats{stats: diskStats, targetID: targetID}
				return nil
			}
		}(targetID))
	}

	err := wg.Wait()
	close(statsCh)
	if err != nil {
		return nil, err
	}

	for diskStats := range statsCh {
		targetID := diskStats.targetID
		for diskName, diskStat := range diskStats.stats {
			allStats = append(allStats, templates.DiskStatsTemplateHelper{TargetID: targetID, DiskName: diskName, Stat: diskStat})
		}
	}

	return allStats, nil
}

// Displays the config of a daemon
func getDaemonConfig(c *cli.Context) error {
	var (
		daemonID = c.Args().Get(0)
		section  = c.Args().Get(1)
		useJSON  = flagIsSet(c, jsonFlag)
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, "daemon ID")
	}

	body, err := api.GetDaemonConfig(defaultAPIParams, daemonID)
	if err != nil {
		return err
	}

	template := templates.ConfigTmpl
	if section != "" {
		if t, ok := templates.ConfigSectionTmpl[section]; ok {
			template = strings.TrimPrefix(t, "\n")
		} else {
			return fmt.Errorf("config section %q not found", section)
		}
	}

	return templates.DisplayOutput(body, c.App.Writer, template, useJSON)
}

// Sets config of specific daemon or cluster
func setConfig(c *cli.Context) error {
	daemonID, nvs, err := daemonKeyValueArgs(c)
	if err != nil {
		return err
	}

	if daemonID == "" {
		if err := api.SetClusterConfig(defaultAPIParams, nvs); err != nil {
			return err
		}

		fmt.Fprintf(c.App.Writer, "config successfully updated\n")
		return nil
	}

	if err := api.SetDaemonConfig(defaultAPIParams, daemonID, nvs); err != nil {
		return err
	}

	fmt.Fprintf(c.App.Writer, "config for node %q successfully updated\n", daemonID)
	return nil
}

func daemonKeyValueArgs(c *cli.Context) (daemonID string, nvs cmn.SimpleKVs, err error) {
	if c.NArg() == 0 {
		return "", nil, missingArgumentsError(c, "attribute name-value pairs")
	}

	args := c.Args()
	daemonID = args.First()
	kvs := args.Tail()

	// Case when DAEMON_ID is not provided by the user:
	// 1. name-value pair separated with '=': `ais set log.level=5`
	// 2. name-value pair separated with space: `ais set log.level 5`. In this case
	//		the first word is looked up in cmn.ConfigPropList
	propList := cmn.ConfigPropList()
	if cmn.StringInSlice(args.First(), propList) || strings.Contains(args.First(), keyAndValueSeparator) {
		daemonID = ""
		kvs = args
	}

	if len(kvs) == 0 {
		return "", nil, missingArgumentsError(c, "attribute name-value pairs")
	}

	nvs, err = makePairs(kvs)
	if err != nil {
		return "", nil, err
	}

	for k := range nvs {
		if !cmn.StringInSlice(k, propList) {
			return "", nil, fmt.Errorf("invalid prop name %q(%+v)", k, nvs)
		}
	}

	return daemonID, nvs, nil
}

func showRebalance(c *cli.Context, keepMonitoring bool, refreshRate time.Duration) error {
	tw := &tabwriter.Writer{}
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)

	// run until rebalance is completed
	xactArgs := api.XactReqArgs{Kind: cmn.ActRebalance}
	for {
		rebStats, err := api.GetXactionStats(defaultAPIParams, xactArgs)
		if err != nil {
			switch err := err.(type) {
			case *cmn.HTTPError:
				if err.Status == http.StatusNotFound {
					fmt.Fprintln(c.App.Writer, "Rebalance has not started yet.")
					return nil
				}
				return err
			default:
				return err
			}
		}

		for daemonID, daemonStats := range rebStats {
			if len(daemonStats) == 0 {
				delete(rebStats, daemonID)
			}
		}

		sortedIDs := make([]string, 0, len(rebStats))
		for daemonID := range rebStats {
			sortedIDs = append(sortedIDs, daemonID)
		}
		sort.Strings(sortedIDs)

		fmt.Fprintln(tw, "DaemonID\tRebID\tObjRcv\tSizeRcv\tObjSent\tSizeSent\tStartTime\tEndTime\tAborted")
		fmt.Fprintln(tw, strings.Repeat("======\t", 9 /* num of columns */))
		for _, daemonID := range sortedIDs {
			st := rebStats[daemonID][0]
			extRebStats := &stats.ExtRebalanceStats{}
			if err := cmn.TryUnmarshal(st.Ext, &extRebStats); err != nil {
				continue
			}

			endTime := "<not completed>"
			if !st.EndTimeX.IsZero() {
				endTime = st.EndTimeX.Format("01-02 15:04:05")
			}
			startTime := st.StartTimeX.Format("01-02 15:04:05")

			fmt.Fprintf(tw,
				"%s\t%d\t%d\t%s\t%d\t%s\t%s\t%s\t%t\n",
				daemonID, extRebStats.RebID,
				extRebStats.RebTxCount, cmn.B2S(extRebStats.RebTxSize, 2),
				extRebStats.RebRxCount, cmn.B2S(extRebStats.RebRxSize, 2),
				startTime, endTime, st.AbortedX,
			)
		}
		tw.Flush()

		if rebStats.Finished() {
			fmt.Fprintln(c.App.Writer, "\nRebalance has been completed.")
			break
		}

		if !keepMonitoring {
			break
		}

		time.Sleep(refreshRate)
	}

	return nil
}
