// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"context"
	"fmt"

	"github.com/NVIDIA/aistore/ios"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"
)

var (
	proxy  = make(map[string]*stats.DaemonStatus)
	target = make(map[string]*stats.DaemonStatus)

	longRunFlags    = []cli.Flag{refreshFlag, countFlag}
	daecluBaseFlags = []cli.Flag{jsonFlag}

	daecluFlags = map[string][]cli.Flag{
		daecluSmap:      daecluBaseFlags,
		daecluStatus:    append(append(daecluBaseFlags, longRunFlags...), noHeaderFlag),
		daecluStats:     append(daecluBaseFlags, longRunFlags...),
		daecluDiskStats: append(append(daecluBaseFlags, longRunFlags...), noHeaderFlag),
	}

	// DaeCluCmds tracks available AIS API Information/Query Commands
	daeCluCmds = []cli.Command{
		{
			Name:         daecluSmap,
			Usage:        "displays cluster map",
			ArgsUsage:    daemonIDArgumentText,
			Action:       queryHandler,
			Flags:        daecluFlags[daecluSmap],
			BashComplete: daemonList,
		},
		{
			Name:         daecluStatus,
			Usage:        "displays status of daemon",
			ArgsUsage:    daemonTypeArgumentText,
			Action:       queryHandler,
			Flags:        daecluFlags[daecluStatus],
			BashComplete: daemonList,
		},
		{
			Name:         daecluStats,
			Usage:        "displays stats of daemon",
			ArgsUsage:    daemonIDArgumentText,
			Action:       queryHandler,
			Flags:        daecluFlags[daecluStats],
			BashComplete: daemonList,
		},
		{
			Name:         daecluDiskStats,
			Usage:        "displays disk stats of targets",
			ArgsUsage:    targetIDArgumentText,
			Action:       queryHandler,
			Flags:        daecluFlags[daecluDiskStats],
			BashComplete: targetList,
		},
	}
)

// Querying information
func queryHandler(c *cli.Context) (err error) {
	if err := fillMap(ClusterURL); err != nil {
		return err
	}

	if err := updateLongRunParams(c); err != nil {
		return err
	}

	var (
		useJSON    = flagIsSet(c, jsonFlag)
		hideHeader = flagIsSet(c, noHeaderFlag)
		baseParams = cliAPIParams(ClusterURL)
		daemonID   = c.Args().First()
		req        = c.Command.Name
	)

	switch req {
	case daecluSmap:
		err = clusterSmap(c, baseParams, daemonID, useJSON)
	case daecluStats:
		err = daemonStats(c, baseParams, daemonID, useJSON)
	case daecluDiskStats:
		err = daemonDiskStats(c, baseParams, daemonID, useJSON, hideHeader)
	case daecluStatus:
		err = daemonStatus(c, daemonID, useJSON, hideHeader)
	default:
		return fmt.Errorf(invalidCmdMsg, req)
	}
	return err
}

// Displays smap of single daemon
func clusterSmap(c *cli.Context, baseParams *api.BaseParams, daemonID string, useJSON bool) error {
	newURL, err := daemonDirectURL(daemonID)
	if err != nil {
		return err
	}

	baseParams.URL = newURL
	body, err := api.GetClusterMap(baseParams)
	if err != nil {
		return err
	}
	return templates.DisplayOutput(body, c.App.Writer, templates.SmapTmpl, useJSON)
}

// Displays the stats of a daemon
func daemonStats(c *cli.Context, baseParams *api.BaseParams, daemonID string, useJSON bool) error {
	if res, ok := proxy[daemonID]; ok {
		return templates.DisplayOutput(res, c.App.Writer, templates.ProxyStatsTmpl, useJSON)
	} else if res, ok := target[daemonID]; ok {
		return templates.DisplayOutput(res, c.App.Writer, templates.TargetStatsTmpl, useJSON)
	} else if daemonID == "" {
		body, err := api.GetClusterStats(baseParams)
		if err != nil {
			return err
		}
		return templates.DisplayOutput(body, c.App.Writer, templates.StatsTmpl, useJSON)
	}
	return fmt.Errorf(invalidDaemonMsg, daemonID)
}

// Displays the disk stats of a target
func daemonDiskStats(c *cli.Context, baseParams *api.BaseParams, daemonID string, useJSON, hideHeader bool) error {
	if _, ok := proxy[daemonID]; ok {
		return fmt.Errorf("daemon with provided ID (%s) is a proxy, but %s works only for targets", daemonID, daecluDiskStats)
	}
	if _, ok := target[daemonID]; daemonID != "" && !ok {
		return fmt.Errorf("invalid target ID (%s) - no such target", daemonID)
	}

	targets := map[string]*stats.DaemonStatus{daemonID: {}}
	if daemonID == "" {
		targets = target
	}

	diskStats, err := getDiskStats(targets, baseParams)
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

// Displays the status of the cluster or daemon
func daemonStatus(c *cli.Context, daemonID string, useJSON, hideHeader bool) (err error) {
	if res, proxyOK := proxy[daemonID]; proxyOK {
		template := chooseTmpl(templates.ProxyInfoSingleBodyTmpl, templates.ProxyInfoSingleTmpl, hideHeader)
		err = templates.DisplayOutput(res, c.App.Writer, template, useJSON)
	} else if res, targetOK := target[daemonID]; targetOK {
		template := chooseTmpl(templates.TargetInfoSingleBodyTmpl, templates.TargetInfoSingleTmpl, hideHeader)
		err = templates.DisplayOutput(res, c.App.Writer, template, useJSON)
	} else if daemonID == cmn.Proxy {
		template := chooseTmpl(templates.ProxyInfoBodyTmpl, templates.ProxyInfoTmpl, hideHeader)
		err = templates.DisplayOutput(proxy, c.App.Writer, template, useJSON)
	} else if daemonID == cmn.Target {
		template := chooseTmpl(templates.TargetInfoBodyTmpl, templates.TargetInfoTmpl, hideHeader)
		err = templates.DisplayOutput(target, c.App.Writer, template, useJSON)
	} else if daemonID == "" {
		if err := templates.DisplayOutput(proxy, c.App.Writer, templates.ProxyInfoTmpl, useJSON); err != nil {
			return err
		}
		err = templates.DisplayOutput(target, c.App.Writer, templates.TargetInfoTmpl, useJSON)
	} else {
		return fmt.Errorf(invalidDaemonMsg, daemonID)
	}

	return err
}

type targetDiskStats struct {
	stats    map[string]*ios.SelectedDiskStats
	targetID string
}

func getDiskStats(targets map[string]*stats.DaemonStatus, baseParams *api.BaseParams) ([]templates.DiskStatsTemplateHelper, error) {
	var (
		allStats = make([]templates.DiskStatsTemplateHelper, 0, len(targets))
		wg, _    = errgroup.WithContext(context.Background())
		statsCh  = make(chan targetDiskStats, len(targets))
	)

	for targetID := range targets {
		wg.Go(func(targetID string) func() error {
			return func() (err error) {
				baseParams.URL, err = daemonDirectURL(targetID)
				if err != nil {
					return err
				}

				diskStats, err := api.GetTargetDiskStats(baseParams)
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
