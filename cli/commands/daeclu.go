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
		cmn.GetWhatSmap:         daecluBaseFlags,
		cmn.GetWhatDaemonStatus: append(append(daecluBaseFlags, longRunFlags...), noHeaderFlag),
		cmn.GetWhatStats:        append(daecluBaseFlags, longRunFlags...),
		cmn.GetWhatDiskStats:    append(append(daecluBaseFlags, longRunFlags...), noHeaderFlag),
	}

	// DaeCluCmds tracks available AIS API Information/Query Commands
	daeCluCmds = []cli.Command{
		{
			Name:         cmn.GetWhatSmap,
			Usage:        "displays cluster map",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatSmap],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatDaemonStatus,
			Usage:        "displays status of daemon",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatDaemonStatus],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatStats,
			Usage:        "displays stats of daemon",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatStats],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatDiskStats,
			Usage:        "displays disk stats of targets",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatDiskStats],
			BashComplete: targetList,
		},
	}
)

func updateLongRunVariables(c *cli.Context) {
	if flagIsSet(c, refreshFlag) {
		refreshRate = parseStrFlag(c, refreshFlag)
		// Run forever unless `count` is also specified
		count = Infinity
	}

	if flagIsSet(c, countFlag) {
		count = parseIntFlag(c, countFlag)
		if count <= 0 {
			fmt.Printf("Warning: '%s' set to %d, but expected value >= 1. Assuming '%s' = %d.\n",
				countFlag.Name, count, countFlag.Name, countDefault)
			count = countDefault
		}
	}
}

// Querying information
func queryHandler(c *cli.Context) (err error) {
	if err = fillMap(ClusterURL); err != nil {
		return errorHandler(err)
	}
	updateLongRunVariables(c)

	var (
		useJSON    = flagIsSet(c, jsonFlag)
		hideHeader = flagIsSet(c, noHeaderFlag)
		baseParams = cliAPIParams(ClusterURL)
		daemonID   = c.Args().First()
		req        = c.Command.Name
	)

	switch req {
	case cmn.GetWhatSmap:
		err = daecluSmap(baseParams, daemonID, useJSON)
	case cmn.GetWhatStats:
		err = daecluStats(baseParams, daemonID, useJSON)
	case cmn.GetWhatDiskStats:
		err = daecluDiskStats(baseParams, daemonID, useJSON, hideHeader)
	case cmn.GetWhatDaemonStatus:
		err = daecluStatus(daemonID, useJSON, hideHeader)
	default:
		return fmt.Errorf(invalidCmdMsg, req)
	}
	return errorHandler(err)
}

// Displays smap of single daemon
func daecluSmap(baseParams *api.BaseParams, daemonID string, useJSON bool) error {
	newURL, err := daemonDirectURL(daemonID)
	if err != nil {
		return err
	}

	baseParams.URL = newURL
	body, err := api.GetClusterMap(baseParams)
	if err != nil {
		return err
	}
	return templates.DisplayOutput(body, templates.SmapTmpl, useJSON)
}

// Displays the stats of a daemon
func daecluStats(baseParams *api.BaseParams, daemonID string, useJSON bool) error {
	if res, ok := proxy[daemonID]; ok {
		return templates.DisplayOutput(res, templates.ProxyStatsTmpl, useJSON)
	} else if res, ok := target[daemonID]; ok {
		return templates.DisplayOutput(res, templates.TargetStatsTmpl, useJSON)
	} else if daemonID == "" {
		body, err := api.GetClusterStats(baseParams)
		if err != nil {
			return err
		}
		return templates.DisplayOutput(body, templates.StatsTmpl, useJSON)
	}
	return fmt.Errorf(invalidDaemonMsg, daemonID)
}

// Displays the disk stats of a target
func daecluDiskStats(baseParams *api.BaseParams, daemonID string, useJSON, hideHeader bool) error {
	if _, ok := proxy[daemonID]; ok {
		return fmt.Errorf("daemon with provided ID (%s) is a proxy, but %s works only for targets", daemonID, cmn.GetWhatDiskStats)
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
	err = templates.DisplayOutput(diskStats, template, useJSON)
	if err != nil {
		return err
	}

	return nil
}

// Displays the status of the cluster or daemon
func daecluStatus(daemonID string, useJSON, hideHeader bool) (err error) {
	if res, proxyOK := proxy[daemonID]; proxyOK {
		template := chooseTmpl(templates.ProxyInfoSingleBodyTmpl, templates.ProxyInfoSingleTmpl, hideHeader)
		err = templates.DisplayOutput(res, template, useJSON)
	} else if res, targetOK := target[daemonID]; targetOK {
		template := chooseTmpl(templates.TargetInfoSingleBodyTmpl, templates.TargetInfoSingleTmpl, hideHeader)
		err = templates.DisplayOutput(res, template, useJSON)
	} else if daemonID == cmn.Proxy {
		template := chooseTmpl(templates.ProxyInfoBodyTmpl, templates.ProxyInfoTmpl, hideHeader)
		err = templates.DisplayOutput(proxy, template, useJSON)
	} else if daemonID == cmn.Target {
		template := chooseTmpl(templates.TargetInfoBodyTmpl, templates.TargetInfoTmpl, hideHeader)
		err = templates.DisplayOutput(target, template, useJSON)
	} else if daemonID == "" {
		if err := templates.DisplayOutput(proxy, templates.ProxyInfoTmpl, useJSON); err != nil {
			return err
		}
		err = templates.DisplayOutput(target, templates.TargetInfoTmpl, useJSON)
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
