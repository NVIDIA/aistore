// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"
)

var (
	curPrxStatus stats.DaemonStatusMap
	curTgtStatus stats.DaemonStatusMap
)

// NOTE: target's metric names & kinds
func getMetricNames(c *cli.Context) (cos.StrKVs, error) {
	smap, err := getClusterMap(c)
	if err != nil {
		return nil, err
	}
	if smap.CountActiveTs() == 0 {
		return nil, nil
	}
	tsi, err := smap.GetRandTarget()
	if err != nil {
		return nil, err
	}
	return api.GetMetricNames(apiBP, tsi)
}

//
// stats.DaemonStatusMap
//

func fillNodeStatusMap(c *cli.Context, daeType string) (*cluster.Smap, error) {
	smap, err := getClusterMap(c)
	if err != nil {
		return nil, err
	}

	var (
		wg         cos.WG
		mu         = &sync.Mutex{}
		pcnt, tcnt = smap.CountProxies(), smap.CountTargets()
	)
	switch daeType {
	case apc.Target:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), tcnt)
		curTgtStatus = make(stats.DaemonStatusMap, tcnt)
		daeStatus(smap.Tmap, curTgtStatus, wg, mu)
	case apc.Proxy:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt)
		curPrxStatus = make(stats.DaemonStatusMap, pcnt)
		daeStatus(smap.Pmap, curPrxStatus, wg, mu)
	default:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt+tcnt)
		curTgtStatus = make(stats.DaemonStatusMap, tcnt)
		curPrxStatus = make(stats.DaemonStatusMap, pcnt)
		daeStatus(smap.Pmap, curPrxStatus, wg, mu)
		daeStatus(smap.Tmap, curTgtStatus, wg, mu)
	}

	wg.Wait()
	return smap, nil
}

func daeStatus(nodeMap cluster.NodeMap, daeMap stats.DaemonStatusMap, wg cos.WG, mu *sync.Mutex) {
	for _, si := range nodeMap {
		wg.Add(1)
		go func(si *cluster.Snode) {
			_status(si, mu, daeMap)
			wg.Done()
		}(si)
	}
}

func _status(node *cluster.Snode, mu *sync.Mutex, daeMap stats.DaemonStatusMap) {
	daeStatus, err := api.GetDaemonStatus(apiBP, node)
	switch {
	case node.Flags.IsSet(cluster.NodeFlagMaint):
		daeStatus.Status = tmpls.MaintenanceSuffix
	case node.Flags.IsSet(cluster.NodeFlagDecomm):
		daeStatus.Status = tmpls.DecommissionSuffix
	case err != nil:
		if strings.HasPrefix(err.Error(), "errNodeNotFound") {
			daeStatus = &stats.DaemonStatus{Snode: node, Status: "[errNodeNotFound]"}
		} else {
			daeStatus = &stats.DaemonStatus{Snode: node, Status: "[" + err.Error() + "]"}
		}
	default:
		// keep daeStatus.Status (api.StatusOnline, ...) as is
	}
	mu.Lock()
	daeMap[node.ID()] = daeStatus
	mu.Unlock()
}

func getDiskStats(targets stats.DaemonStatusMap) ([]tmpls.DiskStatsTemplateHelper, error) {
	var (
		allStats = make([]tmpls.DiskStatsTemplateHelper, 0, len(targets))
		wg, _    = errgroup.WithContext(context.Background())
		statsCh  = make(chan targetDiskStats, len(targets))
	)

	for targetID := range targets {
		wg.Go(func(targetID string) func() error {
			return func() (err error) {
				diskStats, err := api.GetTargetDiskStats(apiBP, targetID)
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
			allStats = append(allStats,
				tmpls.DiskStatsTemplateHelper{TargetID: targetID, DiskName: diskName, Stat: diskStat})
		}
	}

	sort.Slice(allStats, func(i, j int) bool {
		if allStats[i].TargetID != allStats[j].TargetID {
			return allStats[i].TargetID < allStats[j].TargetID
		}
		if allStats[i].DiskName != allStats[j].DiskName {
			return allStats[i].DiskName < allStats[j].DiskName
		}
		return allStats[i].Stat.Util > allStats[j].Stat.Util
	})

	return allStats, nil
}

//
// throughput
//

func computeSleepRefreshBps(c *cli.Context) (sleep, averageOver time.Duration) {
	sleep = calcRefreshRate(c)
	averageOver = cos.MinDuration(cos.MaxDuration(sleep/2, 2*time.Second), 10*time.Second)
	// adjust not to over-sleep
	sleep = cos.MaxDuration(10*time.Millisecond, sleep-averageOver)
	return
}

// as F(stats.ClusterStats)
// (compare w/ computecCluBps2)
func computecCluBps1(c *cli.Context, statsBegin stats.ClusterStats, averageOver time.Duration) error {
	metrics, err := getMetricNames(c)
	if err != nil {
		return err
	}

	time.Sleep(averageOver)

	statsEnd, err := api.GetClusterStats(apiBP)
	if err != nil {
		return err
	}
	seconds := cos.MaxI64(int64(averageOver.Seconds()), 1)
	debug.Assert(seconds > 1)
	for tid, begin := range statsBegin.Target {
		end := statsEnd.Target[tid]
		for k, v := range begin.Tracker {
			vend := end.Tracker[k]
			// (unlike stats.KindComputedThroughput)
			if metrics[k] == stats.KindThroughput {
				if v.Value > 0 {
					throughput := (vend.Value - v.Value) / seconds
					v.Value = throughput
				}
			} else {
				v.Value = vend.Value // more timely
			}
			begin.Tracker[k] = v
		}
	}
	return nil
}

// throughput as F(stats.DaemonStats)
func computeDaeBps(c *cli.Context, node *cluster.Snode, statsBegin *stats.DaemonStats, averageOver time.Duration) error {
	metrics, err := getMetricNames(c)
	if err != nil {
		return err
	}

	time.Sleep(averageOver)

	statsEnd, err := api.GetDaemonStats(apiBP, node)
	if err != nil {
		return err
	}
	seconds := cos.MaxI64(int64(averageOver.Seconds()), 1)
	debug.Assert(seconds > 1)
	for k, v := range statsBegin.Tracker {
		vend := statsEnd.Tracker[k]
		if metrics[k] == stats.KindThroughput {
			if v.Value > 0 {
				throughput := (vend.Value - v.Value) / seconds
				v.Value = throughput
			}
		} else {
			v.Value = vend.Value // more recent
		}
		statsBegin.Tracker[k] = v
	}
	return nil
}

// as F(stats.DaemonStatusMap)
// (compare w/ computecCluBps1)
func computecCluBps2(c *cli.Context, averageOver time.Duration) (stats.DaemonStatusMap, error) {
	metrics, err := getMetricNames(c)
	if err != nil {
		return nil, err
	}

	if _, err := fillNodeStatusMap(c, apc.Target); err != nil {
		return nil, err
	}
	statusMapBegin := curTgtStatus
	time.Sleep(averageOver)
	if _, err := fillNodeStatusMap(c, apc.Target); err != nil {
		return nil, err
	}
	statusMapEnd := curTgtStatus

	seconds := cos.MaxI64(int64(averageOver.Seconds()), 1)
	debug.Assert(seconds > 1)
	for tid, begin := range statusMapBegin {
		end := statusMapEnd[tid]
		for k, v := range begin.Stats.Tracker {
			vend := end.Stats.Tracker[k]
			if metrics[k] == stats.KindThroughput {
				if v.Value > 0 {
					throughput := (vend.Value - v.Value) / seconds
					v.Value = throughput
				}
			} else {
				v.Value = vend.Value // more timely
			}
			begin.Stats.Tracker[k] = v
		}
	}
	return statusMapBegin, nil
}
