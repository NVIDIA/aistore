// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"
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

func fillNodeStatusMap(c *cli.Context, daeType string) (smap *cluster.Smap, tstatusMap, pstatusMap stats.DaemonStatusMap, err error) {
	if smap, err = getClusterMap(c); err != nil {
		return
	}
	var (
		wg         cos.WG
		mu         = &sync.Mutex{}
		pcnt, tcnt = smap.CountProxies(), smap.CountTargets()
	)
	switch daeType {
	case apc.Target:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), tcnt)
		tstatusMap = make(stats.DaemonStatusMap, tcnt)
		daeStatus(smap.Tmap, tstatusMap, wg, mu)
	case apc.Proxy:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt)
		pstatusMap = make(stats.DaemonStatusMap, pcnt)
		daeStatus(smap.Pmap, pstatusMap, wg, mu)
	default:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt+tcnt)
		tstatusMap = make(stats.DaemonStatusMap, tcnt)
		pstatusMap = make(stats.DaemonStatusMap, pcnt)
		daeStatus(smap.Tmap, tstatusMap, wg, mu)
		daeStatus(smap.Pmap, pstatusMap, wg, mu)
	}

	wg.Wait()
	return
}

func daeStatus(nodeMap cluster.NodeMap, out stats.DaemonStatusMap, wg cos.WG, mu *sync.Mutex) {
	for _, si := range nodeMap {
		wg.Add(1)
		go func(si *cluster.Snode) {
			_status(si, mu, out)
			wg.Done()
		}(si)
	}
}

func _status(node *cluster.Snode, mu *sync.Mutex, out stats.DaemonStatusMap) {
	daeStatus, err := api.GetDaemonStatus(apiBP, node)
	if err != nil {
		if herr, ok := err.(*cmn.ErrHTTP); ok {
			daeStatus = &stats.DaemonStatus{Snode: node, Status: herr.TypeCode}
		} else if strings.HasPrefix(err.Error(), "errNodeNotFound") {
			daeStatus = &stats.DaemonStatus{Snode: node, Status: "[errNodeNotFound]"}
		} else {
			daeStatus = &stats.DaemonStatus{Snode: node, Status: "[" + err.Error() + "]"}
		}
	} else if daeStatus.Status == "" {
		daeStatus.Status = tmpls.NodeOnline
		switch {
		case node.Flags.IsSet(cluster.NodeFlagMaint):
			daeStatus.Status = apc.NodeMaintenance
		case node.Flags.IsSet(cluster.NodeFlagDecomm):
			daeStatus.Status = apc.NodeDecommission
		}
	}

	mu.Lock()
	out[node.ID()] = daeStatus
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

// throughput as F(stats.DaemonStats)
func _daeBps(node *cluster.Snode, metrics cos.StrKVs, statsBegin *stats.DaemonStats, averageOver time.Duration) error {
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

// troughput as F(stats.ClusterStats)
func _cluStatsBps(metrics cos.StrKVs, statsBegin stats.ClusterStats, averageOver time.Duration) error {
	time.Sleep(averageOver)

	statsEnd, err := api.GetClusterStats(apiBP)
	if err != nil {
		return err
	}
	seconds := cos.MaxI64(int64(averageOver.Seconds()), 1)
	debug.Assert(seconds > 1)
	for tid, begin := range statsBegin.Target {
		end := statsEnd.Target[tid]
		if begin == nil || end == nil {
			return fmt.Errorf("%s seems to be offline", cluster.Tname(tid))
		}
		for name, v := range begin.Tracker {
			vend := end.Tracker[name]
			// (unlike stats.KindComputedThroughput)
			if metrics[name] == stats.KindThroughput {
				if v.Value > 0 {
					throughput := (vend.Value - v.Value) / seconds
					v.Value = throughput
				}
			} else {
				v.Value = vend.Value // more timely
			}
			begin.Tracker[name] = v
		}
	}
	return nil
}

// units-per-second as F(stats.DaemonStatusMap)
func _cluStatusMapPs(c *cli.Context, mapBegin stats.DaemonStatusMap, metrics cos.StrKVs,
	averageOver time.Duration) (stats.DaemonStatusMap, stats.DaemonStatusMap, error) {
	var (
		mapEnd  stats.DaemonStatusMap
		err     error
		seconds = cos.MaxI64(int64(averageOver.Seconds()), 1) // averaging per second
	)
	debug.Assert(seconds > 1) // expecting a few

	if mapBegin == nil {
		// begin stats
		if _, mapBegin, _, err = fillNodeStatusMap(c, apc.Target); err != nil {
			return nil, nil, err
		}
	}

	time.Sleep(averageOver)

	// post-interval stats
	if _, mapEnd, _, err = fillNodeStatusMap(c, apc.Target); err != nil {
		return nil, nil, err
	}

	// updating and returning mapBegin
	for tid, begin := range mapBegin {
		end := mapEnd[tid]
		for k, v := range begin.Tracker {
			if kind, ok := metrics[k]; !ok || kind == stats.KindCounter { // skip counters, if any
				continue
			}
			vend := end.Tracker[k]
			v.Value = (vend.Value - v.Value) / seconds
			begin.Tracker[k] = v
		}
	}
	return mapBegin, mapEnd, nil
}
