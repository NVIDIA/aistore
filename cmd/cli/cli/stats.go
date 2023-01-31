// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"
)

var (
	curPrxStatus stats.DaemonStatusMap
	curTgtStatus stats.DaemonStatusMap
)

//
// stats.DaemonStatusMap
//

func fillNodeStatusMap(c *cli.Context, targetOnly bool) (*cluster.Smap, error) {
	smap, err := getClusterMap(c)
	if err != nil {
		return nil, err
	}

	pcnt, tcnt := 0, smap.CountTargets()
	if !targetOnly {
		pcnt = smap.CountProxies()
		curPrxStatus = make(stats.DaemonStatusMap, pcnt)
	}
	curTgtStatus = make(stats.DaemonStatusMap, tcnt)

	wg := cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt+tcnt)
	mu := &sync.Mutex{}
	if !targetOnly {
		daeStatus(smap.Pmap, curPrxStatus, wg, mu)
	}
	daeStatus(smap.Tmap, curTgtStatus, wg, mu)
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
	daeInfo, err := api.GetDaemonStatus(apiBP, node)
	if err != nil {
		daeInfo = &stats.DaemonStatus{Snode: node, Status: "Error: " + err.Error()}
	} else if node.Flags.IsSet(cluster.NodeFlagMaint) {
		daeInfo.Status = "maintenance"
	} else if node.Flags.IsSet(cluster.NodeFlagDecomm) {
		daeInfo.Status = "decommission"
	}
	mu.Lock()
	daeMap[node.ID()] = daeInfo
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
// stats.ClusterStats
//

// throughput (Bps)
// TODO: s/inner loop/daemonBps (below)/
func clusterBps(st stats.ClusterStats, averageOver time.Duration) {
	time.Sleep(averageOver)
	st2, _ := api.GetClusterStats(apiBP)
	for tid, tgt := range st.Target {
		tgt2 := st2.Target[tid]
		for k, v := range tgt.Tracker {
			v2 := tgt2.Tracker[k]
			if stats.IsKindThroughput(k) {
				throughput := (v2.Value - v.Value) / cos.MaxI64(int64(averageOver.Seconds()), 1)
				v.Value = throughput
			} else {
				v.Value = v2.Value // more timely
			}
			tgt.Tracker[k] = v
		}
	}
}

func daemonBps(node *cluster.Snode, ds *stats.DaemonStats, averageOver time.Duration) {
	time.Sleep(averageOver)
	ds2, _ := api.GetDaemonStats(apiBP, node)
	for k, v := range ds.Tracker {
		v2 := ds2.Tracker[k]
		if stats.IsKindThroughput(k) {
			throughput := (v2.Value - v.Value) / cos.MaxI64(int64(averageOver.Seconds()), 1)
			v.Value = throughput
		} else {
			v.Value = v2.Value // more recent
		}
		ds.Tracker[k] = v
	}
}
