// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains implementation of the top-level `show` command.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"
)

type (
	dstats struct {
		tid   string
		stats ios.AllDiskStats
		tcdf  *fs.Tcdf
	}
	dstatsCtx struct {
		tid string
		ch  chan dstats
	}
)

// NOTE: [backward compatibility] v3.23
func (ctx *dstatsCtx) get() error {
	// 1. get any stats
	out, err := api.GetAnyStats(apiBP, ctx.tid, apc.WhatDiskRWUtilCap)
	if err != nil {
		return V(err)
	}

	// 2. current version
	var tcdfExt fs.TcdfExt
	err = jsoniter.Unmarshal(out, &tcdfExt)
	if err == nil && tcdfExt.AllDiskStats != nil {
		ctx.ch <- dstats{tid: ctx.tid, stats: tcdfExt.AllDiskStats, tcdf: &tcdfExt.Tcdf}
		return nil
	}

	// 3. v3.23 and older
	var stats ios.AllDiskStats
	err = jsoniter.Unmarshal(out, &stats)
	if err != nil {
		return err
	}
	ctx.ch <- dstats{tid: ctx.tid, stats: stats}
	return nil
}

func getDiskStats(c *cli.Context, smap *meta.Smap, tid string) (_ []*teb.DiskStatsHelper, withCap bool, err error) {
	var (
		targets = smap.Tmap
		l       = smap.CountActiveTs()
	)
	if tid != "" {
		tsi := smap.GetNode(tid)
		if tsi.InMaint() {
			return nil, false, fmt.Errorf("target %s is currently in maintenance", tsi.StringEx())
		}
		if tsi.InMaintOrDecomm() {
			return nil, false, fmt.Errorf("target %s is being decommissioned", tsi.StringEx())
		}
		targets = meta.NodeMap{tid: tsi}
		l = 1
	}
	dsh := make([]*teb.DiskStatsHelper, 0, l)
	ch := make(chan dstats, l)

	wg, _ := errgroup.WithContext(context.Background())
	for tid, tsi := range targets {
		if tsi.InMaintOrDecomm() {
			continue
		}
		ctx := &dstatsCtx{ch: ch, tid: tid}
		wg.Go(ctx.get) // api.GetAnyStats
	}

	err = wg.Wait()
	close(ch)
	if err != nil {
		return nil, false, err
	}
	for res := range ch {
		for name, stat := range res.stats {
			ds := &teb.DiskStatsHelper{TargetID: res.tid, DiskName: name, Stat: stat}
			if res.tcdf != nil {
				for mpath, mi := range res.tcdf.Mountpaths {
					// TODO: multi-disk mountpath
					if len(mi.Disks) == 0 {
						if mi.Label.IsNil() {
							actionWarn(c, "mountpath "+mpath+" has no disks")
						}
					} else if mi.Disks[0] == ds.DiskName {
						ds.Tcdf = res.tcdf
						withCap = true
					}
				}
			}
			dsh = append(dsh, ds)
		}
	}

	sort.Slice(dsh, func(i, j int) bool {
		if dsh[i].TargetID != dsh[j].TargetID {
			return dsh[i].TargetID < dsh[j].TargetID
		}
		if dsh[i].DiskName != dsh[j].DiskName {
			return dsh[i].DiskName < dsh[j].DiskName
		}
		return dsh[i].Stat.Util > dsh[j].Stat.Util
	})

	return dsh, withCap, nil
}

func collapseDisks(dsh []*teb.DiskStatsHelper, numTs int) {
	dnums := make(map[string]int, numTs)
	for _, src := range dsh {
		if _, ok := dnums[src.TargetID]; !ok {
			dnums[src.TargetID] = 1
		} else {
			dnums[src.TargetID]++
		}
	}
	tsums := make(map[string]*teb.DiskStatsHelper, numTs)
	for _, src := range dsh {
		dst, ok := tsums[src.TargetID]
		if !ok {
			dst = &teb.DiskStatsHelper{}
			dn := dnums[src.TargetID]
			dst.TargetID = src.TargetID
			dst.DiskName = fmt.Sprintf("(%d disk%s)", dn, cos.Plural(dn))
			tsums[src.TargetID] = dst
		}
		dst.Stat.RBps += src.Stat.RBps
		dst.Stat.Ravg += src.Stat.Ravg
		dst.Stat.WBps += src.Stat.WBps
		dst.Stat.Wavg += src.Stat.Wavg
		dst.Stat.Util += src.Stat.Util

		dst.Tcdf = src.Tcdf
	}
	for tid, dst := range tsums {
		dn := int64(dnums[tid])
		dst.Stat.Ravg = cos.DivRound(dst.Stat.Ravg, dn)
		dst.Stat.Wavg = cos.DivRound(dst.Stat.Wavg, dn)
		dst.Stat.Util = cos.DivRound(dst.Stat.Util, dn)
	}
	// finally, re-append & sort
	dsh = dsh[:0]
	for tid, dst := range tsums {
		debug.Assert(tid == dst.TargetID)
		dsh = append(dsh, dst)
	}
	sort.Slice(dsh, func(i, j int) bool {
		return dsh[i].TargetID < dsh[j].TargetID
	})
}
