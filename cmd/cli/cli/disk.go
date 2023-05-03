// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains implementation of the top-level `show` command.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ios"
	"golang.org/x/sync/errgroup"
)

type (
	dstats struct {
		tid   string
		stats ios.AllDiskStats
	}
	dstatsCtx struct {
		tid string
		ch  chan dstats
	}
)

func (ctx *dstatsCtx) get() error {
	diskStats, err := api.GetDiskStats(apiBP, ctx.tid)
	if err != nil {
		return err
	}
	ctx.ch <- dstats{stats: diskStats, tid: ctx.tid}
	return nil
}

func getDiskStats(smap *meta.Smap, tid string) ([]teb.DiskStatsHelper, error) {
	var (
		targets = smap.Tmap
		l       = smap.CountActiveTs()
	)
	if tid != "" {
		tsi := smap.GetNode(tid)
		if tsi.InMaintOrDecomm() {
			return nil, fmt.Errorf("target %s is unaivailable at this point", tsi.StringEx())
		}
		targets = meta.NodeMap{tid: tsi}
		l = 1
	}
	dsh := make([]teb.DiskStatsHelper, 0, l)
	ch := make(chan dstats, l)

	wg, _ := errgroup.WithContext(context.Background())
	for tid, tsi := range targets {
		if tsi.InMaintOrDecomm() {
			continue
		}
		ctx := &dstatsCtx{ch: ch, tid: tid}
		wg.Go(ctx.get) // api.GetDiskStats
	}

	err := wg.Wait()
	close(ch)
	if err != nil {
		return nil, err
	}
	for res := range ch {
		for name, stat := range res.stats {
			dsh = append(dsh, teb.DiskStatsHelper{TargetID: res.tid, DiskName: name, Stat: stat})
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

	return dsh, nil
}

func collapseDisks(dsh []teb.DiskStatsHelper, numTs int) {
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
	}
	for tid, dst := range tsums {
		dn := int64(dnums[tid])
		dst.Stat.Ravg = cos.DivRound(dst.Stat.Ravg, dn)
		dst.Stat.Wavg = cos.DivRound(dst.Stat.Wavg, dn)
		dst.Stat.Util = cos.DivRound(dst.Stat.Util, dn)
	}
	// finally, reappend & re-sort
	dsh = dsh[:0]
	for tid, dst := range tsums {
		debug.Assert(tid == dst.TargetID)
		dsh = append(dsh, *dst)
	}
	sort.Slice(dsh, func(i, j int) bool {
		return dsh[i].TargetID < dsh[j].TargetID
	})
}
