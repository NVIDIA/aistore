// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles cluster and daemon operations.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"math"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/urfave/cli"
)

func cluDaeStatus(c *cli.Context, smap *meta.Smap, tstatusMap, pstatusMap teb.StstMap, cfg *cmn.ClusterConfig, sid string) error {
	var (
		usejs       = flagIsSet(c, jsonFlag)
		hideHeader  = flagIsSet(c, noHeaderFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}
	body := teb.StatusHelper{
		Smap:      smap,
		CluConfig: cfg,
		Status: teb.StatsAndStatusHelper{
			Pmap: pstatusMap,
			Tmap: tstatusMap,
		},
	}
	if res, ok := pstatusMap[sid]; ok {
		table := teb.NewDaeStatus(res, smap, apc.Proxy, units)
		out := table.Template(hideHeader)
		return teb.Print(res, out, teb.Jopts(usejs))
	}
	if res, ok := tstatusMap[sid]; ok {
		table := teb.NewDaeStatus(res, smap, apc.Target, units)
		out := table.Template(hideHeader)
		return teb.Print(res, out, teb.Jopts(usejs))
	}
	if sid == apc.Proxy {
		table := teb.NewDaeMapStatus(&body.Status, smap, apc.Proxy, units)
		out := table.Template(hideHeader)
		return teb.Print(body, out, teb.Jopts(usejs))
	}
	if sid == apc.Target {
		table := teb.NewDaeMapStatus(&body.Status, smap, apc.Target, units)
		out := table.Template(hideHeader)
		return teb.Print(body, out, teb.Jopts(usejs))
	}
	if sid != "" {
		return fmt.Errorf("expecting a valid NODE_ID or node type (\"proxy\" or \"target\"), got %q", sid)
	}

	//
	// `ais show cluster` (two tables and Summary)
	//
	tableP := teb.NewDaeMapStatus(&body.Status, smap, apc.Proxy, units)
	tableT := teb.NewDaeMapStatus(&body.Status, smap, apc.Target, units)

	// totals: num disks and capacity; software version and build tiume
	body.NumDisks, body.Capacity = _totals(body.Status.Tmap, units, cfg)
	body.Version, body.BuildTime = _clusoft(body.Status.Tmap, body.Status.Pmap)

	out := tableP.Template(false) + "\n"
	out += tableT.Template(false) + "\n"

	// summary
	title := fgreen("Summary:")
	if isRebalancing(body.Status.Tmap) {
		title = fcyan("Summary:")
	}

	out += title + "\n" + teb.ClusterSummary
	return teb.Print(body, out, teb.Jopts(usejs))
}

func _totals(tmap teb.StstMap, units string, cfg *cmn.ClusterConfig) (num int, cs string) {
	var used, avail int64
outer:
	for _, ds := range tmap {
		var (
			tcdf   = ds.Tcdf
			fsIDs  = make([]cos.FsID, 0, len(tcdf.Mountpaths))
			unique bool
		)
		for _, cdf := range tcdf.Mountpaths {
			fsIDs, unique = cos.AddUniqueFsID(fsIDs, cdf.FS.FsID)
			if !unique {
				continue
			}
			used += int64(cdf.Used)
			avail += int64(cdf.Avail)
			num += len(cdf.Disks)

			// TODO: a simplifying local-playground assumption and shortcut - won't work with loop devices, etc.
			// (ref: 152408)
			if ds.DeploymentType == apc.DeploymentDev {
				break outer
			}
		}
	}
	if avail == 0 {
		debug.Assert(num == 0)
		return 0, ""
	}

	pctUsed := used * 100 / (used + avail)
	if pctUsed > 60 {
		// add precision
		fpct := math.Ceil(float64(used) * 100 / float64(used+avail))
		pctUsed = int64(fpct)
	}

	pct := fmt.Sprintf("%d%%", pctUsed)
	switch {
	case pctUsed >= cfg.Space.HighWM:
		pct = fred(pct)
	case pctUsed > cfg.Space.LowWM:
		pct = fcyan(pct)
	case pctUsed > cfg.Space.CleanupWM:
		pct = fblue(pct)
	default:
		pct = fgreen(pct)
	}
	cs = fmt.Sprintf("used %s (%s), available %s", teb.FmtSize(used, units, 2), pct, teb.FmtSize(avail, units, 2))

	return num, cs
}

func _clusoft(nodemaps ...teb.StstMap) (version, build string) {
	var multiver, multibuild bool
	for _, m := range nodemaps {
		for _, ds := range m {
			if !multiver {
				if version == "" {
					version = ds.Version
				} else if version != ds.Version {
					multiver = true
					version = ""
				}
			}
			if !multibuild {
				if build == "" {
					build = ds.BuildTime
				} else if build != ds.BuildTime {
					multibuild = true
					build = ""
				}
			}
		}
	}
	return version, build
}
