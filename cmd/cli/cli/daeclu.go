// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles cluster and daemon operations.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/urfave/cli"
)

func cluDaeStatus(c *cli.Context, smap *meta.Smap, tstatusMap, pstatusMap teb.StstMap,
	cfg *cmn.ClusterConfig, sid string) error {
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
	// `ais show cluster`
	if sid == "" {
		tableP := teb.NewDaeMapStatus(&body.Status, smap, apc.Proxy, units)
		tableT := teb.NewDaeMapStatus(&body.Status, smap, apc.Target, units)

		// total num disks - compare with teb._sumupMpathsAvail()
	outer:
		for _, node := range body.Status.Tmap {
			tcdf := node.TargetCDF
			for _, mi := range tcdf.Mountpaths {
				body.NumDisks += len(mi.Disks)
				if node.DeploymentType == apc.DeploymentDev {
					break outer // simplifying HACK that'll be true most of the time
				}
			}
		}

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

	return fmt.Errorf("expecting a valid NODE_ID or node type (\"proxy\" or \"target\"), got %q", sid)
}
