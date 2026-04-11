// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"sort"

	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"

	"github.com/urfave/cli"
)

// This file contains implementation of the top-level `show cluster cpu` and `show cluster memory` command.
//
// TODO:
// - support [NODE_ID]
// - support --refresh / --count
// - add a second memory template that omits SWAP * columns when swap is zero across all nodes
// - CPU UX: either rename USER / SYSTEM / TOTAL to make units explicit, or drop them from the default view
// - memory UX pass: shorten headers (PROC SHARED, ACTUAL USED, ACTUAL FREE) to reduce width
// - make proxy/target ordering - respect K8s POD names; primary must be always first

type cluDetail struct {
	Smap *meta.Smap
	Rows []*stats.NodeStatus
}

func showCPUHandler(c *cli.Context) error { return showCluDetail(c, true) }
func showMemHandler(c *cli.Context) error { return showCluDetail(c, false) }

func showCluDetail(c *cli.Context, cpu bool) error {
	setLongRunParams(c)

	smap, tstatusMap, pstatusMap, err := fillNodeStatusMap(c, "")
	if err != nil {
		return err
	}

	// Capability check is structural (nil Proc/Mem), not version-based,
	// because detailed per-process CPU and system memory stats are being added
	// in v4.5, which is still in development.
	//
	// TODO: once 4.5 is released, switch to version-based check:
	//   minVer := aisnodeVer{major: 4, minor: 5}
	//   if !supportsAllAtLeast(tstatusMap, minVer) || !supportsAllAtLeast(pstatusMap, minVer) { ... }
	const errCapability = "this command requires all cluster nodes to run AIS v4.5 or later"
	if cpu {
		if !supportsDetailedCPU(tstatusMap) || !supportsDetailedCPU(pstatusMap) {
			return errors.New(errCapability)
		}
	} else {
		if !supportsDetailedMem(tstatusMap) || !supportsDetailedMem(pstatusMap) {
			return errors.New(errCapability)
		}
	}

	rows := flattenNodeStatusMap(tstatusMap, pstatusMap)
	if len(rows) == 0 {
		return nil
	}

	body := cluDetail{
		Smap: smap,
		Rows: rows,
	}

	tmpl := teb.CluMemTmpl
	if cpu {
		tmpl = teb.CluCPUTmpl
	}
	if flagIsSet(c, noHeaderFlag) {
		if cpu {
			tmpl = teb.CluCPUTmplNoHdr
		} else {
			tmpl = teb.CluMemTmplNoHdr
		}
	}

	return teb.Print(body, tmpl)
}

func flattenNodeStatusMap(tstatusMap, pstatusMap teb.NodeStatusMap) []*stats.NodeStatus {
	rows := make([]*stats.NodeStatus, 0, len(pstatusMap)+len(tstatusMap))

	for _, ds := range pstatusMap {
		rows = append(rows, ds)
	}
	for _, ds := range tstatusMap {
		rows = append(rows, ds)
	}

	sort.Slice(rows, func(i, j int) bool {
		di, dj := rows[i].Snode.ID(), rows[j].Snode.ID()

		// proxies first, then targets; within each group sort by node ID
		ti, tj := rows[i].Snode.DaeType, rows[j].Snode.DaeType
		if ti != tj {
			return ti < tj
		}
		return di < dj
	})

	return rows
}

func supportsDetailedCPU(statusMap teb.NodeStatusMap) bool {
	for _, ds := range statusMap {
		if ds.Node.Snode.InMaintOrDecomm() {
			continue
		}
		if ds.MemCPUInfo.Proc == nil {
			return false
		}
	}
	return true
}

func supportsDetailedMem(statusMap teb.NodeStatusMap) bool {
	for _, ds := range statusMap {
		if ds.Node.Snode.InMaintOrDecomm() {
			continue
		}
		if ds.MemCPUInfo.Proc == nil || ds.MemCPUInfo.Mem == nil {
			return false
		}
	}
	return true
}
