// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"

	"github.com/NVIDIA/aistore/cmd/cli/teb"

	"github.com/urfave/cli"
)

// This file contains implementation of the top-level `show cluster cpu` and `show cluster memory` command.

func showCPUHandler(c *cli.Context) error { return showCluDetail(c, true) }
func showMemHandler(c *cli.Context) error { return showCluDetail(c, false) }

func showCluDetail(c *cli.Context, cpu bool) error {
	var (
		usejs       = flagIsSet(c, jsonFlag)
		hideHeader  = flagIsSet(c, noHeaderFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}

	setLongRunParams(c)

	smap, tstatusMap, pstatusMap, err := fillNodeStatusMap(c, "")
	if err != nil {
		return err
	}
	node, _, errA := arg0Node(c)
	if errA != nil {
		return errA
	}
	if node != nil {
		sid := node.ID()
		if node.IsProxy() {
			tstatusMap = nil
			pstatusMap = teb.NodeStatusMap{sid: pstatusMap[sid]}
		} else {
			pstatusMap = nil
			tstatusMap = teb.NodeStatusMap{sid: tstatusMap[sid]}
		}
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

	if usejs {
		body := struct {
			Proxies teb.NodeStatusMap `json:"proxies,omitempty"`
			Targets teb.NodeStatusMap `json:"targets,omitempty"`
		}{
			Proxies: pstatusMap,
			Targets: tstatusMap,
		}
		return teb.Print(body, "", teb.Jopts(true))
	}

	if cpu {
		tableP, tableT := teb.MakeTabCPU(smap, pstatusMap, tstatusMap)
		out := tableP.Template(hideHeader) + tableT.Template(true)
		return teb.Print(out, out, teb.Jopts(usejs))
	}
	tableP, tableT := teb.MakeTabMem(smap, pstatusMap, tstatusMap, units)
	out := tableP.Template(hideHeader) + tableT.Template(true)
	return teb.Print(out, out, teb.Jopts(usejs))
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
