// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains utility functions and types.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"

	"github.com/urfave/cli"
)

// CLI version-check policy
//
// The CLI determines compatibility based on *release step count*, not on the
// conventional major/minor semantic versioning:
//
// * gap == 0 => fully compatible
// * gap == 1 => warn: “may not be fully compatible”
// * gap >= 2 => incompatible
//
// This rule has always been true for CLI <=> cluster checks, and remains true even
// across a major version bump (e.g., 3.31 → 4.0 counts as a single step).

func checkVersionWarn(c *cli.Context, role string, mmc []string, stmap teb.StstMap) bool {
	const fmtEmptyVer = "empty version from %s (in maintenance mode?)"

	longParams := getLongRunParams(c)
	if longParams != nil && longParams.iters > 0 {
		return false // already warned once, nothing to do
	}

	expected := mmc[0] + versionSepa + mmc[1]
	minc, err := strconv.Atoi(mmc[1])
	if err != nil {
		warn := fmt.Sprintf("unexpected aistore version format: %v", mmc)
		fmt.Fprintln(c.App.ErrWriter, fred("Error: ")+warn)
		debug.Assert(false)
		return false
	}

	for _, ds := range stmap {
		if ds.Version == "" {
			if ds.Node.Snode.InMaintOrDecomm() {
				continue
			}
			warn := fmt.Sprintf(fmtEmptyVer, ds.Node.Snode.StringEx())
			actionWarn(c, warn)
			continue
		}
		mmx := strings.Split(ds.Version, versionSepa)
		if _, err := strconv.Atoi(mmx[0]); err != nil {
			warn := fmt.Sprintf("%s: unexpected version format: %s, %v", ds.Node.Snode.StringEx(), ds.Version, mmx)
			fmt.Fprintln(c.App.ErrWriter, fred("Error: ")+warn)
			debug.Assert(false)
			continue
		}

		// 1. special exception for CLI: (4.0 <=> 3.31) does not constitute major incompatibility
		if verExcept(mmc, mmx) {
			cnt := countMismatch(stmap, ds, func(mmx2 []string) bool {
				return verExcept(mmc, mmx2)
			})
			verWarn(c, ds.Node.Snode, role, ds.Version, expected, cnt, false)
			return false
		}

		// 2. major
		if mmc[0] != mmx[0] {
			cnt := countMismatch(stmap, ds, func(mmx2 []string) bool {
				return mmc[0] != mmx2[0]
			})
			verWarn(c, ds.Node.Snode, role, ds.Version, expected, cnt, true)
			return false
		}

		// 3. minor
		minx, err := strconv.Atoi(mmx[1])
		debug.AssertNoErr(err)
		if minc != minx {
			incompat := minc-minx > 1 || minc-minx < -1
			cnt := countMismatch(stmap, ds, func(mmx2 []string) bool {
				minx2, _ := strconv.Atoi(mmx2[1])
				return minc != minx2
			})
			verWarn(c, ds.Node.Snode, role, ds.Version, expected, cnt, incompat)
			return false
		}
	}
	return true
}

// countMismatch counts nodes (excluding ds itself) that match the mismatch condition
func countMismatch(stmap teb.StstMap, ds *stats.NodeStatus, matchFunc func([]string) bool) int {
	var cnt int
	for _, ds2 := range stmap {
		if ds2.Node.Snode.InMaintOrDecomm() {
			continue
		}
		if ds.Node.Snode.ID() == ds2.Node.Snode.ID() {
			continue
		}
		if ds2.Version == "" {
			continue // empty versions already warned about in main loop
		}
		mmx2 := strings.Split(ds2.Version, versionSepa)
		if matchFunc(mmx2) {
			cnt++
		}
	}
	return cnt
}

func verExcept(mmc, mmx []string) bool {
	switch {
	case len(mmx) < 2 || len(mmc) < 2:
		return false
	case mmc[0] == mmx[0]:
		return false
	case mmc[0] == "4" && mmc[1] == "0" && mmx[0] == "3" && mmx[1] == "31":
		return true
	case mmx[0] == "4" && mmx[1] == "0" && mmc[0] == "3" && mmc[1] == "31":
		return true
	default:
		return false
	}
}

func verWarn(c *cli.Context, snode *meta.Snode, role, version, expected string, cnt int, incompat bool) {
	var (
		sname, warn, s1, s2 string
	)
	if role == apc.Proxy {
		sname = meta.Pname(snode.ID())
	} else {
		sname = meta.Tname(snode.ID())
	}
	s2 = "s"
	if cnt > 0 {
		s2 = ""
		s1 = fmt.Sprintf(" and %d other %s node%s", cnt, role, cos.Plural(cnt))
	}
	if incompat {
		warn = fmt.Sprintf("node %s%s run%s aistore software version %s, which is not compatible with the CLI (expecting v%s)",
			sname, s1, s2, version, expected)
	} else {
		if flagIsSet(c, nonverboseFlag) {
			return
		}
		warn = fmt.Sprintf("node %s%s run%s aistore software version %s, which may not be fully compatible with the CLI (expecting v%s)",
			sname, s1, s2, version, expected)
	}

	actionWarn(c, warn+"\n")
}
