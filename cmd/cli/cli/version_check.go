// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains utility functions and types.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"

	"github.com/urfave/cli"
)

// The CLI determines compatibility as follows:
//
// * different major-version always means: incompatible
//
// * minor-version distance:
//   - gap == 0 => fully compatible
//   - gap == 1 => warn: "may not be fully compatible"
//   - gap >= 2 => incompatible

type cliVersion struct {
	major int
	minor int
}

func parseCliVersion(s string) (v cliVersion, ok bool) {
	mm := strings.Split(s, versionSepa)
	if len(mm) < 2 {
		return v, false
	}
	major, err := strconv.Atoi(mm[0])
	if err != nil {
		return v, false
	}
	minor, err := strconv.Atoi(mm[1])
	if err != nil {
		return v, false
	}
	return cliVersion{major: major, minor: minor}, true
}

func parseCliVersionOrWarn(c *cli.Context, s string) (v cliVersion, ok bool) {
	v, ok = parseCliVersion(s)
	if ok {
		return v, true
	}
	warn := fmt.Sprintf("unexpected aistore version format: %q", s)
	fmt.Fprintln(c.App.ErrWriter, fred("Error: ")+warn)
	debug.Assert(false)
	return v, false
}

func versionExpected(v cliVersion) string {
	return strconv.Itoa(v.major) + versionSepa + strconv.Itoa(v.minor)
}

// NOTE: return absolute gap (CLI older than AIS, and vice versa)
func versionGap(expected, actual cliVersion) (gap int, incompat bool) {
	if expected.major != actual.major {
		return 2, true
	}
	gap = expected.minor - actual.minor
	if gap < 0 {
		gap = -gap
	}
	return gap, gap > 1
}

func supportsAllAtLeast(statusMap teb.NodeStatusMap, major, minor int) bool {
	for _, ds := range statusMap {
		if ds.Node.Snode.InMaintOrDecomm() {
			continue
		}
		if !supportsAtLeast(ds.Version, major, minor) {
			return false
		}
	}
	return true // default
}
func supportsAtLeast(version string, major, minor int) bool {
	v, ok := parseCliVersion(version)
	if !ok {
		return false
	}
	return v.major > major || (v.major == major && v.minor >= minor)
}

func checkVersionWarn(c *cli.Context, role string, stmap teb.NodeStatusMap) bool {
	const fmtEmptyVer = "empty version from %s (in maintenance mode?)"

	longParams := getLongRunParams(c)
	if longParams != nil && longParams.iters > 0 {
		return false // already warned once, nothing to do
	}

	expectedVer, ok := parseCliVersionOrWarn(c, cmn.VersionAIStore)
	if !ok {
		return false
	}
	expected := versionExpected(expectedVer)

	for _, ds := range stmap {
		if ds.Version == "" {
			if ds.Node.Snode.InMaintOrDecomm() {
				continue
			}
			warn := fmt.Sprintf(fmtEmptyVer, ds.Node.Snode.StringEx())
			actionWarn(c, warn)
			continue
		}

		actualVer, ok := parseCliVersion(ds.Version)
		if !ok {
			warn := fmt.Sprintf("%s: unexpected version format: %q", ds.Node.Snode.StringEx(), ds.Version)
			fmt.Fprintln(c.App.ErrWriter, fred("Error: ")+warn)
			debug.Assert(false)
			continue
		}

		gap, incompat := versionGap(expectedVer, actualVer)
		if gap == 0 {
			continue
		}

		cnt := countMismatch(stmap, ds, func(v cliVersion) bool {
			gap2, _ := versionGap(expectedVer, v)
			return gap2 == gap
		})
		verWarn(c, ds.Node.Snode, role, ds.Version, expected, cnt, incompat)
		return false
	}
	return true
}

// countMismatch counts nodes (excluding ds itself) that match the mismatch condition.
func countMismatch(stmap teb.NodeStatusMap, ds *stats.NodeStatus, matchFunc func(cliVersion) bool) int {
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
		v, ok := parseCliVersion(ds2.Version)
		if !ok {
			continue
		}
		if matchFunc(v) {
			cnt++
		}
	}
	return cnt
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
