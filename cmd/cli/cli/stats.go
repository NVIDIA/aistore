// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains utility functions and types.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/urfave/cli"
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
// teb.StstMap
//

const versionSepa = "."

func fillNodeStatusMap(c *cli.Context, daeType string) (smap *meta.Smap, tstatusMap, pstatusMap teb.StstMap, err error) {
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
		tstatusMap = make(teb.StstMap, tcnt)
		daeStatus(smap.Tmap, tstatusMap, wg, mu)
	case apc.Proxy:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt)
		pstatusMap = make(teb.StstMap, pcnt)
		daeStatus(smap.Pmap, pstatusMap, wg, mu)
	default:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt+tcnt)
		tstatusMap = make(teb.StstMap, tcnt)
		pstatusMap = make(teb.StstMap, pcnt)
		daeStatus(smap.Tmap, tstatusMap, wg, mu)
		daeStatus(smap.Pmap, pstatusMap, wg, mu)
	}

	wg.Wait()

	mmc := strings.Split(cmn.VersionAIStore, versionSepa)
	debug.Assert(len(mmc) > 1)
	ok := checkVersionWarn(c, apc.Target, mmc, tstatusMap)
	if ok && pstatusMap != nil {
		_ = checkVersionWarn(c, apc.Proxy, mmc, pstatusMap)
	}
	return
}

func isRebalancing(tstatusMap teb.StstMap) bool {
	for _, ds := range tstatusMap {
		if ds.RebSnap != nil {
			if !ds.RebSnap.IsAborted() && ds.RebSnap.EndTime.IsZero() {
				return true
			}
		}
	}
	return false
}

func checkVersionWarn(c *cli.Context, role string, mmc []string, stmap teb.StstMap) bool {
	const fmtEmptyVer = "empty version from %s (in maintenance mode?)"

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
		// major
		if mmc[0] != mmx[0] {
			// count more of the same
			var cnt int
			for _, ds2 := range stmap {
				if ds.Node.Snode.ID() != ds2.Node.Snode.ID() {
					mmx2 := strings.Split(ds2.Version, versionSepa)
					if mmc[0] != mmx2[0] {
						cnt++
					}
				}
			}
			verWarn(c, ds.Node.Snode, role, ds.Version, expected, cnt, true)
			return false
		}
		// minor
		minx, err := strconv.Atoi(mmx[1])
		debug.AssertNoErr(err)
		if minc != minx {
			incompat := minc-minx > 1 || minc-minx < -1
			// ditto
			var cnt int
			for _, ds2 := range stmap {
				if ds2.Node.Snode.InMaintOrDecomm() {
					continue
				}
				if ds2.Version == "" {
					warn := fmt.Sprintf(fmtEmptyVer, ds2.Node.Snode.StringEx())
					actionWarn(c, warn)
					continue
				}
				if ds.Node.Snode.ID() != ds2.Node.Snode.ID() {
					mmx2 := strings.Split(ds2.Version, versionSepa)
					minx2, _ := strconv.Atoi(mmx2[1])
					if minc != minx2 {
						cnt++
					}
				}
			}
			verWarn(c, ds.Node.Snode, role, ds.Version, expected, cnt, incompat)
			return false
		}
	}
	return true
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
		s1 = fmt.Sprintf(" and %d other node%s", cnt, cos.Plural(cnt))
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

func daeStatus(nodeMap meta.NodeMap, out teb.StstMap, wg cos.WG, mu *sync.Mutex) {
	for _, si := range nodeMap {
		wg.Add(1)
		go func(si *meta.Snode) {
			_addStatus(si, mu, out)
			wg.Done()
		}(si)
	}
}

func _addStatus(node *meta.Snode, mu *sync.Mutex, out teb.StstMap) {
	ds, err := _status(node)
	if err != nil {
		ds = &stats.NodeStatus{}
		ds.Snode = node
		if herr, ok := err.(*cmn.ErrHTTP); ok {
			ds.Status = herr.TypeCode
		} else if strings.HasPrefix(err.Error(), "errNodeNotFound") {
			ds.Status = "[errNodeNotFound]"
		} else {
			ds.Status = "[" + err.Error() + "]"
		}
	} else if ds.Status == "" {
		ds.Status = teb.FmtNodeStatus(node)
	}

	mu.Lock()
	out[node.ID()] = ds
	mu.Unlock()
}

// NOTE: [backward compatibility] v3.22
func _status(node *meta.Snode) (ds *stats.NodeStatus, err error) {
	ds, err = api.GetStatsAndStatus(apiBP, node)
	if err == nil || !strings.Contains(err.Error(), "what=node_status") {
		return ds, err
	}
	var v *stats.NodeStatusV322
	if v, err = api.GetStatsAndStatusV322(apiBP, node); err != nil {
		return nil, err
	}
	ds = &stats.NodeStatus{
		RebSnap:        v.RebSnap,
		Status:         v.Status,
		DeploymentType: v.DeploymentType,
		Version:        v.Version,
		BuildTime:      v.BuildTime,
		K8sPodName:     v.K8sPodName,
		MemCPUInfo:     v.MemCPUInfo,
		SmapVersion:    v.SmapVersion,
	}
	ds.Node.Snode = v.NodeV322.Snode
	ds.Node.Tracker = v.NodeV322.Tracker
	ds.Node.Tcdf.PctMax = v.NodeV322.Tcdf.PctMax
	ds.Node.Tcdf.PctAvg = v.NodeV322.Tcdf.PctAvg
	ds.Node.Tcdf.PctMin = v.NodeV322.Tcdf.PctMin
	ds.Node.Tcdf.CsErr = v.NodeV322.Tcdf.CsErr
	ds.Node.Tcdf.Mountpaths = make(map[string]*fs.CDF, len(v.NodeV322.Tcdf.Mountpaths))

	var used, avail uint64
	for mpath, cdfv322 := range v.NodeV322.Tcdf.Mountpaths {
		cdf := &fs.CDF{}
		cdf.Capacity = cdfv322.Capacity
		used += cdf.Capacity.Used
		avail += cdf.Capacity.Avail
		cdf.Disks = cdfv322.Disks
		if i := strings.Index(cdfv322.FS, "("); i > 0 {
			if j := strings.Index(cdfv322.FS, ")"); j > i {
				cdf.FS.Fs = cdfv322.FS[:i]
				cdf.FS.FsType = cdfv322.FS[i+1 : j]
			}
		}
		ds.Node.Tcdf.Mountpaths[mpath] = cdf
	}
	ds.Node.Tcdf.TotalUsed = used
	ds.Node.Tcdf.TotalAvail = avail
	return ds, nil
}

//
// throughput
//

func _cluStatusBeginEnd(c *cli.Context, ini teb.StstMap, sleep time.Duration) (b, e teb.StstMap, err error) {
	b = ini
	if b == nil {
		// begin stats
		if _, b, _, err = fillNodeStatusMap(c, apc.Target); err != nil {
			return nil, nil, err
		}
	}

	time.Sleep(sleep)

	// post-interval (end) stats
	_, e, _, err = fillNodeStatusMap(c, apc.Target)
	return
}
