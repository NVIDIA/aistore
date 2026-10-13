// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"maps"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

// target placement weights (v5.2); see docs/placement.md

// assign weights 1, 2, 3, ... (times mult) in sorted target-ID order
func seqWeights(smap *meta.Smap, mult int64) map[string]int64 {
	tids := slices.Sorted(maps.Keys(smap.Tmap))
	weights := make(map[string]int64, len(tids))
	for i, tid := range tids {
		weights[tid] = int64(i+1) * mult
	}
	return weights
}

func zeroWeights(smap *meta.Smap) map[string]int64 {
	weights := make(map[string]int64, len(smap.Tmap))
	for tid := range smap.Tmap {
		weights[tid] = 0
	}
	return weights
}

// set weights on a freshly fetched Smap; retry when membership is busy
func setPlacementWeights(tb testing.TB, bp api.BaseParams, weights map[string]int64) (string, error) {
	return membershipRetry(tb, func() (string, error) {
		smap, err := api.GetClusterMap(bp)
		if err != nil {
			return "", err
		}
		for tid, w := range weights {
			if tsi := smap.GetTarget(tid); tsi != nil {
				tsi.SetPlacementWeight(w)
			}
		}
		return api.SetPlacementWeights(bp, smap)
	})
}

func checkPlacementWeights(t *testing.T, smap *meta.Smap, weights map[string]int64) {
	t.Helper()
	for tid, w := range weights {
		tsi := smap.GetTarget(tid)
		tassert.Fatalf(t, tsi != nil, "missing target %s", meta.Tname(tid))
		tassert.Fatalf(t, tsi.PlacementWeight() == w, "%s: expected weight %d, got %d", tsi.StringEx(), w, tsi.PlacementWeight())
	}
}

func checkHTTPStatus(t *testing.T, err error, status int, what string) {
	t.Helper()
	tassert.Fatalf(t, err != nil, "%s: expected error", what)
	herr := cmn.AsErrHTTP(err)
	tassert.Fatalf(t, herr != nil, "%s: expected HTTP error, got %v", what, err)
	tassert.Fatalf(t, herr.Status == status, "%s: expected status %d, got %d (%v)", what, status, herr.Status, err)
}

// ensure rebalance is enabled for the duration of the test
func enableRebalance(t *testing.T, bp api.BaseParams) {
	config, err := api.GetClusterConfig(bp)
	tassert.CheckFatal(t, err)
	if config.Rebalance.Enabled {
		return
	}
	tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": "true"})
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": "false"})
	})
}

// wait for rebalance and resilver that are already running, if any
// - unlike tools.WaitForRebalAndResil, do not wait (poll) for one to start
func waitRunningRebResil(t *testing.T, bp api.BaseParams) {
	t.Helper()
	xargs := xact.ArgsMsg{Kind: apc.ActRebalance, OnlyRunning: true, Timeout: tools.RebalanceTimeout}
	// until no rebalance is running (404), not just the current one finished
	_, err := api.WaitForStatus(bp, &xargs, func(status *nl.Status) (bool, bool, error) {
		return false, status.IsFinished(), nil
	})
	if err != nil && !cmn.IsStatusNotFound(err) {
		tassert.CheckError(t, err)
	}
	xargs = xact.ArgsMsg{Kind: apc.ActResilver, OnlyRunning: true, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForSnaps(bp, &xargs, xargs.NotRunning())
	tassert.CheckError(t, err)
}

// clear weights when the test exits
// - register before creating buckets: cleanups run LIFO, and a bucket override prevents clearing
// - no upfront wait: setPlacementWeights retries while membership is busy (rebalance running)
func clearWeightsOnExit(t *testing.T, bp api.BaseParams) {
	t.Cleanup(func() {
		smap, err := api.GetClusterMap(bp)
		tassert.CheckError(t, err)
		if err != nil || !smap.IsWeighted() {
			return
		}
		xid, err := setPlacementWeights(t, bp, zeroWeights(smap))
		tassert.CheckError(t, err)
		if xid != "" {
			tools.WaitForRebalanceByID(t, bp, xid)
		}
	})
}

// fail: invalid weights must leave the Smap unchanged
func TestPlacementWeightsInvalid(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2})
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		smap     = tools.GetClusterMap(t, proxyURL)
		tsi, _   = smap.GetRandTarget()
		tid      = tsi.ID()
	)
	if smap.IsWeighted() {
		t.Skipf("%s is weighted: %s", smap, smap.StrPlacementWeights())
	}
	clearWeightsOnExit(t, bp)

	tests := []struct {
		modify func(weights map[string]int64)
		name   string
	}{
		{name: "negative", modify: func(w map[string]int64) { w[tid] = -1 }},
		{name: "mixed-zero", modify: func(w map[string]int64) { w[tid] = 0 }},
		{name: "max-ratio", modify: func(w map[string]int64) { w[tid] = int64(len(w)) * 100 * meta.PlacementWeightMaxRatio }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			weights := seqWeights(smap, 100)
			test.modify(weights)
			_, err := setPlacementWeights(t, bp, weights)
			checkHTTPStatus(t, err, http.StatusBadRequest, test.name)
		})
	}

	t.Run("missing-target", func(t *testing.T) {
		clone := tools.GetClusterMap(t, proxyURL)
		for id := range clone.Tmap {
			clone.Tmap[id].SetPlacementWeight(1)
		}
		delete(clone.Tmap, tid)
		_, err := api.SetPlacementWeights(bp, clone)
		checkHTTPStatus(t, err, http.StatusBadRequest, "missing target")
	})

	t.Run("unknown-target", func(t *testing.T) {
		clone := tools.GetClusterMap(t, proxyURL)
		for id := range clone.Tmap {
			clone.Tmap[id].SetPlacementWeight(1)
		}
		fake := &meta.Snode{}
		fake.Init("fake-"+tid, apc.Target, nil)
		fake.SetPlacementWeight(1)
		delete(clone.Tmap, tid)
		clone.Tmap[fake.ID()] = fake
		_, err := api.SetPlacementWeights(bp, clone)
		checkHTTPStatus(t, err, http.StatusBadRequest, "unknown target")
	})

	t.Run("stale-smap", func(t *testing.T) {
		clone := tools.GetClusterMap(t, proxyURL)
		for id := range clone.Tmap {
			clone.Tmap[id].SetPlacementWeight(1)
		}
		clone.Version--
		_, err := api.SetPlacementWeights(bp, clone)
		checkHTTPStatus(t, err, http.StatusConflict, "stale Smap")
	})

	t.Run("wrong-cluster", func(t *testing.T) {
		clone := tools.GetClusterMap(t, proxyURL)
		for _, tsi := range clone.Tmap {
			tsi.SetPlacementWeight(1)
		}
		clone.UUID = cos.GenUUID()
		_, err := api.SetPlacementWeights(bp, clone)
		checkHTTPStatus(t, err, http.StatusConflict, "wrong cluster UUID")
	})

	after := tools.GetClusterMap(t, proxyURL)
	tassert.Errorf(t, after.Version == smap.Version, "Smap changed: v%d => v%d", smap.Version, after.Version)
	tassert.Errorf(t, !after.IsWeighted(), "unexpected weights: %s", after.StrPlacementWeights())
}

// fail: a single-target cluster rejects weights
func TestPlacementWeightsSingleTarget(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MaxTargets: 1})
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		smap     = tools.GetClusterMap(t, proxyURL)
	)
	if smap.IsWeighted() {
		t.Skipf("%s is weighted: %s", smap, smap.StrPlacementWeights())
	}
	_, err := setPlacementWeights(t, bp, seqWeights(smap, 1))
	checkHTTPStatus(t, err, http.StatusBadRequest, "single target")

	after := tools.GetClusterMap(t, proxyURL)
	tassert.Errorf(t, after.Version == smap.Version, "Smap changed: v%d => v%d", smap.Version, after.Version)
	tassert.Errorf(t, !after.IsWeighted(), "unexpected weights: %s", after.StrPlacementWeights())
}

// set, repeat, scale, and clear weights; check Smap versions and rebalance IDs
func TestPlacementWeightsSetClear(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2, Long: true})
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		smap     = tools.GetClusterMap(t, proxyURL)
		weights  = seqWeights(smap, 1)
	)
	if smap.CountActiveTs() != smap.CountTargets() {
		t.Skipf("%s has targets in maintenance", smap)
	}
	if smap.IsWeighted() {
		t.Skipf("%s is weighted: %s", smap, smap.StrPlacementWeights())
	}
	enableRebalance(t, bp)
	clearWeightsOnExit(t, bp)

	tlog.Logfln("set %v", weights)
	xid, err := setPlacementWeights(t, bp, weights)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid != "", "expected rebalance when setting weights")
	tools.WaitForRebalanceByID(t, bp, xid)

	smap = tools.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, smap.IsWeighted(), "expected weighted Smap")
	checkPlacementWeights(t, smap, weights)

	// replicated to all nodes
	for _, nmap := range []meta.NodeMap{smap.Pmap, smap.Tmap} {
		for _, si := range nmap {
			nsmap, err := api.GetNodeClusterMap(bp, si.ID())
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, nsmap.Version >= smap.Version, "%s: Smap v%d < v%d", si.StringEx(), nsmap.Version, smap.Version)
			checkPlacementWeights(t, nsmap, weights)
		}
	}

	tlog.Logln("same weights: no-op")
	xid, err = setPlacementWeights(t, bp, weights)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid == "", "same weights started rebalance %q", xid)
	same := tools.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, same.Version == smap.Version, "same weights changed Smap v%d => v%d", smap.Version, same.Version)

	tlog.Logln("proportional weights: new Smap, no rebalance")
	scaled := seqWeights(smap, 10)
	xid, err = setPlacementWeights(t, bp, scaled)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid == "", "proportional weights started rebalance %q", xid)
	smap2 := tools.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, smap2.Version > smap.Version, "proportional weights: expected new Smap (v%d)", smap.Version)
	checkPlacementWeights(t, smap2, scaled)

	tlog.Logln("rebalance disabled: rejected, weights unchanged")
	tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": "false"})
	t.Cleanup(func() { tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": "true"}) }) // (runs before clearing weights)
	reversed := make(map[string]int64, len(scaled))
	for tid, w := range scaled {
		reversed[tid] = int64(len(scaled)+1)*10 - w // not proportional
	}
	_, err = setPlacementWeights(t, bp, reversed)
	checkHTTPStatus(t, err, http.StatusServiceUnavailable, "rebalance disabled")
	tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": "true"})
	checkPlacementWeights(t, tools.GetClusterMap(t, proxyURL), scaled)

	tlog.Logln("clear weights")
	xid, err = setPlacementWeights(t, bp, zeroWeights(smap))
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid != "", "expected rebalance when clearing weights")
	tools.WaitForRebalanceByID(t, bp, xid)
	smap = tools.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, !smap.IsWeighted(), "expected unweighted Smap, got %s", smap.StrPlacementWeights())
}

// exercise CLI updates, show shortcuts, and shares with weights whose sum exceeds int64
func TestPlacementCLI(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2, Long: true})
	bin, err := exec.LookPath("ais")
	if err != nil {
		t.Skip("'ais' binary not found")
	}
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		smap     = tools.GetClusterMap(t, proxyURL)
	)
	if smap.IsWeighted() || smap.CountActiveTs() != smap.CountTargets() {
		t.Skipf("requires an unweighted cluster with all targets active: %s", smap)
	}
	t.Setenv(env.AisEndpoint, proxyURL)
	if bp.Token != "" {
		t.Setenv(env.AisAuthToken, bp.Token)
		t.Setenv(env.AisAuthTokenFile, "")
	}
	enableRebalance(t, bp)
	clearWeightsOnExit(t, bp)
	waitRunningRebResil(t, bp) // the CLI does not retry when membership is busy

	run := func(args ...string) string {
		t.Helper()
		out, err := exec.Command(bin, args...).CombinedOutput()
		tassert.Fatalf(t, err == nil, "ais %v: %v\n%s", args, err, out)
		return string(out)
	}
	wait := func(out string) {
		t.Helper()
		_, tail, found := strings.Cut(out, "Started rebalance \"")
		tassert.Fatalf(t, found, "expected rebalance ID in CLI output: %s", out)
		xid, _, found := strings.Cut(tail, "\"")
		tassert.Fatalf(t, found && xid != "", "missing rebalance ID: %s", out)
		tools.WaitForRebalanceByID(t, bp, xid)
	}

	const weight int64 = 1 << 62
	args := []string{"cluster", "placement", "set-weights", "rest=" + strconv.FormatInt(weight, 10)}
	weights := make(map[string]int64, len(smap.Tmap))
	for _, tid := range slices.Sorted(maps.Keys(smap.Tmap)) {
		weights[tid] = weight
	}
	out := run(args...)
	tassert.Fatalf(t, strings.Contains(out, "no rebalance required"), "equal weights: %s", out)
	weighted := tools.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, weighted.Version > smap.Version, "equal weights did not update Smap")
	checkPlacementWeights(t, weighted, weights)

	show := run("cluster", "placement", "show")
	tassert.Fatalf(t, show == run("show", "placement"), "show placement differs from canonical command")
	tassert.Fatalf(t, show == run("show", "cluster", "placement"), "show cluster placement differs from canonical command")
	share := strconv.FormatFloat(100/float64(len(weights)), 'f', 1, 64) + "%"
	// equal weights: banner, header, one row per target
	lines := strings.Split(strings.TrimSpace(show), "\n")
	tassert.Fatalf(t, len(lines) == len(weights)+2, "unexpected placement table: %s", show)
	tassert.Fatalf(t, strings.Contains(lines[0], "uniform (target weights are set and equal)"), "equal weights banner: %s", show)
	for _, line := range lines[2:] {
		fields := strings.Fields(line)
		tassert.Fatalf(t, len(fields) == 3 && fields[1] == strconv.FormatInt(weight, 10) && fields[2] == share,
			"large weights: expected share %s, got %q", share, line)
	}

	out = run(args...)
	tassert.Fatalf(t, strings.Contains(out, "weights unchanged"), "same weights: %s", out)
	same := tools.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, same.Version == weighted.Version, "CLI no-op changed Smap")

	// a partial update preserves unlisted targets and changes placement
	tid := slices.Sorted(maps.Keys(weights))[0]
	weights[tid] = weight / 2
	out = run("cluster", "placement", "set-weights", tid+"="+strconv.FormatInt(weights[tid], 10))
	wait(out)
	checkPlacementWeights(t, tools.GetClusterMap(t, proxyURL), weights)

	// explicit assignments override rest in either order; assignment forms are interchangeable
	for _, pair := range [][]string{
		{"rest=" + strconv.FormatInt(weight, 10), meta.Tname(tid) + "=" + strconv.FormatInt(weights[tid], 10)},
		{meta.Tname(tid) + "=", strconv.FormatInt(weights[tid], 10), "rest", "=" + strconv.FormatInt(weight, 10)},
		{tid, strconv.FormatInt(weights[tid], 10), "rest", strconv.FormatInt(weight, 10)},
		{meta.Tname(tid), "=", strconv.FormatInt(weights[tid], 10), "rest", "=", strconv.FormatInt(weight, 10)},
	} {
		out = run(append([]string{"cluster", "placement", "set-weights"}, pair...)...)
		tassert.Fatalf(t, strings.Contains(out, "weights unchanged"), "explicit override of rest: %s", out)
	}
	for id := range weights {
		if id != tid {
			weights[id] = weight / 4
		}
	}
	out = run("cluster", "placement", "set-weights",
		meta.Tname(tid)+"="+strconv.FormatInt(weights[tid], 10), "rest="+strconv.FormatInt(weight/4, 10))
	wait(out)
	checkPlacementWeights(t, tools.GetClusterMap(t, proxyURL), weights)

	wait(run("cluster", "placement", "clear-weights"))
	tassert.Fatalf(t, !tools.GetClusterMap(t, proxyURL).IsWeighted(), "CLI clear left weights set")
	out = run("cluster", "placement", "clear-weights")
	tassert.Fatalf(t, strings.Contains(out, "nothing to do"), "repeated CLI clear: %s", out)
}

// bucket opt-out: placement.weighting=uniform blocks clearing target weights
func TestPlacementBucketOverride(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2, Long: true})
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		smap     = tools.GetClusterMap(t, proxyURL)
		bck      = cmn.Bck{Name: "placement-" + cos.GenTie(), Provider: apc.AIS}
	)
	if smap.IsWeighted() {
		t.Skipf("%s is weighted: %s", smap, smap.StrPlacementWeights())
	}
	enableRebalance(t, bp)
	clearWeightsOnExit(t, bp) // (runs after bucket cleanup)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	_, err := api.SetBucketProps(bp, bck, &cmn.BpropsToSet{
		Placement: &cmn.PlacementConfToSet{Weighting: apc.Ptr("bogus")},
	})
	tassert.Fatalf(t, err != nil, "expected error setting invalid %s", cmn.PropPlacementWeighting)

	xid, err := setPlacementWeights(t, bp, seqWeights(smap, 1))
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, bp, xid)

	tlog.Logfln("set %s=%s", cmn.PropPlacementWeighting, cmn.WeightingUniform)
	_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{
		Placement: &cmn.PlacementConfToSet{Weighting: apc.Ptr(cmn.WeightingUniform)},
	})
	tassert.CheckFatal(t, err)
	props, err := api.HeadBucket(bp, bck, true /*don't add*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, props.Placement != nil && props.Placement.Weighting == cmn.WeightingUniform,
		"expected %s=%s, got %+v", cmn.PropPlacementWeighting, cmn.WeightingUniform, props.Placement)

	_, err = setPlacementWeights(t, bp, zeroWeights(smap))
	checkHTTPStatus(t, err, http.StatusConflict, "clear weights with bucket override")
	tassert.Fatalf(t, tools.GetClusterMap(t, proxyURL).IsWeighted(), "weights cleared despite bucket override")

	tlog.Logfln("reset %s", cmn.PropPlacementWeighting)
	_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{
		Placement: &cmn.PlacementConfToSet{Weighting: apc.Ptr("")},
	})
	tassert.CheckFatal(t, err)
	props, err = api.HeadBucket(bp, bck, true /*don't add*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, props.Placement == nil, "expected default placement, got %+v", props.Placement)

	xid, err = setPlacementWeights(t, bp, zeroWeights(smap))
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, bp, xid)
	tassert.Fatalf(t, !tools.GetClusterMap(t, proxyURL).IsWeighted(), "expected unweighted Smap")
}

// set and clear weights with data in the cluster; read everything back after each rebalance
func TestPlacementWeightsRebalance(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3, Long: true})
	var (
		bp = tools.BaseAPIParams(tools.RandomProxyURL(t))
		m  = &ioContext{t: t, num: 200, fileSize: cos.KiB, fixedSize: true}
	)
	m.initAndSaveState(true /*cleanup*/)
	if m.smap.CountActiveTs() != m.smap.CountTargets() {
		t.Skipf("%s has targets in maintenance", m.smap)
	}
	if m.smap.IsWeighted() {
		t.Skipf("%s is weighted: %s", m.smap, m.smap.StrPlacementWeights())
	}
	enableRebalance(t, bp)
	clearWeightsOnExit(t, bp)
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	m.puts()

	for _, weights := range []map[string]int64{seqWeights(m.smap, 1), zeroWeights(m.smap)} {
		tlog.Logfln("set %v", weights)
		xid, err := setPlacementWeights(t, bp, weights)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, xid != "", "expected rebalance for weight change")
		tools.WaitForRebalanceByID(t, bp, xid)
		m.gets(nil, true /*validate*/)
		m.ensureNoGetErrors()
	}
	m.waitAndCheckCluState()
}

// a target keeps its weight through maintenance; shares exclude targets in maintenance
func TestPlacementWeightsMaintenance(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3, Long: true})
	var (
		bp = tools.BaseAPIParams(tools.RandomProxyURL(t))
		m  = &ioContext{t: t, num: 100, fileSize: cos.KiB, fixedSize: true}
	)
	m.initAndSaveState(true /*cleanup*/)
	if m.smap.CountActiveTs() != m.smap.CountTargets() {
		t.Skipf("%s has targets in maintenance", m.smap)
	}
	if m.smap.IsWeighted() {
		t.Skipf("%s is weighted: %s", m.smap, m.smap.StrPlacementWeights())
	}
	enableRebalance(t, bp)
	clearWeightsOnExit(t, bp)
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	m.puts()

	weights := seqWeights(m.smap, 2)
	xid, err := setPlacementWeights(t, bp, weights)
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, bp, xid)

	tsi, _ := m.smap.GetRandTarget()
	tlog.Logfln("start maintenance %s", tsi.StringEx())
	actVal := &apc.ActValRmNode{DaemonID: tsi.ID()}
	xid, err = startMaintenanceRetry(t, bp, actVal)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { tools.ClearMaintenance(bp, tsi) })
	tools.WaitForRebalanceByID(t, bp, xid)

	smap, err := tools.WaitForClusterState(m.proxyURL, "target in maintenance",
		m.smap.Version, m.originalProxyCount, m.originalTargetCount-1)
	tassert.CheckFatal(t, err)
	checkPlacementWeights(t, smap, weights)
	m.gets(nil, true /*validate*/)
	m.ensureNoGetErrors()

	tlog.Logfln("stop maintenance %s", tsi.StringEx())
	xid, err = stopMaintenanceRetry(t, bp, actVal)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(m.proxyURL, "target is back",
		smap.Version, m.originalProxyCount, m.originalTargetCount)
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, bp, xid)
	checkPlacementWeights(t, smap, weights)

	m.gets(nil, true /*validate*/)
	m.ensureNoGetErrors()
}

// accept weights with one active target, then rebalance when the others leave maintenance
func TestPlacementWeightsOneActive(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2, Long: true})
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		smap     = tools.GetClusterMap(t, proxyURL)
	)
	if smap.IsWeighted() || smap.CountActiveTs() != smap.CountTargets() {
		t.Skipf("requires an unweighted cluster with all targets active: %s", smap)
	}
	enableRebalance(t, bp)
	clearWeightsOnExit(t, bp)
	tids := slices.Sorted(maps.Keys(smap.Tmap))
	args := &apc.ActValRmNode{SkipRebalance: true}
	args.SetIDs(tids[1:]...)
	var inMaintenance bool
	t.Cleanup(func() {
		if !inMaintenance {
			return
		}
		args.SkipRebalance = false
		xid, err := stopMaintenanceRetry(t, bp, args)
		tassert.CheckError(t, err)
		if err != nil {
			return
		}
		_, err = tools.WaitForClusterState(proxyURL, "maintenance cleanup", -1,
			smap.CountActivePs(), len(tids))
		tassert.CheckError(t, err)
		if err == nil {
			tools.WaitForRebalanceByID(t, bp, xid)
		}
	})
	_, err := startMaintenanceRetry(t, bp, args)
	tassert.CheckFatal(t, err)
	inMaintenance = true
	active, err := tools.WaitForClusterState(proxyURL, "one active target", smap.Version, smap.CountActivePs(), 1)
	tassert.CheckFatal(t, err)

	weights := seqWeights(active, 1)
	xid, err := setPlacementWeights(t, bp, weights)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid == "", "one active target started rebalance %q", xid)
	weighted := tools.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, weighted.Version > active.Version, "weights did not update Smap")
	checkPlacementWeights(t, weighted, weights)

	args.SkipRebalance = false
	xid, err = stopMaintenanceRetry(t, bp, args)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid != "", "returning targets did not start rebalance")
	restored, err := tools.WaitForClusterState(proxyURL, "targets back", weighted.Version, smap.CountActivePs(), len(tids))
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, bp, xid)
	inMaintenance = false
	checkPlacementWeights(t, restored, weights)
}

// a target joining a weighted cluster gets the rounded mean weight of active targets
func TestPlacementWeightsJoin(t *testing.T) {
	for _, weight := range []int64{0, 60} {
		t.Run(strconv.FormatInt(weight, 10), func(t *testing.T) { testPlacementWeightsJoin(t, weight) })
	}
}

func testPlacementWeightsJoin(t *testing.T, requested int64) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3, Long: true, RequiredDeployment: tools.ClusterTypeLocal})
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		smap     = tools.GetClusterMap(t, proxyURL)
	)
	if smap.CountActiveTs() != smap.CountTargets() {
		t.Skipf("%s has targets in maintenance", smap)
	}
	if smap.IsWeighted() {
		t.Skipf("%s is weighted: %s", smap, smap.StrPlacementWeights())
	}
	enableRebalance(t, bp)
	clearWeightsOnExit(t, bp)

	// distinct weights so that the mean differs from any single weight
	weights := seqWeights(smap, 3)
	xid, err := setPlacementWeights(t, bp, weights)
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, bp, xid)

	tsi, _ := smap.GetRandTarget()
	tlog.Logfln("decommission %s (weight %d)", tsi.StringEx(), weights[tsi.ID()])
	cmd := tools.GetRestoreCmd(tsi)
	if requested > 0 {
		var localPath string
		for _, arg := range cmd.Args {
			if p, ok := strings.CutPrefix(arg, "-local_config="); ok {
				localPath = p
				break
			}
		}
		tassert.Fatalf(t, localPath != "", "local config not found in restore command: %v", cmd.Args)
		original, err := os.ReadFile(localPath)
		tassert.CheckFatal(t, err)
		var local cmn.LocalConfig
		tassert.CheckFatal(t, cos.JSON.Unmarshal(original, &local))
		local.Placement = &cmn.LocalPlacementConf{Weight: requested}
		updated, err := cos.JSON.Marshal(&local)
		tassert.CheckFatal(t, err)
		tassert.CheckFatal(t, os.WriteFile(localPath, updated, 0o600))
		t.Cleanup(func() { tassert.CheckError(t, os.WriteFile(localPath, original, 0o600)) })
	}
	var decommissioned, restoreStarted bool
	t.Cleanup(func() {
		if !decommissioned {
			return
		}
		if !restoreStarted {
			if err := tools.WaitNodePubAddrNotInUse(tsi, time.Minute); err != nil {
				tassert.CheckError(t, err)
				return
			}
			if err := tools.RestoreNode(cmd, false, "target cleanup"); err != nil {
				tassert.CheckError(t, err)
				return
			}
		}
		_, err := tools.WaitForClusterState(proxyURL, "target cleanup", -1, 0, len(weights))
		tassert.CheckError(t, err)
		if err == nil {
			waitRunningRebResil(t, bp)
		}
	})
	actVal := &apc.ActValRmNode{DaemonID: tsi.ID(), RmUserData: true, KeepInitialConfig: true}
	xid, err = membershipRetry(t, func() (string, error) { return tools.DecommissionNode(bp, actVal) })
	tassert.CheckFatal(t, err)
	decommissioned = true
	smap, err = tools.WaitForClusterState(proxyURL, "target decommissioned",
		smap.Version, smap.CountActivePs(), smap.CountTargets()-1, tsi.ID())
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, bp, xid)

	expected := smap.MeanPlacementWeight()
	if requested > 0 {
		expected = requested
	}
	tlog.Logfln("restore %s; expected weight %d (%s)", tsi.StringEx(), expected, smap.StrPlacementWeights())
	err = tools.WaitNodePubAddrNotInUse(tsi, time.Minute)
	tassert.CheckFatal(t, err)
	err = tools.RestoreNode(cmd, false, "target")
	tassert.CheckFatal(t, err)
	restoreStarted = true
	smap, err = tools.WaitForClusterState(proxyURL, "target restored", smap.Version, 0, smap.CountTargets()+1)
	tassert.CheckFatal(t, err)
	waitRunningRebResil(t, bp) // the rejoin rebalance starts with the new Smap
	decommissioned = false

	nsi := smap.GetTarget(tsi.ID())
	tassert.Fatalf(t, nsi != nil, "%s did not rejoin", tsi.StringEx())
	tassert.Fatalf(t, nsi.PlacementWeight() == expected,
		"%s: expected admission weight %d (request %d), got %d", nsi.StringEx(), expected, requested, nsi.PlacementWeight())
	tlog.Logfln("rejoined: %s", smap.StrPlacementWeights())
}
