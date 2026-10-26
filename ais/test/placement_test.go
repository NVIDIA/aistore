// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"maps"
	"math"
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
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

// target placement weights (v5.2); see docs/placement.md
//
// to run all placement tests: `-run=Placement`

type (
	plTest struct {
		t        *testing.T
		bp       api.BaseParams
		proxyURL string
		smap     *meta.Smap // initial: unweighted, all targets active
		bcks     []*plBucket
	}
	plBucket struct {
		m       *ioContext
		prev    map[string]string // object name => target ID, as of the previous verify
		uniform bool              // placement.weighting=uniform
		ec      bool
	}
	plBckOpts struct {
		ecData, ecParity int // EC every object when ecData > 0
		uniform          bool
	}
)

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

// set-bprops applies placement to existing data, including reset, without changing other buckets or Smap
func TestPlacementBucketOverride(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2, Long: true})
	proxyURL := tools.RandomProxyURL(t)
	pt := &plTest{t: t, proxyURL: proxyURL, bp: tools.BaseAPIParams(proxyURL)}
	pt.smap = tools.GetClusterMap(t, proxyURL)
	if pt.smap.IsWeighted() || pt.smap.CountActiveTs() != pt.smap.CountTargets() {
		t.Skip("requires an unweighted cluster with all targets active")
	}
	enableRebalance(t, pt.bp)
	clearWeightsOnExit(t, pt.bp)
	b := pt.addBucket(1000, plBckOpts{})
	other := pt.addBucket(300, plBckOpts{})
	bck, bp := b.m.bck, pt.bp
	set := func(weighting string) (string, error) {
		return api.SetBucketProps(bp, bck, &cmn.BpropsToSet{
			Placement: &cmn.PlacementConfToSet{Weighting: apc.Ptr(weighting)},
		})
	}
	pt.verify(true)
	_, err := set("bogus")
	tassert.Fatalf(t, err != nil, "accepted invalid placement policy")

	// Metadata-only changes with no weights or equal weights do not move data.
	xid, err := set(cmn.WeightingUniform)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid == "", "unweighted policy change started %q", xid)
	b.uniform = true
	pt.setWeights(plWeights(pt.smap, 1), false)
	xid, err = set("")
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid == "", "equal-weight policy change started %q", xid)
	b.uniform = false
	tassert.Fatalf(t, pt.verify(false) == 0, "metadata-only changes moved objects")

	pt.setWeights(seqWeights(pt.smap, 1), true)
	pt.verify(false)
	version := tools.GetClusterMap(t, proxyURL).Version
	bmd, err := api.GetBMD(bp)
	tassert.CheckFatal(t, err)

	// A single update cannot redistribute and also re-mirror, re-EC, or rechunk.
	for _, update := range []*cmn.BpropsToSet{
		{Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(true), Copies: apc.Ptr[int64](2)}},
		{EC: &cmn.ECConfToSet{Enabled: apc.Ptr(true), DataSlices: apc.Ptr(1), ParitySlices: apc.Ptr(1)}},
		{Chunks: &cmn.ChunksConfToSet{ObjSizeLimit: apc.Ptr(cos.SizeIEC(cos.KiB))}},
	} {
		update.Placement = &cmn.PlacementConfToSet{Weighting: apc.Ptr(cmn.WeightingUniform)}
		_, err = api.SetBucketProps(bp, bck, update)
		tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), "multiple layout xactions"), "expected conflicting layouts, got %v", err)
	}
	tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": "false"})
	t.Cleanup(func() { tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": "true"}) })
	_, err = set(cmn.WeightingUniform)
	tassert.Fatalf(t, err != nil, "changed placement while rebalance is disabled")
	tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": "true"})
	after, err := api.GetBMD(bp)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, after.Version == bmd.Version, "rejected requests changed BMD: %d => %d", bmd.Version, after.Version)

	// Continuously read while the policy is published and objects migrate.
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		for ctx.Err() == nil {
			for _, name := range b.m.objNames[:32] {
				_, err := api.GetObjectWithValidation(bp, bck, name, &api.GetArgs{Context: ctx})
				if err != nil {
					if ctx.Err() != nil {
						err = nil
					}
					done <- err
					return
				}
			}
		}
		done <- nil
	}()
	t.Cleanup(func() { cancel(); tassert.CheckError(t, <-done) })

	wait := func(xid string) {
		t.Helper()
		tassert.Fatalf(t, xact.IsValidRebID(xid), "expected rebalance ID, got %q", xid)
		for _, psi := range pt.smap.Pmap {
			statusBP := bp
			statusBP.URL = psi.URL(cmn.NetPublic)
			status, err := api.GetOneXactionStatus(statusBP, &xact.ArgsMsg{Kind: apc.ActRebalance, Bck: bck})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, status.UUID == xid, "bucket status returned %q, expected %q", status.UUID, xid)
		}
		tools.WaitForRebalanceByID(t, bp, xid)
		snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActRebalance})
		tassert.CheckFatal(t, err)
		for _, list := range snaps {
			for _, snap := range list {
				tassert.Fatalf(t, !snap.IsAborted() && snap.Err == "", "rebalance %s failed: %s %s", xid, snap.AbortErr, snap.Err)
			}
		}
		pt.cleanup()
		tassert.Fatalf(t, tools.GetClusterMap(t, proxyURL).Version == version, "bucket policy change modified Smap")
	}
	untouched := maps.Clone(other.prev)
	before := maps.Clone(b.prev)
	xid, err = set(cmn.WeightingUniform)
	tassert.CheckFatal(t, err)
	wait(xid)
	b.uniform = true
	pt.verify(true)
	tassert.Fatalf(t, !maps.Equal(before, b.prev), "uniform policy moved no objects")
	tassert.Fatalf(t, maps.Equal(untouched, other.prev), "redistributed the other bucket")

	xid, err = set(cmn.WeightingUniform)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid == "", "unchanged policy started %q", xid)
	_, err = setPlacementWeights(t, bp, zeroWeights(pt.smap))
	checkHTTPStatus(t, err, http.StatusConflict, "clear weights with bucket override")

	// Reset the placement field, then exercise reset-bprops as well.
	xid, err = set("")
	tassert.CheckFatal(t, err)
	wait(xid)
	b.uniform = false
	pt.verify(true)
	xid, err = set(cmn.WeightingUniform)
	tassert.CheckFatal(t, err)
	wait(xid)
	b.uniform = true
	pt.verify(true)
	xid, err = api.ResetBucketProps(bp, bck)
	tassert.CheckFatal(t, err)
	wait(xid)
	b.uniform = false
	pt.verify(true)
	tassert.Fatalf(t, maps.Equal(untouched, other.prev), "reset redistributed the other bucket")
	props, err := api.HeadBucket(bp, bck, true)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, props.Placement == nil, "reset retained placement override")

	pt.setWeights(zeroWeights(pt.smap), true)
	pt.verify(true)
}

// EC can encode with D+P+1 targets, but migrating its owner needs one more active target
func TestPlacementWeightsECAdmission(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3, Long: true})
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	smap := tools.GetClusterMap(t, proxyURL)
	if smap.IsWeighted() || smap.CountActiveTs() != smap.CountTargets() || smap.CountTargets() > cmn.MaxSliceCount+2 {
		t.Skip("requires an unweighted cluster with all targets active")
	}
	enableRebalance(t, bp)
	clearWeightsOnExit(t, bp)
	bck := cmn.Bck{Name: "placement-ec-" + cos.GenTie(), Provider: apc.AIS}
	tools.CreateBucket(t, proxyURL, bck, &cmn.BpropsToSet{EC: &cmn.ECConfToSet{
		Enabled: apc.Ptr(true), DataSlices: apc.Ptr(smap.CountTargets() - 2), ParitySlices: apc.Ptr(1), ObjSizeLimit: apc.Ptr[int64](0),
	}}, true)

	_, err := setPlacementWeights(t, bp, seqWeights(smap, 1))
	tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), "including a spare"), "accepted EC migration without a spare: %v", err)
	tassert.Fatalf(t, tools.GetClusterMap(t, proxyURL).Version == smap.Version, "rejected weights changed Smap")
	// An override bucket is unaffected by target weights; reject removing that override instead.
	xid, err := api.SetBucketProps(bp, bck, &cmn.BpropsToSet{
		Placement: &cmn.PlacementConfToSet{Weighting: apc.Ptr(cmn.WeightingUniform)},
	})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid == "", "unweighted policy change started %q", xid)
	xid, err = setPlacementWeights(t, bp, seqWeights(smap, 1))
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, bp, xid)
	bmd, err := api.GetBMD(bp)
	tassert.CheckFatal(t, err)
	_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{
		Placement: &cmn.PlacementConfToSet{Weighting: apc.Ptr("")}, Force: true,
	})
	tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), "including a spare"), "accepted EC migration without a spare: %v", err)
	after, err := api.GetBMD(bp)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, after.Version == bmd.Version, "rejected policy change modified BMD")
}

// happy path: with weights 1:2:3, each target owns its share of objects - per HRW, per stored
// location, and per summary measured on the targets; proportional weights move nothing;
// clearing weights restores uniform placement; the uniform-override bucket does not move
func TestPlacementWeightsDistribution(t *testing.T) {
	pt := newPlTest(t)
	pt.addBucket(3000, plBckOpts{})
	uniform := pt.addBucket(500, plBckOpts{uniform: true})
	pt.verify(true)

	weights := plWeights(pt.smap, 1, 2, 3)
	pt.setWeights(weights, true)
	tassert.Fatalf(t, pt.verify(false) > 0, "weights moved no objects")

	pt.setWeights(plScaled(weights, 1<<56), false)
	tassert.Fatalf(t, pt.verify(false) == 0, "proportional weights moved objects")

	pt.resetUniform(uniform)
	pt.setWeights(zeroWeights(pt.smap), true)
	tassert.Fatalf(t, pt.verify(false) > 0, "clearing weights moved no objects")
}

// data-producing jobs write to (and read from) weighted owners
func TestPlacementWeightsJobs(t *testing.T) {
	pt := newPlTest(t)
	src := pt.addBucket(1000, plBckOpts{})
	pt.setWeights(plWeights(pt.smap, 1, 3, 6), true)
	pt.verify(false)
	var (
		m    = src.m
		wait = func(kind, xid string) {
			t.Helper()
			err := api.WaitForXaction(pt.bp, &xact.ArgsMsg{ID: xid, Kind: kind, Timeout: tools.CopyBucketTimeout})
			tassert.CheckFatal(t, err)
		}
		newBck = func(prefix string) cmn.Bck {
			bck := cmn.Bck{Name: prefix + cos.GenTie(), Provider: apc.AIS}
			tools.CreateBucket(t, pt.proxyURL, bck, nil, true /*cleanup*/)
			return bck
		}
	)

	// copy bucket
	cp := cmn.Bck{Name: "pl-cp-" + cos.GenTie(), Provider: apc.AIS}
	tlog.Logfln("copy %s => %s", m.bck.String(), cp.String())
	xid, err := api.CopyBucket(pt.bp, m.bck, cp, &apc.TCBMsg{})
	tassert.CheckFatal(t, err)
	wait(apc.ActCopyBck, xid)
	cpBucket := pt.addJobBucket(cp, m.objNames, m.fileSize)

	// copy objects (list)
	half := m.objNames[:len(m.objNames)/2]
	tco := newBck("pl-tco-")
	tlog.Logfln("copy %d objects %s => %s", len(half), m.bck.String(), tco.String())
	tcomsg := &cmn.TCOMsg{ToBck: tco}
	tcomsg.ListRange.ObjNames = half
	xid, err = api.CopyMultiObj(pt.bp, m.bck, tcomsg)
	tassert.CheckFatal(t, err)
	wait(apc.ActCopyObjects, xid)
	pt.addJobBucket(tco, half, m.fileSize)

	// archive: shards land on the owners of their names
	arch := newBck("pl-arch-")
	shards := make([]string, 0, 20)
	tlog.Logfln("archive %s => %s", m.bck.String(), arch.String())
	for i := range cap(shards) {
		amsg := &cmn.ArchiveBckMsg{ToBck: arch, ArchiveMsg: apc.ArchiveMsg{ArchName: fmt.Sprintf("shard-%02d.tar", i)}}
		amsg.ListRange.ObjNames = m.objNames[i*10 : i*10+10]
		_, err = api.ArchiveMultiObj(pt.bp, m.bck, amsg)
		tassert.CheckFatal(t, err)
		shards = append(shards, amsg.ArchName)
	}
	err = api.WaitForXaction(pt.bp, &xact.ArgsMsg{Kind: apc.ActArchive, Bck: m.bck, Timeout: tools.CopyBucketTimeout})
	tassert.CheckFatal(t, err)
	pt.addJobBucket(arch, shards, 0 /*variable size*/)

	// rename: objects land on the owners of their new unames
	rn := cmn.Bck{Name: "pl-rn-" + cos.GenTie(), Provider: apc.AIS}
	tlog.Logfln("rename %s => %s", cp.String(), rn.String())
	xid, err = api.RenameBucket(pt.bp, cp, rn)
	tassert.CheckFatal(t, err)
	wait(apc.ActMoveBck, xid)
	pt.dropBucket(cpBucket)
	pt.addJobBucket(rn, m.objNames, m.fileSize)

	tassert.Fatalf(t, pt.verify(false) == 0, "jobs moved existing objects")

	// get-batch reads across weighted owners
	req := &apc.MossReq{OnlyObjName: true}
	for _, name := range m.objNames[:200] {
		req.In = append(req.In, apc.MossIn{ObjName: name})
	}
	tlog.Logfln("get-batch %d objects from %s", len(req.In), m.bck.String())
	resp, err := api.GetBatch(pt.bp, m.bck, req, io.Discard)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(resp.Out) == len(req.In), "get-batch: expected %d results, got %d", len(req.In), len(resp.Out))
	for _, out := range resp.Out {
		tassert.Fatalf(t, out.ErrMsg == "" && out.Size == int64(m.fileSize), "get-batch %s: size %d, err %q", out.ObjName, out.Size, out.ErrMsg)
	}

	// EC encode an existing bucket: full replicas on weighted owners (moving an owner needs a spare target)
	if pt.smap.CountActiveTs() < 4 {
		tlog.Logln("skipping EC encode: requires 4 targets")
		return
	}
	ec := pt.addBucket(200, plBckOpts{})
	tlog.Logfln("EC encode %s", ec.m.bck.String())
	xid, err = api.SetBucketProps(pt.bp, ec.m.bck, &cmn.BpropsToSet{EC: &cmn.ECConfToSet{
		Enabled: apc.Ptr(true), DataSlices: apc.Ptr(1), ParitySlices: apc.Ptr(1), ObjSizeLimit: apc.Ptr[int64](0),
	}})
	tassert.CheckFatal(t, err)
	wait(apc.ActECEncode, xid)
	ec.ec = true
	pt.verify(false)
	for _, weighting := range []string{cmn.WeightingUniform, ""} {
		xid, err = api.SetBucketProps(pt.bp, ec.m.bck, &cmn.BpropsToSet{
			Placement: &cmn.PlacementConfToSet{Weighting: apc.Ptr(weighting)},
		})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, xact.IsValidRebID(xid), "expected EC placement rebalance, got %q", xid)
		tools.WaitForRebalanceByID(t, pt.bp, xid)
		snaps, err := api.QueryXactionSnaps(pt.bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActRebalance})
		tassert.CheckFatal(t, err)
		for _, list := range snaps {
			for _, snap := range list {
				tassert.Fatalf(t, !snap.IsAborted() && snap.Err == "", "EC migration failed: %s %s", snap.AbortErr, snap.Err)
			}
		}
		ec.uniform = weighting == cmn.WeightingUniform
		pt.cleanup()
		pt.verify(true)
	}
	before := maps.Clone(ec.prev)
	pt.setWeights(plWeights(pt.smap, 6, 3, 1), true)
	pt.verify(true)
	t.Run("recover-migrated-EC", func(t *testing.T) {
		tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
		initMountpaths(t, pt.proxyURL)
		checked := 0
		for name, owner := range ec.prev {
			if before[name] == owner {
				continue
			}
			parts, _ := ecGetAllSlices(t, ec.m.bck, name)
			removed := 0
			for fqn := range parts {
				ct, err := core.NewCTFromFQN(fqn, nil)
				tassert.CheckFatal(t, err)
				if ct.ContentType() == fs.ObjCT {
					tassert.CheckFatal(t, os.Remove(fqn))
					removed++
				}
			}
			tassert.Fatalf(t, removed > 0, "no full replicas found for migrated %s", name)
			_, err := api.GetObjectWithValidation(pt.bp, ec.m.bck, name, nil)
			tassert.CheckFatal(t, err)
			checked++
			if checked == 4 {
				break
			}
		}
		tassert.Fatalf(t, checked == 4, "expected four migrated EC objects, checked %d", checked)
	})
}

// graceful membership events: maintenance, shutdown and restart, decommission and rejoin
func TestPlacementWeightsMembership(t *testing.T) {
	pt := newPlTest(t)
	pt.addBucket(1000, plBckOpts{})
	pt.addBucket(300, plBckOpts{uniform: true})
	weights := plWeights(pt.smap, 1, 3, 6)
	pt.setWeights(weights, true)
	pt.verify(false)
	var (
		heavy    = pt.smap.GetTarget(plHeaviest(weights))
		nps, nts = pt.smap.CountActivePs(), pt.smap.CountActiveTs()
		actVal   = &apc.ActValRmNode{DaemonID: heavy.ID()}
	)

	// 1. maintenance: the target's objects (including the uniform bucket's) move per the remaining weights - and back
	tlog.Logfln("start maintenance %s (weight %d)", heavy.StringEx(), weights[heavy.ID()])
	xid, err := startMaintenanceRetry(t, pt.bp, actVal)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { tools.ClearMaintenance(pt.bp, heavy) })
	tools.WaitForRebalanceByID(t, pt.bp, xid)
	smap, err := tools.WaitForClusterState(pt.proxyURL, "target in maintenance", pt.smap.Version, nps, nts-1)
	tassert.CheckFatal(t, err)
	pt.cleanup()
	checkPlacementWeights(t, smap, weights)
	tassert.Fatalf(t, pt.verify(true) > 0, "maintenance moved no objects")

	tlog.Logfln("stop maintenance %s", heavy.StringEx())
	xid, err = stopMaintenanceRetry(t, pt.bp, actVal)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(pt.proxyURL, "target is back", smap.Version, nps, nts)
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, pt.bp, xid)
	pt.cleanup()
	checkPlacementWeights(t, smap, weights)
	tassert.Fatalf(t, pt.verify(true) > 0, "returning target moved no objects")

	// 2. graceful shutdown and restart: the target keeps its assigned weight
	_, cmd, xid, err := tools.ShutdownNode(pt.bp, heavy)
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, pt.bp, xid)
	smap, err = tools.WaitForClusterState(pt.proxyURL, "target shut down", smap.Version, nps, nts-1, heavy.ID())
	tassert.CheckFatal(t, err)
	pt.cleanup()
	pt.verify(true)

	tassert.CheckFatal(t, tools.WaitNodePubAddrNotInUse(heavy, time.Minute))
	tassert.CheckFatal(t, tools.RestoreNode(cmd, false, "target"))
	_, err = stopMaintenanceRetry(t, pt.bp, actVal)
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(pt.proxyURL, "target restarted", smap.Version, nps, nts)
	tassert.CheckFatal(t, err)
	pt.settle()
	checkPlacementWeights(t, tools.GetClusterMap(t, pt.proxyURL), weights)
	pt.verify(true)

	// 3. decommission and rejoin: a new admission gets the (rounded) mean weight of active targets
	pt.decommissionRejoin(heavy)
}

// node kills and restarts: proxies (no placement change), target (evicted, then rejoins as a new admission)
func TestPlacementWeightsNodeKill(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinProxies: 3})
	pt := newPlTest(t)
	pt.addBucket(1000, plBckOpts{})
	pt.addBucket(300, plBckOpts{uniform: true})
	weights := plWeights(pt.smap, 1, 3, 6)
	pt.setWeights(weights, true)
	pt.verify(false)
	pt.usePrimary()

	// 1. kill a non-primary proxy: weights unchanged, nothing moves
	smap := tools.GetClusterMap(t, pt.proxyURL)
	psi, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)
	tlog.Logfln("kill %s", psi.StringEx())
	cmd, err := tools.KillNode(pt.bp, psi)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(pt.proxyURL, "proxy killed", smap.Version, smap.CountActivePs()-1, smap.CountActiveTs())
	tassert.CheckFatal(t, err)
	checkPlacementWeights(t, smap, weights)
	tassert.Fatalf(t, pt.verify(false) == 0, "killed proxy moved objects")
	tassert.CheckFatal(t, tools.RestoreNode(cmd, false, "proxy"))
	_, err = tools.WaitForClusterState(pt.proxyURL, "proxy restored", smap.Version, smap.CountActivePs()+1, smap.CountActiveTs())
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, pt.verify(false) == 0, "restored proxy moved objects")

	// 2. kill the primary: weights survive election, and the new primary sets weights
	killRestorePrimary(t, pt.proxyURL, true /*restore as primary*/, func(smap *meta.Smap, newPrimary, _ *meta.Snode) {
		pt.proxyURL = newPrimary.URL(cmn.NetPublic)
		pt.bp = tools.BaseAPIParams(pt.proxyURL)
		checkPlacementWeights(t, smap, weights)
		tassert.Fatalf(t, pt.verify(false) == 0, "primary change moved objects")
		weights = plWeights(pt.smap, 6, 3, 1)
		pt.setWeights(weights, true) // (may be busy until members keepalive the new primary)
		tassert.Fatalf(t, pt.verify(false) > 0, "weights via new primary moved no objects")
	})
	pt.usePrimary()
	tassert.Fatalf(t, pt.verify(false) == 0, "restored primary moved objects")

	// 3. kill the heaviest target: no data until it returns; the restarted target is a new admission
	smap = tools.GetClusterMap(t, pt.proxyURL)
	tsi := smap.GetTarget(plHeaviest(weights))
	nps, nts := smap.CountActivePs(), smap.CountActiveTs()
	tlog.Logfln("kill %s (weight %d)", tsi.StringEx(), weights[tsi.ID()])
	cmd, err = tools.KillNode(pt.bp, tsi)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(pt.proxyURL, "target killed", smap.Version, nps, nts-1, tsi.ID())
	tassert.CheckFatal(t, err)
	waitRunningRebResil(t, pt.bp)
	expected := smap.MeanPlacementWeight()

	tlog.Logfln("restore %s; expected weight %d (%s)", tsi.StringEx(), expected, smap.StrPlacementWeights())
	tassert.CheckFatal(t, tools.RestoreNode(cmd, false, "target"))
	_, err = tools.WaitForClusterState(pt.proxyURL, "target restored", smap.Version, nps, nts)
	tassert.CheckFatal(t, err)
	pt.settle()
	smap = tools.GetClusterMap(t, pt.proxyURL)
	nsi := smap.GetTarget(tsi.ID())
	tassert.Fatalf(t, nsi != nil && nsi.PlacementWeight() == expected, "%s: expected weight %d, got %v", tsi.StringEx(), expected, nsi)
	pt.verify(true)
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
	smap, err = tools.WaitForClusterState(proxyURL, "target restored", smap.Version, 0, len(weights))
	tassert.CheckFatal(t, err)
	waitRunningRebResil(t, bp) // the rejoin rebalance starts with the new Smap
	decommissioned = false

	nsi := smap.GetTarget(tsi.ID())
	tassert.Fatalf(t, nsi != nil, "%s did not rejoin", tsi.StringEx())
	tassert.Fatalf(t, nsi.PlacementWeight() == expected,
		"%s: expected admission weight %d (request %d), got %d", nsi.StringEx(), expected, requested, nsi.PlacementWeight())
	tlog.Logfln("rejoined: %s", smap.StrPlacementWeights())
}

//
// placement helpers
//

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

func plWeight(tsi *meta.Snode, uniform bool) float64 {
	if w := tsi.PlacementWeight(); !uniform && w > 0 {
		return float64(w)
	}
	return 1
}

// (for logging: "t[id]=count", sorted)
func plCounts(counts map[string]int) []string {
	out := make([]string, 0, len(counts))
	for _, tid := range slices.Sorted(maps.Keys(counts)) {
		out = append(out, meta.Tname(tid)+"="+strconv.Itoa(counts[tid]))
	}
	return out
}

// assign the given weights to targets in sorted ID order; the remaining targets get 1
func plWeights(smap *meta.Smap, ws ...int64) map[string]int64 {
	weights := make(map[string]int64, len(smap.Tmap))
	for i, tid := range slices.Sorted(maps.Keys(smap.Tmap)) {
		weights[tid] = 1
		if i < len(ws) {
			weights[tid] = ws[i]
		}
	}
	return weights
}

func plScaled(weights map[string]int64, mult int64) map[string]int64 {
	scaled := make(map[string]int64, len(weights))
	for tid, w := range weights {
		scaled[tid] = w * mult
	}
	return scaled
}

func plHeaviest(weights map[string]int64) string {
	return slices.MaxFunc(slices.Sorted(maps.Keys(weights)), func(a, b string) int { return cmp.Compare(weights[a], weights[b]) })
}

////////////
// plTest //
////////////

func newPlTest(t *testing.T) *plTest {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3, Long: true})
	proxyURL := tools.RandomProxyURL(t)
	pt := &plTest{t: t, bp: tools.BaseAPIParams(proxyURL), proxyURL: proxyURL}
	pt.smap = tools.GetClusterMap(t, proxyURL)
	if pt.smap.IsWeighted() || pt.smap.CountActiveTs() != pt.smap.CountTargets() {
		t.Skipf("requires an unweighted cluster with all targets active: %s", pt.smap)
	}
	enableRebalance(t, pt.bp)
	clearWeightsOnExit(t, pt.bp) // (runs after bucket cleanup)
	return pt
}

// create bucket and PUT num objects
func (pt *plTest) addBucket(num int, opts plBckOpts) *plBucket {
	t := pt.t
	m := &ioContext{t: t, num: num, fileSize: cos.KiB, fixedSize: true, prefix: "pl/"}
	m.initAndSaveState(false)
	var props *cmn.BpropsToSet
	if opts.uniform || opts.ecData > 0 {
		props = &cmn.BpropsToSet{}
	}
	if opts.uniform {
		props.Placement = &cmn.PlacementConfToSet{Weighting: apc.Ptr(cmn.WeightingUniform)}
	}
	if opts.ecData > 0 {
		props.EC = &cmn.ECConfToSet{
			Enabled:      apc.Ptr(true),
			DataSlices:   apc.Ptr(opts.ecData),
			ParitySlices: apc.Ptr(opts.ecParity),
			ObjSizeLimit: apc.Ptr[int64](0),
		}
	}
	tools.CreateBucket(t, pt.proxyURL, m.bck, props, true /*cleanup*/)
	m.puts()
	b := &plBucket{m: m, uniform: opts.uniform, ec: opts.ecData > 0}
	pt.bcks = append(pt.bcks, b)
	return b
}

// add a bucket populated by a job (copy, archive, rename, etc.)
func (pt *plTest) addJobBucket(bck cmn.Bck, objNames []string, fileSize uint64) *plBucket {
	m := &ioContext{t: pt.t, bck: bck, fileSize: fileSize, fixedSize: fileSize > 0}
	m.init(false)
	m.objNames = slices.Clone(objNames)
	m.num = len(m.objNames)
	pt.t.Cleanup(func() { tools.DestroyBucket(pt.t, pt.proxyURL, bck) })
	b := &plBucket{m: m}
	pt.bcks = append(pt.bcks, b)
	return b
}

func (pt *plTest) dropBucket(b *plBucket) {
	pt.bcks = slices.DeleteFunc(pt.bcks, func(x *plBucket) bool { return x == b })
}

// empty the override bucket and reset its policy: clearing target weights is rejected while any bucket has an override
func (pt *plTest) resetUniform(b *plBucket) {
	b.m.del()
	xid, err := api.SetBucketProps(pt.bp, b.m.bck, &cmn.BpropsToSet{Placement: &cmn.PlacementConfToSet{Weighting: apc.Ptr("")}})
	tassert.CheckFatal(pt.t, err)
	tools.WaitForRebalanceByID(pt.t, pt.bp, xid)
	pt.dropBucket(b)
}

// set weights; when relative shares change, wait for rebalance and remove misplaced copies
func (pt *plTest) setWeights(weights map[string]int64, wantReb bool) {
	t := pt.t
	t.Helper()
	tlog.Logfln("set placement weights %v", weights)
	xid, err := setPlacementWeights(t, pt.bp, weights)
	tassert.CheckFatal(t, err)
	if !wantReb {
		tassert.Fatalf(t, xid == "", "unexpected rebalance %q", xid)
		return
	}
	tassert.Fatalf(t, xid != "", "expected rebalance")
	tools.WaitForRebalanceByID(t, pt.bp, xid)
	pt.cleanup()
}

// after a membership change: drain rebalance (and resilver), remove misplaced copies
func (pt *plTest) settle() {
	pt.t.Helper()
	waitRunningRebResil(pt.t, pt.bp)
	pt.cleanup()
}

// rebalance leaves source copies behind; remove them (see reb/cleanup.go)
func (pt *plTest) cleanup() {
	pt.t.Helper()
	for range lcMaxCleanupPasses {
		if lcRunCleanup(pt.t, pt.bp) == 0 {
			return
		}
	}
	pt.t.Fatalf("cleanup did not converge in %d passes", lcMaxCleanupPasses)
}

// verify every bucket; return the number of objects that moved since the previous verify
// (excluding buckets with uniform override - those must not move unless uniformMayMove)
func (pt *plTest) verify(uniformMayMove bool) (moved int) {
	pt.t.Helper()
	smap := tools.GetClusterMap(pt.t, pt.proxyURL)
	smap.InitDigests()
	for _, b := range pt.bcks {
		moved += pt.verifyBucket(b, smap, uniformMayMove)
	}
	return moved
}

func (pt *plTest) verifyBucket(b *plBucket, smap *meta.Smap, uniformMayMove bool) (moved int) {
	var (
		t   = pt.t
		m   = b.m
		bid = m.bck.String()
	)
	t.Helper()
	props, err := api.HeadBucket(pt.bp, m.bck, true /*don't add*/)
	tassert.CheckFatal(t, err)
	if b.ec {
		err = api.WaitForXaction(pt.bp, &xact.ArgsMsg{Kind: apc.ActECPut, Bck: m.bck, Timeout: ECPutTimeOut})
		tassert.CheckFatal(t, err)
	}
	bck := meta.NewBck(m.bck.Name, m.bck.Provider, m.bck.Ns, props)

	// 1. expected owners
	expected := make(map[string]int, smap.CountActiveTs())
	owners := make(map[string]string, len(m.objNames))
	for _, name := range m.objNames {
		tsi, err := smap.HrwObjName2T(bck, name)
		tassert.CheckFatal(t, err)
		expected[tsi.ID()]++
		owners[name] = tsi.ID()
	}

	// 2. stored locations: on the owner, status OK
	msg := &apc.LsoMsg{Props: apc.JoinProps(apc.GetPropsLocation, apc.GetPropsStatus, apc.GetPropsSize)}
	lst, err := api.ListObjects(pt.bp, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == len(m.objNames), "%s: expected %d objects, got %d", bid, len(m.objNames), len(lst.Entries))
	locations := make(map[string]string, len(lst.Entries))
	for _, en := range lst.Entries {
		tassert.Fatalf(t, strings.Contains(en.Location, apc.LocationPropSepa), "%s: missing location for %s", bid, en.Name)
		tname, _ := core.ParseObjLoc(en.Location)
		tid := meta.N2ID(tname)
		tassert.Fatalf(t, tid == owners[en.Name], "%s/%s: stored on %s, expected %s", bid, en.Name, tname, meta.Tname(owners[en.Name]))
		tassert.Fatalf(t, en.Flags&apc.LsoStatusMask == apc.LocOK, "%s/%s: location status %d", bid, en.Name, en.Flags&apc.LsoStatusMask)
		if m.fixedSize {
			tassert.Fatalf(t, en.Size == int64(m.fileSize), "%s/%s: size %d", bid, en.Name, en.Size)
		}
		locations[en.Name] = tid
		if old := b.prev[en.Name]; old != "" && old != tid {
			tassert.Fatalf(t, !b.uniform || uniformMayMove, "%s: uniform override moved %s: %s => %s", bid, en.Name, old, tid)
			if !b.uniform {
				moved++
			}
		}
	}
	b.prev = locations

	// 3. per-target summary (measured on targets) equals expected exactly - no misplaced copies
	// (EC: slices and replicas are not objects; skip)
	if !b.ec {
		ctrl := &apc.BsummCtrlMsg{ObjCached: true, BckPresent: true, PerTarget: true}
		_, summaries, err := api.GetBucketSummary(pt.bp, cmn.QueryBcks(m.bck), ctrl, api.BsummArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(summaries) == 1, "%s: expected one summary, got %d", bid, len(summaries))
		for tid, tsi := range smap.Tmap {
			var have uint64
			if res, ok := summaries[0].Targets[tid]; ok {
				have = res.ObjCount.Present
			}
			tassert.Fatalf(t, have == uint64(expected[tid]), "%s: %s summary has %d objects, expected %d (in maintenance: %t)",
				bid, tsi.StringEx(), have, expected[tid], tsi.InMaintOrDecomm())
		}
	}

	// 4. shares among active targets: a generous statistical bound, independent of HRW
	var sum float64
	for _, tsi := range smap.Tmap {
		if !tsi.InMaintOrDecomm() {
			sum += plWeight(tsi, b.uniform)
		}
	}
	n := float64(len(m.objNames))
	for tid, tsi := range smap.Tmap {
		if tsi.InMaintOrDecomm() {
			tassert.Fatalf(t, expected[tid] == 0, "%s: %s in maintenance owns %d objects", bid, tsi.StringEx(), expected[tid])
			continue
		}
		p := plWeight(tsi, b.uniform) / sum
		limit := 6*math.Sqrt(n*p*(1-p)) + 2
		tassert.Fatalf(t, math.Abs(float64(expected[tid])-n*p) <= limit,
			"%s: %s owns %d objects, expected %.1f +/- %.1f", bid, tsi.StringEx(), expected[tid], n*p, limit)
	}
	tlog.Logfln("%s: %v (uniform: %t, moved: %d)", bid, plCounts(expected), b.uniform, moved)

	// 5. everything reads back
	m.gets(nil, true /*validate*/)
	m.ensureNoGetErrors()
	return moved
}

// decommission a target and restore it: it rejoins as a new admission with the mean weight
func (pt *plTest) decommissionRejoin(tsi *meta.Snode) {
	t := pt.t
	smap := tools.GetClusterMap(t, pt.proxyURL)
	cmd := tools.GetRestoreCmd(tsi)
	var restored bool
	t.Cleanup(func() {
		if restored {
			return
		}
		if err := tools.WaitNodePubAddrNotInUse(tsi, time.Minute); err == nil {
			tassert.CheckError(t, tools.RestoreNode(cmd, false, "target cleanup"))
			_, err = tools.WaitForClusterState(pt.proxyURL, "target cleanup", -1, 0, smap.CountTargets())
			tassert.CheckError(t, err)
			waitRunningRebResil(t, pt.bp)
		}
	})
	tlog.Logfln("decommission %s (weight %d)", tsi.StringEx(), smap.GetTarget(tsi.ID()).PlacementWeight())
	actVal := &apc.ActValRmNode{DaemonID: tsi.ID(), RmUserData: true, KeepInitialConfig: true}
	xid, err := membershipRetry(t, func() (string, error) { return tools.DecommissionNode(pt.bp, actVal) })
	tassert.CheckFatal(t, err)
	nsmap, err := tools.WaitForClusterState(pt.proxyURL, "target decommissioned", smap.Version, smap.CountActivePs(), smap.CountTargets()-1, tsi.ID())
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, pt.bp, xid)
	pt.cleanup()
	pt.verify(true)

	expected := nsmap.MeanPlacementWeight()
	tlog.Logfln("restore %s; expected weight %d (%s)", tsi.StringEx(), expected, nsmap.StrPlacementWeights())
	tassert.CheckFatal(t, tools.WaitNodePubAddrNotInUse(tsi, time.Minute))
	tassert.CheckFatal(t, tools.RestoreNode(cmd, false, "target"))
	restored = true
	_, err = tools.WaitForClusterState(pt.proxyURL, "target restored", nsmap.Version, 0, smap.CountTargets())
	tassert.CheckFatal(t, err)
	pt.settle()
	nsi := tools.GetClusterMap(t, pt.proxyURL).GetTarget(tsi.ID())
	tassert.Fatalf(t, nsi != nil && nsi.PlacementWeight() == expected, "%s: expected admission weight %d, got %v", tsi.StringEx(), expected, nsi)
	tassert.Fatalf(t, pt.verify(true) > 0, "rejoined target moved no objects")
}

// talk to the current primary (node kills must not take down pt.proxyURL)
func (pt *plTest) usePrimary() {
	smap := tools.GetClusterMap(pt.t, pt.proxyURL)
	pt.proxyURL = smap.Primary.URL(cmn.NetPublic)
	pt.bp = tools.BaseAPIParams(pt.proxyURL)
}
