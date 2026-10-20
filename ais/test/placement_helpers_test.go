// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"cmp"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

// placement weights with data: shared helpers (see placement_test.go)

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

// empty the override bucket and reset its policy: clearing target weights is rejected while any bucket
// has an override (and redistribution on override change is pending)
func (pt *plTest) resetUniform(b *plBucket) {
	b.m.del()
	_, err := api.SetBucketProps(pt.bp, b.m.bck, &cmn.BpropsToSet{Placement: &cmn.PlacementConfToSet{Weighting: apc.Ptr("")}})
	tassert.CheckFatal(pt.t, err)
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
