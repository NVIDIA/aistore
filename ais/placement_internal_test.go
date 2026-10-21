// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
)

const (
	plT1 = "t1234567"
	plT2 = "t7654321"
)

func placementTestSmap() *smapX {
	psi := &meta.Snode{}
	psi.Init("p1234567", apc.Proxy, nil)
	m := newSmap()
	m.Primary, m.UUID, m.Version = psi, cos.GenUUID(), 42
	m.Pmap[psi.ID()] = psi
	for _, id := range []string{plT1, plT2} {
		si := &meta.Snode{}
		si.Init(id, apc.Target, nil)
		m.Tmap[id] = si
	}
	return m
}

func plTestProxy() *proxy {
	p := &proxy{prim: &primary{}}
	p.si = &meta.Snode{}
	p.si.Init("p1234567", apc.Proxy, nil)
	return p
}

func plSetVersions(p *proxy, versions map[string]string) {
	p.prim.reg.nodeVersions.Clear()
	for sid, ver := range versions {
		p.prim.reg.nodeVersions.Store(sid, ver)
	}
}

// BMD with a single bucket; uniform: with placement override
func placementTestBMD(uniform bool) *bucketMD {
	bmd := newBucketMD()
	props := &cmn.Bprops{}
	if uniform {
		props.Placement = &cmn.PlacementConf{Weighting: cmn.WeightingUniform}
	}
	bmd.Add(meta.NewBck("bpl", apc.AIS, cmn.NsGlobal, props))
	return bmd
}

func TestPlacementWeightsPre(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	saved := config.Rebalance.Enabled
	cmn.GCO.CommitUpdate(config)
	t.Cleanup(func() {
		config := cmn.GCO.BeginUpdate()
		config.Rebalance.Enabled = saved
		cmn.GCO.CommitUpdate(config)
	})

	weighted := func(w1, w2 int64) func(*smapX, *apc.ActValPlacementWeights) {
		return func(m *smapX, _ *apc.ActValPlacementWeights) {
			m.Tmap[plT1].SetPlacementWeight(w1)
			m.Tmap[plT2].SetPlacementWeight(w2)
		}
	}
	for _, test := range []struct {
		name                                 string
		w1, w2                               int64
		prepare                              func(*smapX, *apc.ActValPlacementWeights)
		wantErr, noChange, skipReb, disabled bool
		override                             bool
		status                               int
	}{
		{name: "set", w1: 1, w2: 3},
		{name: "clear", prepare: weighted(1, 3)},
		{name: "clear-with-override", prepare: weighted(1, 3), override: true, wantErr: true, status: http.StatusConflict},
		{name: "set-with-override", w1: 1, w2: 3, override: true},
		{name: "same", w1: 1, w2: 3, prepare: weighted(1, 3), noChange: true},
		{name: "proportional", w1: 2, w2: 6, prepare: weighted(1, 3), skipReb: true},
		{name: "inactive", w1: 1, w2: 3, skipReb: true, prepare: func(m *smapX, _ *apc.ActValPlacementWeights) { m.Tmap[plT1].Flags = meta.SnodeMaint }},
		{name: "stale", w1: 1, w2: 3, wantErr: true, status: http.StatusConflict, prepare: func(_ *smapX, v *apc.ActValPlacementWeights) { v.Version-- }},
		{name: "missing-target", w1: 1, w2: 3, wantErr: true, status: http.StatusBadRequest, prepare: func(_ *smapX, v *apc.ActValPlacementWeights) { delete(v.Weights, plT2) }},
		{name: "rebalance-disabled", w1: 1, w2: 3, disabled: true, wantErr: true, status: http.StatusServiceUnavailable},
		{name: "single-target", w1: 1, wantErr: true, status: http.StatusBadRequest, prepare: func(m *smapX, v *apc.ActValPlacementWeights) {
			delete(m.Tmap, plT2)
			delete(v.Weights, plT2)
		}},
		{name: "single-target-clear", skipReb: true, prepare: func(m *smapX, v *apc.ActValPlacementWeights) {
			m.Tmap[plT1].SetPlacementWeight(5) // weighted cluster shrunk to one target
			delete(m.Tmap, plT2)
			delete(v.Weights, plT2)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := cmn.GCO.BeginUpdate()
			config.Rebalance.Enabled = !test.disabled
			cmn.GCO.CommitUpdate(config)
			m := placementTestSmap()
			value := &apc.ActValPlacementWeights{UUID: m.UUID, Version: m.Version,
				Weights: map[string]int64{plT1: test.w1, plT2: test.w2}}
			if test.prepare != nil {
				test.prepare(m, value)
			}
			original := m.Tmap[plT1].PlacementWeight()
			p := &proxy{prim: &primary{}}
			plSetVersions(p, map[string]string{plT1: cmn.VersionAIStore, plT2: cmn.VersionAIStore})
			p.si, p.owner.rmd = m.Primary, &rmdOwner{}
			p.owner.bmd = newBMDOwnerPrx(cmn.GCO.Get())
			p.owner.bmd.put(placementTestBMD(test.override))
			p.startup.cluster.Store(1)
			ctx := &smapModifier{pre: p._placementWeightsPre, smap: m,
				msg: &apc.ActMsg{Action: apc.ActSetPlacementWeights, Value: value}}
			clone := m.clone()
			err := ctx.pre(ctx, clone)
			switch {
			case test.noChange:
				if !errors.Is(err, errSmapNoChange) {
					t.Fatalf("expected no-op, got %v", err)
				}
			case (err != nil) != test.wantErr || (test.wantErr && ctx.status != test.status):
				t.Fatalf("error=%v status=%d", err, ctx.status)
			}
			if m.Version != 42 || m.Tmap[plT1].PlacementWeight() != original {
				t.Fatal("original Smap modified")
			}
			if err == nil {
				if clone.Version != 43 || ctx.skipReb != test.skipReb {
					t.Fatalf("version=%d skipReb=%t", clone.Version, ctx.skipReb)
				}
				if got := mustRebalance(ctx, clone); got == test.skipReb {
					t.Fatalf("mustRebalance=%t", got)
				}
			}
		})
	}
}

func TestPlacementWeightPutNode(t *testing.T) {
	const (
		t3     = "t1111111"
		w1, w2 = 100 << 30, 300 << 30
	)
	newTarget := func() *meta.Snode {
		si := &meta.Snode{}
		si.Init(t3, apc.Target, nil)
		si.SetPlacementWeight(12345) // stale
		return si
	}

	// not weighted: none
	m := placementTestSmap()
	clone := m.clone()
	clone.putNode(newTarget(), 0)
	if clone.IsWeighted() {
		t.Fatal(clone.StrPlacementWeights())
	}

	// weighted: putNode leaves a new target unweighted; the caller assigns the mean of active targets
	m.Tmap[plT1].SetPlacementWeight(w1)
	m.Tmap[plT2].SetPlacementWeight(w2)
	clone = m.clone()
	nsi := newTarget()
	w, err := (&clupost{p: plTestProxy(), nsi: nsi}).placementWeight(clone)
	if err != nil || w != (w1+w2)/2 {
		t.Fatalf("expected %d, got %d (%v)", (w1+w2)/2, w, err)
	}
	clone.putNode(nsi, 0)
	if clone.Tmap[t3].PlacementWeight() != 0 {
		t.Fatalf("stale weight retained: %d", clone.Tmap[t3].PlacementWeight())
	}
	nsi.SetPlacementWeight(w)

	// re-registration: retains
	nsi = clone.Tmap[t3].Clone()
	nsi.Placement, nsi.PubNet.Hostname = nil, "new-host"
	again := clone.clone()
	again.putNode(nsi, 0)
	if again.Tmap[t3].PlacementWeight() != (w1+w2)/2 || clone.Tmap[t3].PubNet.Hostname == "new-host" {
		t.Fatal("re-registration lost weight or modified the original")
	}

	// startup: restores
	boot := placementTestSmap()
	boot.UUID = m.UUID
	boot = boot.mergeNodeProps(m)
	if boot.Tmap[plT1].PlacementWeight() != w1 {
		t.Fatal("startup lost weight")
	}

	// maintenance: excluded from the mean
	m.Tmap[plT2].Flags = meta.SnodeMaint
	if w, err := (&clupost{p: plTestProxy(), nsi: newTarget()}).placementWeight(m.clone()); err != nil || w != w1 {
		t.Fatalf("expected %d, got %d (%v)", w1, w, err)
	}
}

func TestPlacementVersionGate(t *testing.T) {
	m := placementTestSmap()
	p := &proxy{prim: &primary{}}
	p.si = m.Primary

	if err := p.checkPlacementVer(m); err == nil || !cmn.IsErrBusy(err) {
		t.Fatalf("expected busy (unknown versions), got %v", err)
	}
	plSetVersions(p, map[string]string{plT1: cmn.VersionAIStore, plT2: cmn.VersionAIStore, "t-gone": "5.0"})
	if err := p.checkPlacementVer(m); err != nil {
		t.Fatal(err)
	}
	p.prim.reg.nodeVersions.Store(plT2, "5.0")
	if err := p.checkPlacementVer(m); err == nil || cmn.IsErrBusy(err) || !strings.Contains(err.Error(), plT2) {
		t.Fatalf("expected version mismatch for %s, got %v", plT2, err)
	}
	// maintenance: version-checked on rejoin
	maint := m.clone()
	maint.Tmap[plT2].Flags = meta.SnodeMaint
	p.prim.reg.nodeVersions.Delete(plT2)
	if err := p.checkPlacementVer(maint); err != nil {
		t.Fatalf("maintenance target must not block: %v", err)
	}

	weighted := m.clone()
	weighted.Tmap[plT1].SetPlacementWeight(1)
	weighted.Tmap[plT2].SetPlacementWeight(3)
	var (
		plain    = placementTestBMD(false)
		override = placementTestBMD(true)
		nsi      = &meta.Snode{}
	)
	nsi.Init("t-new", apc.Target, nil)
	for _, test := range []struct {
		smap *smapX
		bmd  *bucketMD
		ver  string
		ok   bool
	}{
		{m, plain, "5.1", true},
		{weighted, plain, "5.2", true},
		{weighted, plain, "6.0", true},
		{weighted, plain, "5.1", false},
		{weighted, plain, "", false},
		{m, override, "5.2.rc1", true},
		{m, override, "5.1", false},
	} {
		c := &clupost{p: p, nsi: nsi, nversStr: test.ver}
		err := c.checkJoinPlacement(test.smap, test.bmd)
		if (err == nil) != test.ok {
			t.Fatalf("join %q (weighted=%t, bmd=%v): expected ok=%t, got %v",
				test.ver, test.smap.IsWeighted(), test.bmd, test.ok, err)
		}
	}
}

func TestPlacementAdmissionWeight(t *testing.T) {
	m := placementTestSmap()
	m.Tmap[plT1].SetPlacementWeight(100)
	m.Tmap[plT2].SetPlacementWeight(300)
	nsi := &meta.Snode{}
	nsi.Init("t1111111", apc.Target, nil)
	c := &clupost{p: plTestProxy(), nsi: nsi}
	for _, test := range []struct {
		request, want int64
		bad           bool
	}{
		{0, 200, false},
		{500, 500, false},
		{10000, 0, true},
	} {
		c.regReq.JoinWeight = test.request
		w, err := c.placementWeight(m)
		if (err != nil) != test.bad || w != test.want {
			t.Fatalf("request %d: got %d, %v", test.request, w, err)
		}
		if m.GetTarget(nsi.ID()) != nil || nsi.PlacementWeight() != 0 {
			t.Fatal("admission validation mutated membership")
		}
	}
	c.nsi = m.Tmap[plT1].Clone()
	c.regReq.JoinWeight = 999999 // local preference cannot override a retained Smap assignment
	if w, err := c.placementWeight(m); err != nil || w != 100 {
		t.Fatalf("re-registration: %d, %v", w, err)
	}
	plain := placementTestSmap()
	if w, err := c.placementWeight(plain); err != nil || w != 0 {
		t.Fatalf("unweighted cluster: %d, %v", w, err)
	}
}

func TestPlacementStartupWeights(t *testing.T) {
	const t3, t4 = "t3333333", "t4444444"
	newSmap := func(weights ...int64) *smapX {
		m := placementTestSmap()
		for _, id := range []string{t3, t4} {
			si := &meta.Snode{}
			si.Init(id, apc.Target, nil)
			m.Tmap[id] = si
		}
		for i, tid := range []string{plT1, plT2, t3, t4} {
			m.Tmap[tid].SetPlacementWeight(weights[i])
		}
		return m
	}
	newProxy := func(m *smapX) *proxy {
		p := plTestProxy()
		p.si = m.Primary
		p.prim.startingUp = true
		return p
	}
	for _, test := range []struct {
		name     string
		weights  []int64
		want     []int64 // nil: unchanged
		weighted bool
	}{
		{"unweighted", []int64{0, 0, 0, 0}, nil, false},
		{"all weighted", []int64{100, 300, 200, 200}, nil, true},
		{"super-majority: assign mean", []int64{100, 300, 200, 0}, []int64{100, 300, 200, 200}, true},
		{"no super-majority: unweighted", []int64{100, 300, 0, 0}, []int64{0, 0, 0, 0}, false},
		{"invalid ratio: unweighted", []int64{1, 1000, 1, 1}, []int64{0, 0, 0, 0}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := newSmap(test.weights...)
			p := newProxy(m)
			out, _ := p.startupWeights(m)
			if p.prim.startingUp {
				t.Fatal("deferred weights not resolved")
			}
			if test.want == nil {
				if out != m {
					t.Fatalf("unexpected change: %s", out.StrPlacementWeights())
				}
				return
			}
			if out == m || out.Version != m.Version+1 || out.IsWeighted() != test.weighted {
				t.Fatalf("expected new weighted=%t version, got %s v%d", test.weighted, out.StrPlacementWeights(), out.Version)
			}
			for i, tid := range []string{plT1, plT2, t3, t4} {
				if w := out.Tmap[tid].PlacementWeight(); w != test.want[i] {
					t.Fatalf("%s: expected %d, got %d", tid, test.want[i], w)
				}
			}
			if m.Tmap[t4].PlacementWeight() != test.weights[3] {
				t.Fatal("modified the original")
			}
		})
	}

	// weighted: remove pre-5.2 members, keep unobserved ones
	m := newSmap(100, 100, 100, 100)
	p := newProxy(m)
	plSetVersions(p, map[string]string{plT1: cmn.VersionAIStore, plT2: "5.1"})
	if out, _ := p.startupWeights(m); out.GetTarget(plT2) != nil || out.GetTarget(plT1) == nil || out.GetTarget(t3) == nil {
		t.Fatalf("expected %s removed: %s", plT2, out.StringEx())
	}
	// remove pre-5.2 members even when reconciliation falls back to unweighted
	m = newSmap(100, 0, 0, 0)
	p = newProxy(m)
	plSetVersions(p, map[string]string{plT2: "5.1"})
	if out, _ := p.startupWeights(m); out.GetTarget(plT2) != nil || out.IsWeighted() || out.Version <= m.Version {
		t.Fatalf("expected %s removed and weights cleared (new version): %s", plT2, out.StringEx())
	}
	// removing the only weighted member leaves the Smap unweighted (new version)
	m = newSmap(100, 0, 0, 0)
	p = newProxy(m)
	plSetVersions(p, map[string]string{plT1: "5.1"})
	if out, msgs := p.startupWeights(m); out.GetTarget(plT1) != nil || out.IsWeighted() || out.Version <= m.Version || len(msgs) != 1 {
		t.Fatalf("expected %s removed (new version): %s, %v", plT1, out.StringEx(), msgs)
	}

	// deferred admission: Smap assignment, else requested weight; neither mean nor validation
	m = newSmap(100, 300, 0, 0)
	c := &clupost{p: newProxy(m), nsi: m.Tmap[plT1].Clone()}
	c.regReq.JoinWeight = 999999
	if w, err := c.placementWeight(m); err != nil || w != 100 {
		t.Fatalf("assigned: %d, %v", w, err)
	}
	c.nsi = m.Tmap[t3].Clone()
	if w, err := c.placementWeight(m); err != nil || w != 999999 {
		t.Fatalf("requested: %d, %v", w, err)
	}
	c.regReq.JoinWeight = 0
	if w, err := c.placementWeight(m); err != nil || w != 0 {
		t.Fatalf("none: %d, %v", w, err)
	}

	// startup merge: unweighted saved entry keeps the requested weight
	saved := placementTestSmap()
	boot := placementTestSmap()
	boot.Tmap[plT1].SetPlacementWeight(500)
	if out := boot.mergeNodeProps(saved); out.Tmap[plT1].PlacementWeight() != 500 {
		t.Fatalf("requested weight lost: %d", out.Tmap[plT1].PlacementWeight())
	}
}

func TestPlacementJoinRejected(t *testing.T) {
	if !isErrJoinRejected(newErrJoinRejected(nil, errors.New("invalid placement weight"))) {
		t.Fatal("expected join-rejected")
	}
	// any other 400 (e.g., not primary) remains retriable
	if isErrJoinRejected(cmn.NewErrHTTP(nil, errors.New("not primary"), http.StatusBadRequest)) {
		t.Fatal("unexpected join-rejected")
	}
}

func TestPlacementFastKaliveVer(t *testing.T) {
	m := placementTestSmap()
	p := plTestProxy()
	p.si = m.Primary
	p.owner.bmd = newBMDOwnerPrx(cmn.GCO.Get())
	p.owner.bmd.put(placementTestBMD(false))
	si := m.Tmap[plT1]

	r := &http.Request{Header: http.Header{apc.HdrNodeVersion: []string{cmn.VersionAIStore}}}
	if !p.fastKaliveVer(r, si, m) || p.prim.nodeVer(plT1) != cmn.VersionAIStore {
		t.Fatal("expected recorded same-version")
	}
	if !p.fastKaliveVer(r, si, m) { // recorded: read-only path
		t.Fatal("expected same-version accepted")
	}

	// weighted: reject pre-5.2
	m.Tmap[plT1].SetPlacementWeight(1)
	m.Tmap[plT2].SetPlacementWeight(1)
	r.Header.Set(apc.HdrNodeVersion, "5.1")
	if p.fastKaliveVer(r, si, m) {
		t.Fatal("expected pre-5.2 rejected")
	}
}

// startup: a target admitted during startup survives adoption of a newer regpool Smap that omits it
func TestPlacementUpdSnodeAbsent(t *testing.T) {
	adopted := placementTestSmap()
	nsi := &meta.Snode{}
	nsi.Init("t3333333", apc.Target, nil)
	nsi.SetPlacementWeight(300)

	out, cloned := _updSnode(adopted, nsi, false, 0)
	if !cloned || out == adopted || adopted.GetTarget(nsi.ID()) != nil {
		t.Fatal("expected a clone; the adopted Smap must remain unchanged")
	}
	tsi := out.GetTarget(nsi.ID())
	if tsi == nil || tsi == nsi || tsi.PlacementWeight() != 300 {
		t.Fatalf("expected %s added (cloned) with its requested weight, got %v", nsi, tsi)
	}

	// already cloned: no second clone
	other := &meta.Snode{}
	other.Init("t4444444", apc.Target, nil)
	if again, c := _updSnode(out, other, true, 0); again != out || !c || again.GetTarget(other.ID()) == nil {
		t.Fatal("expected in-place add to the existing clone")
	}
}

// startup: Smap assignment wins; otherwise the requested weight
func TestPlacementUpdSnodeWeight(t *testing.T) {
	adopted := placementTestSmap()
	nsi := adopted.Tmap[plT1].Clone()

	// unweighted entry: apply the request
	out, cloned := _updSnode(adopted, nsi, false, 500)
	if !cloned || out == adopted || out.Tmap[plT1].PlacementWeight() != 500 || adopted.Tmap[plT1].PlacementWeight() != 0 {
		t.Fatalf("expected requested weight in a clone: %d", out.Tmap[plT1].PlacementWeight())
	}
	if out.Tmap[plT1] == nsi {
		t.Fatal("pool entry must not be shared with the Smap")
	}

	// assigned entry: keep the assignment
	adopted.Tmap[plT1].SetPlacementWeight(100)
	adopted.Tmap[plT2].SetPlacementWeight(100)
	out, cloned = _updSnode(adopted, nsi, false, 500)
	if cloned || out != adopted || out.Tmap[plT1].PlacementWeight() != 100 {
		t.Fatalf("expected assignment retained: %d", out.Tmap[plT1].PlacementWeight())
	}
}

// startup readiness: a no-op unless placement weights are involved
func TestPlacementWeightsReady(t *testing.T) {
	var (
		plain    = placementTestSmap()
		weighted = placementTestSmap()
	)
	weighted.Tmap[plT1].SetPlacementWeight(100)
	weighted.Tmap[plT2].SetPlacementWeight(100)

	p := plTestProxy()
	if !p.weightsReady(plain, "") {
		t.Fatal("unweighted startup must not wait")
	}
	// pending: weighted Smap (e.g., adopted or discovered) before reconciliation
	if p.weightsReady(weighted, "100") {
		t.Fatal("expected to wait for reconciliation (weighted Smap)")
	}
	// pending: weighted loaded Smap, unweighted working Smap
	p.prim.wver.Store(-1)
	if p.weightsReady(plain, "100") {
		t.Fatal("expected to wait for reconciliation (pending)")
	}
	// reconciled
	p.prim.wver.Store(50)
	for sver, want := range map[string]bool{"": false, "49": false, "50": true, "51": true} {
		if p.weightsReady(plain, sver) != want {
			t.Fatalf("Smap v%q: expected ready=%t", sver, want)
		}
	}
	// cluster started: reconciled by this primary - still requires the reconciled Smap
	p.startup.cluster.Store(1)
	if p.weightsReady(weighted, "49") || !p.weightsReady(weighted, "50") {
		t.Fatal("expected the reconciled-version check after cluster startup")
	}
	// cluster started: not reconciled by this primary (e.g., elected later) - no wait
	p = plTestProxy()
	p.startup.cluster.Store(1)
	if !p.weightsReady(weighted, "") {
		t.Fatal("expected no wait once the cluster has started")
	}
	p.prim.wver.Store(-1)
	if !p.weightsReady(plain, "") {
		t.Fatal("expected no wait once the cluster has started (pending)")
	}
}
