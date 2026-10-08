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
		status                               int
	}{
		{name: "set", w1: 1, w2: 3},
		{name: "clear", prepare: weighted(1, 3)},
		{name: "same", w1: 1, w2: 3, prepare: weighted(1, 3), noChange: true},
		{name: "proportional", w1: 2, w2: 6, prepare: weighted(1, 3), skipReb: true},
		{name: "inactive", w1: 1, w2: 3, skipReb: true, prepare: func(m *smapX, _ *apc.ActValPlacementWeights) { m.Tmap[plT1].Flags = meta.SnodeMaint }},
		{name: "stale", w1: 1, w2: 3, wantErr: true, status: http.StatusConflict, prepare: func(_ *smapX, v *apc.ActValPlacementWeights) { v.Version-- }},
		{name: "missing-target", w1: 1, w2: 3, wantErr: true, status: http.StatusBadRequest, prepare: func(_ *smapX, v *apc.ActValPlacementWeights) { delete(v.Weights, plT2) }},
		{name: "rebalance-disabled", w1: 1, w2: 3, disabled: true, wantErr: true, status: http.StatusServiceUnavailable},
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
			p.si, p.owner.rmd = m.Primary, &rmdOwner{}
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
	clone.putNode(newTarget(), 0, true)
	if clone.IsWeighted() {
		t.Fatal(clone.StrPlacementWeights())
	}

	// weighted: mean of active targets
	m.Tmap[plT1].SetPlacementWeight(w1)
	m.Tmap[plT2].SetPlacementWeight(w2)
	clone = m.clone()
	clone.putNode(newTarget(), 0, true)
	if w := clone.Tmap[t3].PlacementWeight(); w != (w1+w2)/2 {
		t.Fatalf("expected %d, got %d", (w1+w2)/2, w)
	}

	// re-registration: retains
	nsi := clone.Tmap[t3].Clone()
	nsi.Placement, nsi.PubNet.Hostname = nil, "new-host"
	again := clone.clone()
	again.putNode(nsi, 0, true)
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
	clone = m.clone()
	clone.putNode(newTarget(), 0, true)
	if w := clone.Tmap[t3].PlacementWeight(); w != w1 {
		t.Fatalf("expected %d, got %d", w1, w)
	}
}

func TestPlacementVersionGate(t *testing.T) {
	m := placementTestSmap()
	p := &proxy{prim: &primary{}}
	p.si = m.Primary

	if err := p.checkSameVersion(m); err != nil {
		t.Fatal(err)
	}
	p.prim.reg.verMismatch = map[string]string{"t-gone": "5.0"} // not in Smap
	if err := p.checkSameVersion(m); err != nil {
		t.Fatal(err)
	}
	p.prim.reg.verMismatch[plT2] = "5.0"
	if err := p.checkSameVersion(m); err == nil || !strings.Contains(err.Error(), plT2) {
		t.Fatalf("expected version mismatch for %s, got %v", plT2, err)
	}

	weighted := m.clone()
	weighted.Tmap[plT1].SetPlacementWeight(1)
	weighted.Tmap[plT2].SetPlacementWeight(3)
	for _, test := range []struct {
		smap *smapX
		ver  string
		ok   bool
	}{
		{m, "5.1", true},
		{weighted, "5.2", true},
		{weighted, "6.0", true},
		{weighted, "5.1", false},
		{weighted, "", false},
	} {
		err := checkJoinWeighted("p", "t[new]", test.ver, test.smap)
		if (err == nil) != test.ok {
			t.Fatalf("join %q (weighted=%t): expected ok=%t, got %v", test.ver, test.smap.IsWeighted(), test.ok, err)
		}
	}
}
