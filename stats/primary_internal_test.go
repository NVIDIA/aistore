// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core/meta"

	"github.com/prometheus/client_golang/prometheus"
)

func TestPrimaryInfoMetricTracksSmapPrimary(t *testing.T) {
	prevNodeID, hadNodeID := staticLabs[ConstlabNode]
	t.Cleanup(func() {
		if hadNodeID {
			staticLabs[ConstlabNode] = prevNodeID
		} else {
			delete(staticLabs, ConstlabNode)
		}
	})
	staticLabs[ConstlabNode] = "node1"
	sowner := &testSowner{smap: primarySmap("primary1")}

	reg := prometheus.NewRegistry()
	reg.MustRegister(newPrimaryInfo(sowner))

	val, nodeID, primaryID := gatherPrimaryInfo(t, reg)
	if val != 1 {
		t.Fatalf("expected primary1 metric value 1, got %v", val)
	}
	if nodeID != staticLabs[ConstlabNode] {
		t.Fatalf("expected node_id label %q, got %q", staticLabs[ConstlabNode], nodeID)
	}
	if primaryID != "primary1" {
		t.Fatalf("expected primary_id label primary1, got %q", primaryID)
	}

	sowner.smap = primarySmap("primary2")
	val, nodeID, primaryID = gatherPrimaryInfo(t, reg)
	if val != 1 {
		t.Fatalf("expected primary2 metric value 1, got %v", val)
	}
	if nodeID != staticLabs[ConstlabNode] {
		t.Fatalf("expected node_id label %q, got %q", staticLabs[ConstlabNode], nodeID)
	}
	if primaryID != "primary2" {
		t.Fatalf("expected primary_id label primary2, got %q", primaryID)
	}
}

func gatherPrimaryInfo(t *testing.T, reg *prometheus.Registry) (val float64, nodeID, primaryID string) {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, fam := range families {
		if fam.GetName() != "ais_node_primary_info" {
			continue
		}
		ms := fam.GetMetric()
		if len(ms) != 1 {
			t.Fatalf("expected 1 sample, got %d", len(ms))
		}
		m := ms[0]
		g := m.GetGauge()
		if g == nil {
			t.Fatal("expected gauge metric")
		}
		for _, lp := range m.GetLabel() {
			switch lp.GetName() {
			case ConstlabNode:
				nodeID = lp.GetValue()
			case primaryIDLabel:
				primaryID = lp.GetValue()
			}
		}
		if nodeID == "" {
			t.Fatal("expected node_id label")
		}
		if primaryID == "" {
			t.Fatal("expected primary_id label")
		}
		return g.GetValue(), nodeID, primaryID
	}
	t.Fatal("metric ais_node_primary_info not found")
	return 0, "", ""
}

func primarySmap(primaryID string) *meta.Smap {
	primary := &meta.Snode{}
	primary.Init(primaryID, apc.Proxy, nil /*verifying key*/)
	return &meta.Smap{Primary: primary}
}

type testSowner struct {
	smap *meta.Smap
}

func (s *testSowner) Get() *meta.Smap             { return s.smap }
func (*testSowner) Listeners() meta.SmapListeners { return nil }
