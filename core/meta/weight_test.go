// Package meta_test: unit tests for the package
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package meta_test

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func newWeightedSmap(weights map[string]int64) *meta.Smap {
	smap := &meta.Smap{Tmap: meta.NodeMap{}, Pmap: meta.NodeMap{}}
	for tid, w := range weights {
		tsi := &meta.Snode{}
		tsi.Init(tid, apc.Target, nil)
		tsi.SetPlacementWeight(w)
		smap.Tmap[tid] = tsi
	}
	return smap
}

func TestCheckPlacementWeights(t *testing.T) {
	tests := []struct {
		weights map[string]int64
		ratio   int64
		ok      bool
	}{
		{map[string]int64{"a": 1, "b": 3, "c": 6}, 6, true},
		{map[string]int64{"a": 100 << 30, "b": 300 << 30, "c": 600 << 30}, 6, true},
		{map[string]int64{"a": 0, "b": 0}, 0, true},
		{map[string]int64{"a": 1, "b": 10}, 10, true},
		{map[string]int64{"a": 1, "b": 99}, 99, true},
		{map[string]int64{"a": 1, "b": 100}, 100, false},
		{map[string]int64{"a": 1, "b": 0}, 0, false},
		{map[string]int64{}, 0, false},
		{map[string]int64{"a": 1, "b": 1<<63 - 1}, 1<<63 - 1, false},
		{map[string]int64{"a": 1, "b": -1}, 0, false},
	}
	for _, test := range tests {
		ratio, err := meta.CheckPlacementWeights(test.weights)
		tassert.Errorf(t, (err == nil) == test.ok, "%v: expected ok=%t, got err %v", test.weights, test.ok, err)
		if test.ratio != 0 {
			tassert.Errorf(t, ratio == test.ratio, "%v: expected ratio %d, got %d", test.weights, test.ratio, ratio)
		}
	}
}

func TestSamePlacementWeights(t *testing.T) {
	var (
		none   = newWeightedSmap(map[string]int64{"a": 0, "b": 0, "c": 0})
		equal  = newWeightedSmap(map[string]int64{"a": 3, "b": 3, "c": 3})
		w136   = newWeightedSmap(map[string]int64{"a": 1, "b": 3, "c": 6})
		w2612  = newWeightedSmap(map[string]int64{"a": 2, "b": 6, "c": 12})
		w1312  = newWeightedSmap(map[string]int64{"a": 1, "b": 3, "c": 12})
		capGiB = newWeightedSmap(map[string]int64{"a": 100 << 30, "b": 300 << 30, "c": 600 << 30})
	)
	tassert.Errorf(t, !none.IsWeighted() && equal.IsWeighted(), "IsWeighted")

	tassert.Errorf(t, none.SamePlacementWeights(equal) && equal.SamePlacementWeights(none), "uniform vs all-equal")
	tassert.Errorf(t, w136.SamePlacementWeights(w2612) && w136.SamePlacementWeights(capGiB), "proportional")
	tassert.Errorf(t, !w136.SamePlacementWeights(w1312), "different")
	tassert.Errorf(t, !w136.SamePlacementWeights(none) && !none.SamePlacementWeights(w136), "weighted vs uniform")

	// inactive
	w1312.Tmap["c"].Flags = meta.SnodeMaint
	w2612.Tmap["c"].Flags = meta.SnodeMaint
	tassert.Errorf(t, w1312.SamePlacementWeights(w2612), "inactive")

	// unassigned
	joined := newWeightedSmap(map[string]int64{"a": 1, "b": 3, "c": 6, "d": 0})
	tassert.Errorf(t, !joined.SamePlacementWeights(newWeightedSmap(map[string]int64{"a": 1, "b": 3, "c": 6, "d": 6})),
		"unassigned vs assigned")
}

func TestPlacementWeightJSON(t *testing.T) {
	tsi := &meta.Snode{}
	tsi.Init("t1", apc.Target, nil)

	// not weighted: omitted
	b, err := cos.JSON.Marshal(tsi)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !strings.Contains(string(b), "placement"), "unexpected weight in %s", b)

	for _, weight := range []int64{600 << 30, 9007199254740993, 1<<63 - 1} {
		tsi.SetPlacementWeight(weight)
		b, err = cos.JSON.Marshal(tsi)
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, strings.Contains(string(b), `"weight":"`), "weight must be quoted: %s", b)
		var out meta.Snode
		tassert.CheckFatal(t, cos.JSON.Unmarshal(b, &out))
		tassert.Errorf(t, out.PlacementWeight() == weight, "json: %d != %d", out.PlacementWeight(), weight)
	}
}

func TestMeanPlacementWeight(t *testing.T) {
	const big = 1 << 62
	tests := []struct {
		name    string
		weights map[string]int64
		maint   []string
		mean    int64
	}{
		{"not weighted", map[string]int64{"a": 0, "b": 0}, nil, 0},
		{"1:3:6", map[string]int64{"a": 1, "b": 3, "c": 6}, nil, 3},
		{"2:6:12", map[string]int64{"a": 2, "b": 6, "c": 12}, nil, 6},
		{"GiB", map[string]int64{"a": 100 << 30, "b": 300 << 30, "c": 600 << 30}, nil, 300 << 30},
		{"round half up", map[string]int64{"a": 2, "b": 3}, nil, 3},
		{"no overflow", map[string]int64{"a": big, "b": big, "c": big, "d": big}, nil, big},
		{"128-bit normalized sum", map[string]int64{"a": big, "b": big + 1, "c": big, "d": big + 1}, nil, big + 1},
		{"maint excluded", map[string]int64{"a": 1, "b": 3, "c": 50}, []string{"c"}, 2},
		{"all maint: fallback", map[string]int64{"a": 2, "b": 4}, []string{"a", "b"}, 4},
	}
	for _, test := range tests {
		smap := newWeightedSmap(test.weights)
		for _, tid := range test.maint {
			smap.Tmap[tid].Flags = meta.SnodeMaint
		}
		mean := smap.MeanPlacementWeight()
		tassert.Errorf(t, mean == test.mean, "%s: expected %d, got %d", test.name, test.mean, mean)

		// within [min, max]
		if mean == 0 {
			continue
		}
		var lo, hi int64
		for tid, w := range test.weights {
			if smap.Tmap[tid].InMaintOrDecomm() && len(test.maint) < len(test.weights) {
				continue
			}
			if lo == 0 || w < lo {
				lo = w
			}
			hi = max(hi, w)
		}
		tassert.Errorf(t, lo <= mean && mean <= hi, "%s: mean %d outside [%d, %d]", test.name, mean, lo, hi)
	}
}

func TestPlacementWeightsActionJSON(t *testing.T) {
	for _, weight := range []int64{0, 9007199254740993, 1<<63 - 1} {
		value := apc.ActValPlacementWeights{Weights: map[string]int64{"a": weight}, UUID: "cluster", Version: 42}
		body := cos.MustMarshal(apc.ActMsg{Action: apc.ActSetPlacementWeights, Value: value})
		for range 2 { // receive, forward
			var msg apc.ActMsg
			tassert.CheckFatal(t, cmn.ReadJSON(httptest.NewRecorder(), httptest.NewRequest("PUT", "/v1/cluster", bytes.NewReader(body)), &msg))
			var decoded apc.ActValPlacementWeights
			tassert.CheckFatal(t, cos.MorphMarshal(msg.Value, &decoded))
			tassert.Errorf(t, decoded.Weights["a"] == weight && decoded.UUID == value.UUID && decoded.Version == value.Version,
				"weight %d became %+v", weight, decoded)
			body = cos.MustMarshal(msg)
		}
	}
	for _, value := range []string{`1`, `"1.5"`, `"9223372036854775808"`} {
		var decoded apc.ActValPlacementWeights
		err := cos.JSON.Unmarshal([]byte(`{"weights":{"a":`+value+`},"uuid":"cluster","version":"42"}`), &decoded)
		tassert.Errorf(t, err != nil, "expected invalid quoted integer %s to fail", value)
	}
}
