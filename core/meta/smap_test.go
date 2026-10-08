// Package meta_test: unit tests for the package
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package meta_test

import (
	"bytes"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
)

const (
	cmpSelfID = "self-id"
	cmpPeerID = "peer-id"
)

func TestCompareTargets(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(old, current *meta.Smap)
		want   bool // same targets
	}{
		{name: "same-version", mutate: func(_, current *meta.Smap) { current.Version-- }, want: true},
		{name: "proxy-only-version-bump", want: true},
		{
			name: "proportional-weights",
			mutate: func(old, current *meta.Smap) {
				old.Tmap[cmpSelfID].SetPlacementWeight(1)
				old.Tmap[cmpPeerID].SetPlacementWeight(3)
				current.Tmap[cmpSelfID].SetPlacementWeight(2)
				current.Tmap[cmpPeerID].SetPlacementWeight(6)
			},
			want: true,
		},
		{
			name: "equal-weights-vs-none",
			mutate: func(_, current *meta.Smap) {
				current.Tmap[cmpSelfID].SetPlacementWeight(5)
				current.Tmap[cmpPeerID].SetPlacementWeight(5)
			},
			want: true,
		},
		{
			name:   "weight-changed",
			mutate: func(_, current *meta.Smap) { current.Tmap[cmpPeerID].SetPlacementWeight(30) },
		},
		{
			name:   "target-added",
			mutate: func(_, current *meta.Smap) { current.Tmap["new-peer"] = cmpSnode("new-peer", 3) },
		},
		{
			name:   "target-removed",
			mutate: func(_, current *meta.Smap) { delete(current.Tmap, cmpPeerID) },
		},
		{
			name:   "maintenance",
			mutate: func(_, current *meta.Smap) { current.Tmap[cmpPeerID].Flags = meta.SnodeMaint },
		},
		{
			name: "rebalanced-out",
			mutate: func(_, current *meta.Smap) {
				current.Tmap[cmpPeerID].Flags = meta.SnodeMaint | meta.SnodeMaintPostReb
			},
		},
		{
			name: "rebalanced-in",
			mutate: func(old, _ *meta.Smap) {
				old.Tmap[cmpPeerID].Flags = meta.SnodeMaint | meta.SnodeMaintPostReb
			},
		},
		{
			name:   "restarted",
			mutate: func(_, current *meta.Smap) { current.Tmap[cmpPeerID].VerifyingKey = cmpKey(2) },
		},
		{
			name:   "verifying-key-added", // pre-5.1 peer restarting into 5.1
			mutate: func(old, _ *meta.Smap) { old.Tmap[cmpPeerID].VerifyingKey = nil },
		},
		{
			name: "both-verifying-keys-empty",
			mutate: func(old, current *meta.Smap) {
				old.Tmap[cmpPeerID].VerifyingKey = nil
				current.Tmap[cmpPeerID].VerifyingKey = nil
			},
			want: true,
		},
		{
			name:   "data-endpoint-changed",
			mutate: func(_, current *meta.Smap) { current.Tmap[cmpPeerID].DataNet.Hostname = "peer-data-new" },
		},
		{
			name:   "control-endpoint-changed",
			mutate: func(_, current *meta.Smap) { current.Tmap[cmpPeerID].ControlNet.Hostname = "peer-ctrl-new" },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			old := cmpSmap(10)
			current := cmpClone(old)
			current.Version++
			if test.mutate != nil {
				test.mutate(old, current)
			}
			if got := old.CompareTargets(current); got != test.want {
				t.Fatalf("CompareTargets() = %t, want %t", got, test.want)
			}
		})
	}
}

func cmpSmap(version int64) *meta.Smap {
	self, peer := cmpSnode(cmpSelfID, 1), cmpSnode(cmpPeerID, 1)
	return &meta.Smap{
		Version: version,
		Tmap:    meta.NodeMap{self.ID(): self, peer.ID(): peer},
	}
}

func cmpSnode(id string, key byte) *meta.Snode {
	return &meta.Snode{
		DaeID:        id,
		DaeType:      apc.Target,
		VerifyingKey: cmpKey(key),
		PubNet:       meta.NetInfo{Hostname: id + "-pub", Port: "51081"},
		ControlNet:   meta.NetInfo{Hostname: id + "-ctrl", Port: "51081"},
		DataNet:      meta.NetInfo{Hostname: id + "-data", Port: "51081"},
	}
}

func cmpKey(b byte) []byte { return bytes.Repeat([]byte{b}, cos.NodeSigningPublicKeySize) }

func cmpClone(src *meta.Smap) *meta.Smap {
	dst := &meta.Smap{Version: src.Version, Tmap: make(meta.NodeMap, len(src.Tmap))}
	for id, si := range src.Tmap {
		clone := *si
		clone.VerifyingKey = bytes.Clone(si.VerifyingKey)
		dst.Tmap[id] = &clone
	}
	return dst
}
