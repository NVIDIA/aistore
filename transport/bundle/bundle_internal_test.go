// Package bundle: unit tests for transport stream bundles.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"bytes"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
)

const testPeerID = "peer-id"

func TestStreamsStale(t *testing.T) {
	selfID := mock.NewTarget(nil).SID()

	tests := []struct {
		name     string
		mutate   func(old, current *meta.Smap)
		wantCtrl bool
		wantData bool
	}{
		{name: "proxy-only-version-bump"},
		{
			name: "target-added",
			mutate: func(_, current *meta.Smap) {
				current.Tmap["new-peer"] = testSnode("new-peer", 3)
			},
			wantCtrl: true,
			wantData: true,
		},
		{
			name: "target-removed",
			mutate: func(_, current *meta.Smap) {
				delete(current.Tmap, testPeerID)
			},
			wantCtrl: true,
			wantData: true,
		},
		{
			name: "target-rebalanced-out",
			mutate: func(_, current *meta.Smap) {
				current.Tmap[testPeerID].Flags = meta.SnodeMaint | meta.SnodeMaintPostReb
			},
			wantCtrl: true,
			wantData: true,
		},
		{
			name: "target-rebalanced-in",
			mutate: func(old, _ *meta.Smap) {
				old.Tmap[testPeerID].Flags = meta.SnodeMaint | meta.SnodeMaintPostReb
			},
			wantCtrl: true,
			wantData: true,
		},
		{
			name: "maintenance-without-post-rebalance",
			mutate: func(_, current *meta.Smap) {
				current.Tmap[testPeerID].Flags = meta.SnodeMaint
			},
		},
		{
			name: "verifying-key-changed",
			mutate: func(_, current *meta.Smap) {
				current.Tmap[testPeerID].VerifyingKey = testKey(2)
			},
			wantCtrl: true,
			wantData: true,
		},
		{
			name: "verifying-key-added",
			mutate: func(old, _ *meta.Smap) {
				old.Tmap[testPeerID].VerifyingKey = nil
			},
			wantCtrl: true,
			wantData: true,
		},
		{
			name: "both-verifying-keys-empty",
			mutate: func(old, current *meta.Smap) {
				old.Tmap[testPeerID].VerifyingKey = nil
				current.Tmap[testPeerID].VerifyingKey = nil
			},
		},
		{
			name: "control-endpoint-changed",
			mutate: func(_, current *meta.Smap) {
				current.Tmap[testPeerID].ControlNet.URL = "http://control-new:8080"
			},
			wantCtrl: true,
		},
		{
			name: "data-endpoint-changed",
			mutate: func(_, current *meta.Smap) {
				current.Tmap[testPeerID].DataNet.URL = "http://data-new:8081"
			},
			wantData: true,
		},
		{
			name: "public-endpoint-changed",
			mutate: func(_, current *meta.Smap) {
				current.Tmap[testPeerID].PubNet.URL = "http://public-new:8082"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			old := testSmap(1, selfID)
			current := cloneTestSmap(old)
			current.Version++
			if test.mutate != nil {
				test.mutate(old, current)
			}

			for _, network := range []struct {
				name string
				net  string
				want bool
			}{
				{name: "control", net: cmn.NetIntraControl, want: test.wantCtrl},
				{name: "data", net: cmn.NetIntraData, want: test.wantData},
			} {
				t.Run(network.name, func(t *testing.T) {
					sb := &Streams{smap: old, network: network.net}
					if got := sb.Stale(current); got != network.want {
						t.Fatalf("Stale(%s) = %t, want %t", network.net, got, network.want)
					}
				})
			}
		})
	}
}

func testSmap(version int64, selfID string) *meta.Smap {
	self := testSnode(selfID, 1)
	peer := testSnode(testPeerID, 1)
	return &meta.Smap{
		Version: version,
		Tmap: meta.NodeMap{
			self.ID(): self,
			peer.ID(): peer,
		},
	}
}

func testSnode(id string, key byte) *meta.Snode {
	return &meta.Snode{
		DaeID:        id,
		DaeType:      apc.Target,
		VerifyingKey: testKey(key),
		PubNet:       meta.NetInfo{URL: "http://public:8082"},
		ControlNet:   meta.NetInfo{URL: "http://control:8080"},
		DataNet:      meta.NetInfo{URL: "http://data:8081"},
	}
}

func testKey(b byte) []byte { return bytes.Repeat([]byte{b}, cos.NodeSigningPublicKeySize) }

func cloneTestSmap(src *meta.Smap) *meta.Smap {
	dst := &meta.Smap{Version: src.Version, Tmap: make(meta.NodeMap, len(src.Tmap))}
	for id, si := range src.Tmap {
		clone := *si
		clone.VerifyingKey = bytes.Clone(si.VerifyingKey)
		dst.Tmap[id] = &clone
	}
	return dst
}
