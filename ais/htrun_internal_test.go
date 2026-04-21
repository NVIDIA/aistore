// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// newHTRunWithSmap creates a minimal htrun with a valid smap containing one proxy node.
func newHTRunWithSmap() (*htrun, string) {
	h := &htrun{}
	primaryID := "p-primary"
	h.si = newSnode(primaryID, apc.Proxy, meta.NetInfo{}, meta.NetInfo{}, meta.NetInfo{})

	config := cmn.GCO.BeginUpdate()
	cmn.GCO.CommitUpdate(config)

	h.owner.smap = newSmapOwner(config)
	smap := newSmap()
	smap.addProxy(h.si)
	smap.Primary = h.si
	h.owner.smap.put(smap)
	return h, primaryID
}

func TestIsClusterNode_KnownNode(t *testing.T) {
	h, primaryID := newHTRunWithSmap()
	hdr := http.Header{}
	hdr.Set(apc.HdrSenderID, primaryID)

	snode, smap := h.isClusterNode(hdr)
	tassert.Errorf(t, snode != nil, "expected known node, got nil")
	tassert.Errorf(t, smap != nil, "expected valid smap, got nil")
	tassert.Errorf(t, snode.ID() == primaryID, "expected ID %q, got %q", primaryID, snode.ID())
}

func TestIsClusterNode_UnknownNode(t *testing.T) {
	h, _ := newHTRunWithSmap()
	hdr := http.Header{}
	hdr.Set(apc.HdrSenderID, "unknown-node-id")

	snode, smap := h.isClusterNode(hdr)
	tassert.Errorf(t, snode == nil, "expected nil for unknown node, got %v", snode)
	tassert.Errorf(t, smap != nil, "expected valid smap, got nil")
}

func TestIsClusterNode_EmptyHeader(t *testing.T) {
	h, _ := newHTRunWithSmap()
	hdr := http.Header{}

	snode, smap := h.isClusterNode(hdr)
	tassert.Errorf(t, snode == nil, "expected nil for empty header, got %v", snode)
	tassert.Errorf(t, smap == nil, "expected nil smap for empty header, got %v", smap)
}

func TestIsClusterNode_InvalidSmap(t *testing.T) {
	h := &htrun{}
	h.si = newSnode("p1", apc.Proxy, meta.NetInfo{}, meta.NetInfo{}, meta.NetInfo{})

	config := cmn.GCO.BeginUpdate()
	cmn.GCO.CommitUpdate(config)

	h.owner.smap = newSmapOwner(config)
	h.owner.smap.put(newSmap())

	hdr := http.Header{}
	hdr.Set(apc.HdrSenderID, "p1")

	snode, smap := h.isClusterNode(hdr)
	tassert.Errorf(t, snode == nil, "expected nil when smap is invalid, got %v", snode)
	tassert.Errorf(t, smap == nil, "expected nil smap when invalid, got %v", smap)
}

func TestIsClusterNode_SpoofedHeaders(t *testing.T) {
	h, _ := newHTRunWithSmap()

	tests := []struct {
		name     string
		callerID string
	}{
		{"arbitrary fake ID", "fakeNodeId42"},
		{"empty-looking ID", ""},
		{"plausible but unknown ID", "p-unknown-proxy"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hdr := http.Header{}
			if tc.callerID != "" {
				hdr.Set(apc.HdrSenderID, tc.callerID)
				hdr.Set(apc.HdrSenderName, "p["+tc.callerID+"]")
			}
			snode, _ := h.isClusterNode(hdr)
			tassert.Errorf(t, snode == nil, "spoofed header %q should not be treated as cluster node", tc.callerID)
		})
	}
}
