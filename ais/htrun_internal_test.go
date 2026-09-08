// Package ais: internal unit tests
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

func isClusterNode(h *htrun, hdr http.Header) (*meta.Snode, *smapX) {
	sid := hdr.Get(apc.HdrSenderID)
	if sid == "" {
		return nil, nil
	}
	smap := h.owner.smap.get()
	if !smap.isValid() {
		return nil, nil
	}
	return smap.GetNode(sid), smap
}

// newHTRunWithSmap creates a minimal htrun with a valid smap containing one proxy node.
func newHTRunWithSmap() (*htrun, string) {
	h := &htrun{}
	primaryID := "p-primary"
	h.si = newSnode(primaryID, apc.Proxy, meta.NetInfo{}, meta.NetInfo{}, meta.NetInfo{})

	config := cmn.GCO.BeginUpdate()
	cmn.GCO.CommitUpdate(config)

	h.owner.smap = newSmapOwner(config, false /*isTarget*/)
	smap := newSmap()
	smap.addProxy(h.si)
	smap.Primary = h.si
	h.owner.smap.put(smap)
	h.svs.init()
	return h, primaryID
}

func TestIsClusterNode_KnownNode(t *testing.T) {
	h, primaryID := newHTRunWithSmap()
	hdr := http.Header{}
	hdr.Set(apc.HdrSenderID, primaryID)

	snode, smap := isClusterNode(h, hdr)
	tassert.Errorf(t, snode != nil, "expected known node, got nil")
	tassert.Errorf(t, smap != nil, "expected valid smap, got nil")
	tassert.Errorf(t, snode.ID() == primaryID, "expected ID %q, got %q", primaryID, snode.ID())
}

func TestIsClusterNode_UnknownNode(t *testing.T) {
	h, _ := newHTRunWithSmap()
	hdr := http.Header{}
	hdr.Set(apc.HdrSenderID, "unknown-node-id")

	snode, smap := isClusterNode(h, hdr)
	tassert.Errorf(t, snode == nil, "expected nil for unknown node, got %v", snode)
	tassert.Errorf(t, smap != nil, "expected valid smap, got nil")
}

func TestIsClusterNode_EmptyHeader(t *testing.T) {
	h, _ := newHTRunWithSmap()
	hdr := http.Header{}

	snode, smap := isClusterNode(h, hdr)
	tassert.Errorf(t, snode == nil, "expected nil for empty header, got %v", snode)
	tassert.Errorf(t, smap == nil, "expected nil smap for empty header, got %v", smap)
}

func TestIsClusterNode_InvalidSmap(t *testing.T) {
	h := &htrun{}
	h.si = newSnode("p1", apc.Proxy, meta.NetInfo{}, meta.NetInfo{}, meta.NetInfo{})

	config := cmn.GCO.BeginUpdate()
	cmn.GCO.CommitUpdate(config)

	h.owner.smap = newSmapOwner(config, false /*isTarget*/)
	h.owner.smap.put(newSmap())
	h.svs.init()

	hdr := http.Header{}
	hdr.Set(apc.HdrSenderID, "p1")

	snode, smap := isClusterNode(h, hdr)
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
			snode, _ := isClusterNode(h, hdr)
			tassert.Errorf(t, snode == nil, "spoofed header %q should not be treated as cluster node", tc.callerID)
		})
	}
}

func TestSetIntraHdrs_ClearsRelayedSenderHdrs(t *testing.T) {
	spoofed := func() http.Header {
		hdr := http.Header{}
		hdr.Set(apc.HdrSenderID, "p-spoofed")
		hdr.Set(apc.HdrSenderName, "p[spoofed]")
		hdr.Set(apc.HdrSenderIsPrimary, "true")
		hdr.Set(apc.HdrSenderSmapVer, "99999")
		hdr.Set(apc.HdrSenderNonce, "42")
		hdr.Set(apc.HdrSenderSig, "not-a-signature")
		return hdr
	}

	// self is NOT primary; signing disabled and grace expired (svState.init),
	// so signIntra is skipped - exactly the window where a relayed signature would survive
	h, _ := newHTRunWithSmap()
	tassert.Fatalf(t, !h.svs.sign(), "expecting sign() == false")

	smap := h.owner.smap.get().clone()
	other := newSnode("p-other", apc.Proxy, meta.NetInfo{}, meta.NetInfo{}, meta.NetInfo{})
	smap.addProxy(other)
	smap.Primary = other
	smap.Version++
	h.owner.smap.put(smap)
	smap = h.owner.smap.get()

	req := &http.Request{Header: spoofed()}
	h.setIntraHdrs(req, smap, true /*peer present*/)

	for _, tc := range []struct{ hdr, expected string }{
		{apc.HdrSenderID, h.SID()},
		{apc.HdrSenderName, h.si.Name()},
		{apc.HdrSenderIsPrimary, ""},
		{apc.HdrSenderSmapVer, smap.vstr},
		{apc.HdrSenderNonce, ""},
		{apc.HdrSenderSig, ""},
	} {
		got := req.Header.Get(tc.hdr)
		tassert.Errorf(t, got == tc.expected, "%s: got %q, expected %q", tc.hdr, got, tc.expected)
	}

	// and the legitimate primary case is not broken by the above
	hp, _ := newHTRunWithSmap()
	reqp := &http.Request{Header: spoofed()}
	hp.setIntraHdrs(reqp, hp.owner.smap.get(), true /*peer present*/)
	got := reqp.Header.Get(apc.HdrSenderIsPrimary)
	tassert.Errorf(t, got == "true", "%s: got %q, expected %q", apc.HdrSenderIsPrimary, got, "true")
}
