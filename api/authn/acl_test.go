// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package authn_test

import (
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
)

func TestMergeClusterACLsUnion(t *testing.T) {
	to := []*authn.CluACL{{ID: "c1", Access: apc.AccessAttrs(0b0100)}}
	from := []*authn.CluACL{{ID: "c1", Access: apc.AccessAttrs(0b0010)}}
	got := authn.MergeClusterACLs(to, from, "", true)
	if len(got) != 1 {
		t.Fatalf("union: want 1 element, got %d", len(got))
	}
	if got[0].Access != 0b0110 {
		t.Fatalf("union: want 0b0110, got %b", got[0].Access)
	}
}

func TestMergeClusterACLsReplace(t *testing.T) {
	to := []*authn.CluACL{{ID: "c1", Access: apc.AccessAttrs(0b0100)}}
	from := []*authn.CluACL{{ID: "c1", Access: apc.AccessAttrs(0b0010)}}
	got := authn.MergeClusterACLs(to, from, "", false)
	if len(got) != 1 {
		t.Fatalf("replace: want 1 element, got %d", len(got))
	}
	if got[0].Access != 0b0010 {
		t.Fatalf("replace: want 0b0010, got %b", got[0].Access)
	}
}

func TestMergeClusterACLsFilter(t *testing.T) {
	to := []*authn.CluACL{}
	from := []*authn.CluACL{
		{ID: "c1", Access: apc.AccessAttrs(1)},
		{ID: "c2", Access: apc.AccessAttrs(2)},
	}
	got := authn.MergeClusterACLs(to, from, "c1", false)
	if len(got) != 1 || got[0].ID != "c1" {
		t.Fatalf("filter: want [c1], got %v", got)
	}
}

func TestMergeBckACLsUnion(t *testing.T) {
	bck := cmn.Bck{Name: "b", Provider: "ais"}
	to := []*authn.BckACL{{Bck: bck, Access: apc.AccessAttrs(0b0100)}}
	from := []*authn.BckACL{{Bck: bck, Access: apc.AccessAttrs(0b0010)}}
	got := authn.MergeBckACLs(to, from, "", true)
	if len(got) != 1 {
		t.Fatalf("union: want 1 element, got %d", len(got))
	}
	if got[0].Access != 0b0110 {
		t.Fatalf("union: want 0b0110, got %b", got[0].Access)
	}
}

func TestMergeBckACLsFilter(t *testing.T) {
	b1 := cmn.Bck{Name: "b1", Provider: "ais", Ns: cmn.Ns{UUID: "c1"}}
	b2 := cmn.Bck{Name: "b2", Provider: "ais", Ns: cmn.Ns{UUID: "c2"}}
	to := []*authn.BckACL{}
	from := []*authn.BckACL{
		{Bck: b1, Access: apc.AccessAttrs(1)},
		{Bck: b2, Access: apc.AccessAttrs(2)},
	}
	got := authn.MergeBckACLs(to, from, "c1", false)
	if len(got) != 1 || got[0].Bck.Name != "b1" {
		t.Fatalf("filter: want [b1], got %v", got)
	}
}
