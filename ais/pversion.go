// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

// Node-version tracking (primary-side): the verMismatch map records version of any node
// when the former != primary's.
// Rules:
// - absence from the map means: same version as primary (matches are deleted).
// - trust Smap first; entries in verMismatch are not pruned on membership events.
// - empty/nil map == same-version satisfied
// - self-join only (admin-join enforces the version boundary but is not tracked)
// - callers must enforce the version boundary first (see checkNodeVer)

func (p *proxy) noteNodeVersion(nsi *meta.Snode, nverStr string, nversParsed cos.Version) {
	debug.Assert(nverStr != "", nsi) // boundary enforced prior to tracking
	primary := p.primary()
	if nverStr == cmn.VersionAIStore { // exact match (including rc suffix, if exists)
		primary.reg.mtv.Lock()
		if primary.reg.verMismatch != nil {
			delete(primary.reg.verMismatch, nsi.ID())
			if len(primary.reg.verMismatch) == 0 {
				primary.reg.verMismatch = nil
			}
		}
		primary.reg.mtv.Unlock()
		return
	}

	// add and warn
	primary.reg.mtv.Lock()
	if primary.reg.verMismatch == nil {
		primary.reg.verMismatch = make(map[string]string, 4)
	}

	old := primary.reg.verMismatch[nsi.ID()]
	primary.reg.verMismatch[nsi.ID()] = nverStr
	primary.reg.mtv.Unlock()

	if old != nverStr {
		_warnNodeVer(nsi, nverStr, nversParsed)
	}
}

// am primary here
func _warnNodeVer(nsi *meta.Snode, nverStr string, nversParsed cos.Version) {
	selfVer, ok := cos.ParseVersion(cmn.VersionAIStore)
	debug.Assert(ok)

	sname := nsi.StringEx()
	switch {
	case nversParsed.Major > selfVer.Major || (nversParsed.Major == selfVer.Major && nversParsed.Minor > selfVer.Minor):
		// TODO: configurable option to fail the join
		nlog.Errorln(sname, "runs newer version:", nverStr, "[ have:", cmn.VersionAIStore, "]")
	case nversParsed.Major < selfVer.Major || (nversParsed.Major == selfVer.Major && nversParsed.Minor < selfVer.Minor):
		// expected during rolling upgrade
		nlog.Warningln(sname, "runs older version:", nverStr)
	default:
		nlog.Warningln(sname, "runs version:", nverStr, "with a different (release candidate) suffix [ have:", cmn.VersionAIStore, "]")
	}
}

// node 5.x (self) => primary 4.x
func checkPrimVer(sname string, hdr http.Header) error {
	primVer := hdr.Get(apc.HdrNodeVersion)
	reject := enforceVerBoundary(primVer)
	if reject {
		return fmt.Errorf("%s: %s node cannot join via pre-5.0 primary: '%s=%s'\n"+uptip,
			sname, cmn.VersionAIStore, apc.HdrNodeVersion, primVer)
	}
	return nil
}

// joining node => primary (self-join or admin-join)
func checkNodeVer(pname, sname, nversStr string) error {
	reject := enforceVerBoundary(nversStr)
	if reject {
		return fmt.Errorf("%s: cannot join pre-5.0 node %s "+uptip, pname, sname)
	}
	return nil
}

// Version boundary: v5.0 was the mandatory bridge release; 5.1+ refuses pre-5.0 peers in both directions.
// Pre-5.0 nodes do not send apc.HdrNodeVersion - hence, empty is rejected.
// Keep in place to fail a skipped-bridge upgrade (4.x => 5.1+) with a specific error.

const (
	uptip = "(tip: direct upgrade from 4.x to 5.x is not supported; upgrade the cluster to 5.0 first)"
)

func enforceVerBoundary(otherVerStr string) (reject bool) {
	if otherVerStr == "" {
		return true // pre-header => pre-5.0
	}

	other, ok := cos.ParseVersion(otherVerStr)
	if !ok {
		return true // unparseable
	}
	debug.Assert(other.Major >= 5) // apc.HdrNodeVersion was introduced in 5.0
	return other.Major < 5
}
