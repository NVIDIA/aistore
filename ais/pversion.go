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
// - empty "" is stored on purpose, not skipped: a pre-5.0 node sends no Ais-Node-Version
// - trust Smap first; entries in verMismatch are not pruned on membership events.
// - empty/nil map == same-version satisfied
// - self-join is supported, admin-join is not
// First intended usage: transition 4.7 => 5.0

func (p *proxy) noteNodeVersion(nsi *meta.Snode, nverStr string, nversParsed cos.Version) {
	if nverStr == cmn.VersionAIStore { // exact match (including rc suffix, if exists)
		p.reg.mtv.Lock()
		if p.reg.verMismatch != nil {
			delete(p.reg.verMismatch, nsi.ID())
			if len(p.reg.verMismatch) == 0 {
				p.reg.verMismatch = nil
			}
		}
		p.reg.mtv.Unlock()
		return
	}

	// add and warn
	p.reg.mtv.Lock()
	if p.reg.verMismatch == nil {
		p.reg.verMismatch = make(map[string]string, 4)
	}

	old := p.reg.verMismatch[nsi.ID()]
	p.reg.verMismatch[nsi.ID()] = nverStr // keep pre-5.0 "" as-is
	p.reg.mtv.Unlock()

	if old != nverStr && nverStr != "" { // empty nverStr tracked but not warned (4.x => 5.x transition)
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

//
// 5.0 boundary enforcement - both ways
//

const (
	uptip = "(tip: direct upgrade from 4.x to 5.x is not supported; upgrade the cluster to 5.0 first)"
)

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

// node 4.x => primary 5.x (self)
func checkNodeVer(pname, sname, nversStr string) error {
	reject := enforceVerBoundary(nversStr)
	if reject {
		return fmt.Errorf("%s: cannot join pre-5.0 node %s "+uptip, pname, sname)
	}
	return nil
}

// 5.0 is the mandatory bridge: permissive both ways so a 4.x<=>5.0 rolling
// upgrade can complete. From 5.1 on, a pre-5.0 peer is refused in both
// directions.
func enforceVerBoundary(otherVerStr string) (reject bool) {
	selfVer, ok := cos.ParseVersion(cmn.VersionAIStore)
	debug.Assert(ok)
	debug.Assert(selfVer.Major >= 5)

	if selfVer.Major == 5 && selfVer.Minor == 0 {
		return false
	}
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
