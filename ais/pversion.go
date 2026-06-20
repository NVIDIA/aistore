// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
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

func (p *proxy) noteNodeVersion(nsi *meta.Snode, version string, nodeVer cos.Version) {
	if version == cmn.VersionAIStore { // exact match (including rc suffix, if exists)
		p.reg.mu.Lock()
		if p.reg.verMismatch != nil {
			delete(p.reg.verMismatch, nsi.ID())
			if len(p.reg.verMismatch) == 0 {
				p.reg.verMismatch = nil
			}
		}
		p.reg.mu.Unlock()
		return
	}

	// add and warn
	p.reg.mu.Lock()
	if p.reg.verMismatch == nil {
		p.reg.verMismatch = make(map[string]string, 4)
	}

	old := p.reg.verMismatch[nsi.ID()]
	p.reg.verMismatch[nsi.ID()] = version // keep pre-5.0 "" as-is
	p.reg.mu.Unlock()

	if old != version && version != "" { // empty version tracked but not warned (temp transition)
		p.warnNodeVersion(nsi, version, nodeVer)
	}
}

// TODO: try to skip (expected) warnings during rolling upgrade
func (p *proxy) warnNodeVersion(nsi *meta.Snode, version string, nodeVer cos.Version) {
	primaryVer, ok := cos.ParseVersion(cmn.VersionAIStore)
	debug.Assert(ok)

	sname := nsi.StringEx()
	switch {
	case nodeVer.Major > primaryVer.Major || (nodeVer.Major == primaryVer.Major && nodeVer.Minor > primaryVer.Minor):
		nlog.Errorf("%s: %s runs newer version %q than primary %q", p, sname, version, cmn.VersionAIStore)
	case nodeVer.Major < primaryVer.Major || (nodeVer.Major == primaryVer.Major && nodeVer.Minor < primaryVer.Minor):
		nlog.Warningf("%s: %s runs older version %q than primary %q", p, sname, version, cmn.VersionAIStore)
	default:
		// different (release candidate) suffix
		nlog.Warningf("%s: %s runs version %q, primary runs %q", p, sname, version, cmn.VersionAIStore)
	}
}
