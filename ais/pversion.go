// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"slices"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

// Node-version tracking (primary-side): record the version of every observed node.
// Rules:
// - absence means unknown, including after primary reelection;
// - trust Smap first; entries are not pruned on membership events;
// - self-join, admin-join, and slow keepalive: recorded under the Smap lock once accepted;
// - fast keepalive: recorded when changed (lock-free);
// - callers must enforce the version boundary first (see checkNodeVer).

// Version boundary: v5.0 was the mandatory bridge release; 5.1+ refuses pre-5.0 peers in both directions.
// Pre-5.0 nodes do not send apc.HdrNodeVersion - hence, empty is rejected.
// Keep in place to fail a skipped-bridge upgrade (4.x => 5.1+) with a specific error.
const (
	uptip = "(tip: direct upgrade from 4.x to 5.x is not supported; upgrade the cluster to " + cmn.MinVerNodeJoin + " first)"
)

func (p *proxy) noteNodeVersion(nsi *meta.Snode, nverStr string, nversParsed cos.Version) {
	debug.Assert(nverStr != "", nsi) // boundary enforced prior to tracking
	old, _ := p.primary().reg.nodeVersions.Swap(nsi.ID(), nverStr)
	if old != nverStr && nverStr != cmn.VersionAIStore {
		_warnNodeVer(nsi, nverStr, nversParsed)
	}
}

// return recorded version or empty string if unknown
func (pr *primary) nodeVer(sid string) string {
	if v, ok := pr.reg.nodeVersions.Load(sid); ok {
		return v.(string)
	}
	return ""
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

func enforceVerBoundary(otherVerStr string) (reject bool) {
	return !cos.VersionAtLeast(otherVerStr, cmn.MinVerNodeJoin)
}

// reject placement updates until every Smap member has reported the primary's version
// - unknown only (e.g., right after primary reelection): busy - retry once members keepalive
func (p *proxy) checkPlacementVer(smap *smapX, action string) error {
	var (
		differ  []string
		unknown []string
		primary = p.primary()
	)
	for _, nodes := range []meta.NodeMap{smap.Pmap, smap.Tmap} {
		for sid, si := range nodes {
			// maintenance: version-checked on rejoin (see checkJoinPlacement)
			if sid == p.SID() || si.InMaintOrDecomm() {
				continue
			}
			switch ver := primary.nodeVer(sid); ver {
			case cmn.VersionAIStore:
			case "":
				unknown = append(unknown, si.StringEx())
			default:
				differ = append(differ, si.StringEx()+"("+ver+")")
			}
		}
	}
	switch {
	case len(differ) > 0:
		for _, name := range unknown {
			differ = append(differ, name+"(unknown)")
		}
		slices.Sort(differ)
		return fmt.Errorf("%s: cannot %s: node versions differ %v (primary: %s) - complete the upgrade",
			p, action, differ, cmn.VersionAIStore)
	case len(unknown) > 0:
		slices.Sort(unknown)
		return cmn.NewErrBusy(membershipTag, action,
			fmt.Sprintf("node versions not yet known: %v", unknown))
	}
	return nil
}

// placement in use: weighted Smap or bucket placement override(s)
func placementInUse(smap *smapX, bmd *bucketMD) bool {
	return smap.IsWeighted() || (bmd != nil && bmd.GetBucketWithPlacement() != nil)
}

// reject pre-5.2 joiner when the cluster uses placement: weighted Smap or bucket override(s)
func (c *clupost) checkJoinPlacement(smap *smapX, bmd *bucketMD) error {
	if cos.VersionAtLeast(c.nversStr, cmn.MinVerPlacement) {
		return nil
	}
	var what string
	if smap.IsWeighted() {
		what = "placement weights"
	} else if bck := bmd.GetBucketWithPlacement(); bck != nil {
		what = "bucket " + bck.Cname("") + " placement override"
	} else {
		return nil
	}
	return fmt.Errorf("%s: cannot join %s (version %q) - cluster has %s that requires version %s or later",
		c.p, c.nsi.StringEx(), c.nversStr, what, cmn.MinVerPlacement)
}
