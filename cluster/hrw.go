// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xoshiro256"
	"github.com/OneOfOne/xxhash"
)

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)

// Returns the target with highest HRW that is "available"(e.g, is not under maintenance).
func HrwTarget(uname string, smap *Smap, inMaintenance ...bool) (si *Snode, err error) {
	var (
		max             uint64
		digest          = xxhash.ChecksumString64S(uname, cos.MLCG32)
		skipMaintenance = true
	)
	if len(inMaintenance) != 0 {
		skipMaintenance = !inMaintenance[0]
	}
	for _, tsi := range smap.Tmap {
		// Assumes that sinfo.idDigest is initialized
		if skipMaintenance && tsi.inMaintenance() {
			continue
		}
		cs := xoshiro256.Hash(tsi.idDigest ^ digest)
		if cs >= max {
			max = cs
			si = tsi
		}
	}
	if si == nil {
		err = cmn.NewNoNodesError(cmn.Target)
	}
	return
}

// Utility struct to generate a list of N first Snodes sorted by their weight
type hrwList struct {
	hs  []uint64
	sis Nodes
	n   int
}

func newHrwList(count int) *hrwList {
	return &hrwList{hs: make([]uint64, 0, count), sis: make(Nodes, 0, count), n: count}
}

// Adds Snode with `weight`. The result is sorted on the fly with insertion sort
// and it makes sure that the length of resulting list never exceeds `count`
func (hl *hrwList) add(weight uint64, sinfo *Snode) {
	l := len(hl.sis)
	if l == hl.n && weight <= hl.hs[l-1] {
		return
	}
	if l == hl.n {
		hl.hs[l-1] = weight
		hl.sis[l-1] = sinfo
	} else {
		hl.hs = append(hl.hs, weight)
		hl.sis = append(hl.sis, sinfo)
		l++
	}
	idx := l - 1
	for idx > 0 && hl.hs[idx-1] < hl.hs[idx] {
		hl.hs[idx], hl.hs[idx-1] = hl.hs[idx-1], hl.hs[idx]
		hl.sis[idx], hl.sis[idx-1] = hl.sis[idx-1], hl.sis[idx]
		idx--
	}
}

func (hl *hrwList) get() Nodes {
	return hl.sis
}

// Sorts all targets in a cluster by their respective HRW (weights) in a descending order;
// returns resulting subset (aka slice) that has the requested length = count.
// Returns error if the cluster does not have enough targets.
// If count == length of Smap.Tmap, the function returns as many targets as possible.
func HrwTargetList(uname string, smap *Smap, count int) (sis Nodes, err error) {
	cnt := smap.CountTargets()
	if cnt < count {
		err = fmt.Errorf("insufficient targets: required %d, available %d, %s", count, cnt, smap)
		return
	}
	digest := xxhash.ChecksumString64S(uname, cos.MLCG32)
	hlist := newHrwList(count)

	for _, tsi := range smap.Tmap {
		cs := xoshiro256.Hash(tsi.idDigest ^ digest)
		if tsi.inMaintenance() {
			continue
		}
		hlist.add(cs, tsi)
	}
	sis = hlist.get()
	if count != cnt && len(sis) < count {
		err = fmt.Errorf("insufficient targets: required %d, available %d, %s", count, len(sis), smap)
		return nil, err
	}
	return sis, nil
}

func HrwProxy(smap *Smap, idToSkip string) (pi *Snode, err error) {
	var max uint64
	for pid, psi := range smap.Pmap {
		if pid == idToSkip {
			continue
		}
		if psi.Flags.IsSet(SnodeNonElectable) {
			continue
		}
		if psi.inMaintenance() {
			continue
		}
		if psi.idDigest >= max {
			max = psi.idDigest
			pi = psi
		}
	}
	if pi == nil {
		err = cmn.NewNoNodesError(cmn.Proxy)
	}
	return
}

func HrwIC(smap *Smap, uuid string) (pi *Snode, err error) {
	var (
		max    uint64
		digest = xxhash.ChecksumString64S(uuid, cos.MLCG32)
	)
	for _, psi := range smap.Pmap {
		if psi.inMaintenance() || !psi.isIC() {
			continue
		}
		cs := xoshiro256.Hash(psi.idDigest ^ digest)
		if cs >= max {
			max = cs
			pi = psi
		}
	}
	if pi == nil {
		err = fmt.Errorf("IC is empty %s: %s", smap, smap.StrIC(nil))
	}
	return
}

// Returns a target for a given task. E.g. usage: list objects in a cloud bucket
// (we want only one target to do it).
func HrwTargetTask(uuid string, smap *Smap) (si *Snode, err error) {
	var (
		max    uint64
		digest = xxhash.ChecksumString64S(uuid, cos.MLCG32)
	)
	for _, tsi := range smap.Tmap {
		if tsi.inMaintenance() {
			continue
		}
		// Assumes that sinfo.idDigest is initialized
		cs := xoshiro256.Hash(tsi.idDigest ^ digest)
		if cs >= max {
			max = cs
			si = tsi
		}
	}
	if si == nil {
		err = cmn.NewNoNodesError(cmn.Target)
	}
	return
}

func HrwMpath(uname string) (mi *fs.MountpathInfo, digest uint64, err error) {
	var (
		max               uint64
		availablePaths, _ = fs.Get()
	)
	if len(availablePaths) == 0 {
		err = fs.ErrNoMountpaths
		return
	}
	digest = xxhash.ChecksumString64S(uname, cos.MLCG32)
	for _, mpathInfo := range availablePaths {
		cs := xoshiro256.Hash(mpathInfo.PathDigest ^ digest)
		if cs >= max {
			max = cs
			mi = mpathInfo
		}
	}
	return
}
