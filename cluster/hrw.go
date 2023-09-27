// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
	"github.com/NVIDIA/aistore/fs"
	"github.com/OneOfOne/xxhash"
)

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)

func HrwName2T(uname string, smap *meta.Smap, skipMaint bool) (*meta.Snode, error) {
	digest := xxhash.Checksum64S(cos.UnsafeB(uname), cos.MLCG32)
	return HrwHash2T(digest, smap, skipMaint)
}

func HrwHash2T(digest uint64, smap *meta.Smap, skipMaint bool) (si *meta.Snode, err error) {
	var max uint64
	for _, tsi := range smap.Tmap {
		if skipMaint && tsi.InMaintOrDecomm() {
			continue
		}
		cs := xoshiro256.Hash(tsi.Digest() ^ digest)
		if cs >= max {
			max = cs
			si = tsi
		}
	}
	if si == nil {
		err = cmn.NewErrNoNodes(apc.Target, len(smap.Tmap))
	}
	return
}

func HrwProxy(smap *meta.Smap, idToSkip string) (pi *meta.Snode, err error) {
	var max uint64
	for pid, psi := range smap.Pmap {
		if pid == idToSkip {
			continue
		}
		if psi.Flags.IsSet(meta.SnodeNonElectable) {
			continue
		}
		if psi.InMaintOrDecomm() {
			continue
		}
		if d := psi.Digest(); d >= max {
			max = d
			pi = psi
		}
	}
	if pi == nil {
		err = cmn.NewErrNoNodes(apc.Proxy, len(smap.Pmap))
	}
	return
}

func HrwIC(smap *meta.Smap, uuid string) (pi *meta.Snode, err error) {
	var (
		max    uint64
		digest = xxhash.Checksum64S(cos.UnsafeB(uuid), cos.MLCG32)
	)
	for _, psi := range smap.Pmap {
		if psi.InMaintOrDecomm() || !psi.IsIC() {
			continue
		}
		cs := xoshiro256.Hash(psi.Digest() ^ digest)
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
func HrwTargetTask(uuid string, smap *meta.Smap) (si *meta.Snode, err error) {
	var (
		max    uint64
		digest = xxhash.Checksum64S(cos.UnsafeB(uuid), cos.MLCG32)
	)
	for _, tsi := range smap.Tmap {
		if tsi.InMaintOrDecomm() {
			continue
		}
		// Assumes that sinfo.idDigest is initialized
		cs := xoshiro256.Hash(tsi.Digest() ^ digest)
		if cs >= max {
			max = cs
			si = tsi
		}
	}
	if si == nil {
		err = cmn.NewErrNoNodes(apc.Target, len(smap.Tmap))
	}
	return
}

func HrwMpath(uname string) (mi *fs.Mountpath, digest uint64, err error) {
	var (
		max   uint64
		avail = fs.GetAvail()
	)
	digest = xxhash.Checksum64S(cos.UnsafeB(uname), cos.MLCG32)
	for _, mpathInfo := range avail {
		if mpathInfo.IsAnySet(fs.FlagWaitingDD) {
			continue
		}
		cs := xoshiro256.Hash(mpathInfo.PathDigest ^ digest)
		if cs >= max {
			max = cs
			mi = mpathInfo
		}
	}
	if mi == nil {
		err = cmn.ErrNoMountpaths
	}
	return
}

/////////////
// hrwList //
/////////////

type hrwList struct {
	hs  []uint64
	sis meta.Nodes
	n   int
}

// Sorts all targets in a cluster by their respective HRW (weights) in a descending order;
// returns resulting subset (aka slice) that has the requested length = count.
// Returns error if the cluster does not have enough targets.
// If count == length of Smap.Tmap, the function returns as many targets as possible.

func HrwTargetList(uname string, smap *meta.Smap, count int) (sis meta.Nodes, err error) {
	const fmterr = "%v: required %d, available %d, %s"
	cnt := smap.CountTargets()
	if cnt < count {
		err = fmt.Errorf(fmterr, cmn.ErrNotEnoughTargets, count, cnt, smap)
		return
	}
	digest := xxhash.Checksum64S(cos.UnsafeB(uname), cos.MLCG32)
	hlist := newHrwList(count)

	for _, tsi := range smap.Tmap {
		cs := xoshiro256.Hash(tsi.Digest() ^ digest)
		if tsi.InMaintOrDecomm() {
			continue
		}
		hlist.add(cs, tsi)
	}
	sis = hlist.get()
	if count != cnt && len(sis) < count {
		err = fmt.Errorf(fmterr, cmn.ErrNotEnoughTargets, count, len(sis), smap)
		return nil, err
	}
	return sis, nil
}

func newHrwList(count int) *hrwList {
	return &hrwList{hs: make([]uint64, 0, count), sis: make(meta.Nodes, 0, count), n: count}
}

func (hl *hrwList) get() meta.Nodes { return hl.sis }

// Adds Snode with `weight`. The result is sorted on the fly with insertion sort
// and it makes sure that the length of resulting list never exceeds `count`
func (hl *hrwList) add(weight uint64, sinfo *meta.Snode) {
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
