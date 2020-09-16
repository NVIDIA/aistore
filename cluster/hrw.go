// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"
	"sort"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xoshiro256"
	"github.com/OneOfOne/xxhash"
)

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)

type (
	NoNodesError struct {
		role string
		smap *Smap
		skip string
	}
)

func (e *NoNodesError) Error() string {
	var skip string
	if e.skip != "" {
		skip = fmt.Sprintf(", skip=%s", e.skip)
	}
	if e.role == cmn.Proxy {
		return fmt.Sprintf("no available proxies, %s%s", e.smap.StringEx(), skip)
	}
	return fmt.Sprintf("no available targets, %s%s", e.smap.StringEx(), skip)
}

// Requires elements of smap.Tmap to have their idDigest initialized
func HrwTarget(uname string, smap *Smap) (si *Snode, err error) {
	var (
		max    uint64
		digest = xxhash.ChecksumString64S(uname, cmn.MLCG32)
	)
	for _, sinfo := range smap.Tmap {
		// Assumes that sinfo.idDigest is initialized
		cs := xoshiro256.Hash(sinfo.idDigest ^ digest)
		if cs >= max {
			max = cs
			si = sinfo
		}
	}
	if si == nil {
		err = &NoNodesError{cmn.Target, smap, ""}
	}
	return
}

// Sorts all targets in a cluster by their respective HRW (weights) in a descending order;
// returns resulting subset (aka slice) that has the requested length = count.
// Returns error if the cluster does not have enough targets.
//
// TODO: avoid allocating `arr`
func HrwTargetList(uname string, smap *Smap, count int) (sis Nodes, err error) {
	cnt := smap.CountTargets()
	if cnt < count {
		err = fmt.Errorf("insufficient targets: required %d, available %d, %s", count, cnt, smap)
		return
	}
	var (
		arr = make([]struct {
			node *Snode
			hash uint64
		}, cnt)
		digest = xxhash.ChecksumString64S(uname, cmn.MLCG32)
		i      int
	)
	sis = make(Nodes, count)
	for _, sinfo := range smap.Tmap {
		cs := xoshiro256.Hash(sinfo.idDigest ^ digest)
		arr[i] = struct {
			node *Snode
			hash uint64
		}{sinfo, cs}
		i++
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].hash > arr[j].hash })
	for i := 0; i < count; i++ {
		sis[i] = arr[i].node
	}
	return
}

func HrwProxy(smap *Smap, idToSkip string) (pi *Snode, err error) {
	var (
		max     uint64
		skipped int
	)
	for id, sinfo := range smap.Pmap {
		if id == idToSkip {
			skipped++
			continue
		}
		if _, ok := smap.NonElects[id]; ok {
			skipped++
			continue
		}
		if sinfo.idDigest >= max {
			max = sinfo.idDigest
			pi = sinfo
		}
	}
	if pi == nil {
		err = &NoNodesError{cmn.Proxy, smap, idToSkip}
	}
	return
}

func HrwIC(smap *Smap, uuid string) (pi *Snode, err error) {
	var (
		max    uint64
		digest = xxhash.ChecksumString64S(uuid, cmn.MLCG32)
	)
	for pid := range smap.IC {
		psi := smap.GetProxy(pid)
		cs := xoshiro256.Hash(psi.idDigest ^ digest)
		if cs >= max {
			max = cs
			pi = psi
		}
	}
	if pi == nil {
		err = fmt.Errorf("IC is empty %v, %s", smap.IC, smap)
	}
	return
}

// Returns target which should be responsible for a given task. E.g. usage
// to list cloud bucket (we want only one target to do it).
func HrwTargetTask(uuid string, smap *Smap) (si *Snode, err error) {
	var (
		max    uint64
		digest = xxhash.ChecksumString64S(uuid, cmn.MLCG32)
	)
	for _, sinfo := range smap.Tmap {
		// Assumes that sinfo.idDigest is initialized
		cs := xoshiro256.Hash(sinfo.idDigest ^ digest)
		if cs >= max {
			max = cs
			si = sinfo
		}
	}
	if si == nil {
		err = &NoNodesError{cmn.Target, smap, ""}
	}
	return
}

func HrwMpath(uname string) (mi *fs.MountpathInfo, digest uint64, err error) {
	var (
		max               uint64
		availablePaths, _ = fs.Get()
	)
	if len(availablePaths) == 0 {
		err = errors.New(cmn.NoMountpaths)
		return
	}
	digest = xxhash.ChecksumString64S(uname, cmn.MLCG32)
	for _, mpathInfo := range availablePaths {
		cs := xoshiro256.Hash(mpathInfo.PathDigest ^ digest)
		if cs >= max {
			max = cs
			mi = mpathInfo
		}
	}
	return
}
