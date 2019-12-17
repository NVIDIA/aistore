// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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

var (
	errNoTargets = errors.New("no targets registered in the cluster")
)

type tsi struct {
	node *Snode
	hash uint64
}

// Requires elements of smap.Tmap to have their idDigest initialized
func HrwTarget(bck *Bck, objName string, smap *Smap) (si *Snode, err error) {
	var (
		max    uint64
		uname  = bck.MakeUname(objName)
		digest = xxhash.ChecksumString64S(uname, cmn.MLCG32)
	)
	for _, sinfo := range smap.Tmap {
		// Assumes that sinfo.idDigest is initialized
		cs := xoshiro256.Hash(sinfo.idDigest ^ digest)
		if cs > max {
			max = cs
			si = sinfo
		}
	}
	if si == nil {
		err = errNoTargets
	}
	return
}

// Sorts all targets in a cluster by their respective HRW (weights) in a descending order;
// returns resulting subset (aka slice) that has the requested length = count.
// Returns error if the cluster does not have enough targets.
func HrwTargetList(bck *Bck, objName string, smap *Smap, count int) (si []*Snode, err error) {
	if count <= 0 {
		return nil, fmt.Errorf("invalid number of targets requested: %d", count)
	}
	cnt := smap.CountTargets()
	if cnt < count {
		err = fmt.Errorf("number of targets %d is fewer than requested %d", smap.CountTargets(), count)
		return
	}
	var (
		arr    = make([]tsi, cnt)
		uname  = bck.MakeUname(objName)
		digest = xxhash.ChecksumString64S(uname, cmn.MLCG32)
		i      int
	)
	si = make([]*Snode, count)
	for _, sinfo := range smap.Tmap {
		cs := xoshiro256.Hash(sinfo.idDigest ^ digest)
		arr[i] = tsi{sinfo, cs}
		i++
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].hash > arr[j].hash })
	for i := 0; i < count; i++ {
		si[i] = arr[i].node
	}
	return
}

func HrwProxy(smap *Smap, idToSkip string) (pi *Snode, err error) {
	if smap.CountProxies() == 0 {
		err = errors.New("cluster map is empty: no proxies")
		return
	}
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
		if sinfo.idDigest > max {
			max = sinfo.idDigest
			pi = sinfo
		}
	}
	if pi == nil {
		err = fmt.Errorf("cannot HRW-select proxy: current count=%d, skipped=%d", smap.CountProxies(), skipped)
	}
	return
}

// Returns target which should be responsible for given task. Eg. used when
// it is required to list cloud bucket objects (we want only one target to do it).
func HrwTargetTask(taskID uint64, smap *Smap) (si *Snode, err error) {
	var (
		max uint64
	)
	for _, sinfo := range smap.Tmap {
		// Assumes that sinfo.idDigest is initialized
		cs := xoshiro256.Hash(sinfo.idDigest ^ taskID)
		if cs > max {
			max = cs
			si = sinfo
		}
	}
	if si == nil {
		err = errNoTargets
	}
	return
}

func HrwMpath(bck *Bck, objName string) (mi *fs.MountpathInfo, digest uint64, err error) {
	availablePaths, _ := fs.Mountpaths.Get()
	if len(availablePaths) == 0 {
		err = fmt.Errorf("%s: cannot hrw(%s/%s)", cmn.NoMountpaths, bck.Name, objName)
		return
	}
	var (
		max   uint64
		uname = bck.MakeUname(objName)
	)
	digest = xxhash.ChecksumString64S(uname, cmn.MLCG32)
	for _, mpathInfo := range availablePaths {
		cs := xoshiro256.Hash(mpathInfo.PathDigest ^ digest)
		if cs > max {
			max = cs
			mi = mpathInfo
		}
	}
	return
}
