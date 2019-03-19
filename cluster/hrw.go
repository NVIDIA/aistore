// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"path"
	"sort"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xoshiro256"
	"github.com/OneOfOne/xxhash"
)

const MLCG32 = 1103515245

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)
func Uname(bucket, objname string) string {
	return path.Join(bucket, objname)
}

func HrwTarget(bucket, objname string, smap *Smap) (si *Snode, errstr string) {
	var (
		max    uint64
		name   = Uname(bucket, objname)
		digest = xxhash.ChecksumString64S(name, MLCG32)
	)
	for _, sinfo := range smap.Tmap {
		cs := xoshiro256.Hash(sinfo.idDigest ^ digest)
		if cs > max {
			max = cs
			si = sinfo
		}
	}
	if si == nil {
		errstr = "cluster map is empty: no targets"
	}
	return
}

// Returns count number of first targets with highest random weight. The list
// of targets is sorted from the greatest to least.
// Returns error if the cluster does not have enough targets
func HrwTargetList(bucket, objname string, smap *Smap, count int) (si []*Snode, errstr string) {
	if count <= 0 {
		return nil, fmt.Sprintf("invalid number of targets requested: %d", count)
	}
	if smap.CountTargets() < count {
		errstr = fmt.Sprintf("Number of targets %d is fewer than requested %d", smap.CountTargets(), count)
		return
	}

	type tsi struct {
		node *Snode
		hash uint64
	}
	arr := make([]tsi, len(smap.Tmap))
	si = make([]*Snode, count)
	name := Uname(bucket, objname)
	digest := xxhash.ChecksumString64S(name, MLCG32)

	i := 0
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

func HrwProxy(smap *Smap, idToSkip string) (pi *Snode, errstr string) {
	if smap.CountProxies() == 0 {
		errstr = "cluster map is empty: no proxies"
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
		errstr = fmt.Sprintf("Cannot HRW-select proxy: current count=%d, skipped=%d", smap.CountProxies(), skipped)
	}
	return
}

func hrwMpath(bucket, objname string) (mi *fs.MountpathInfo, errstr string) {
	availablePaths, _ := fs.Mountpaths.Get()
	if len(availablePaths) == 0 {
		errstr = fmt.Sprintf("%s: cannot hrw(%s/%s)", cmn.NoMountpaths, bucket, objname)
		return
	}
	var (
		max    uint64
		name   = Uname(bucket, objname)
		digest = xxhash.ChecksumString64S(name, MLCG32)
	)
	for _, mpathInfo := range availablePaths {
		cs := xoshiro256.Hash(mpathInfo.PathDigest ^ digest)
		if cs > max {
			max = cs
			mi = mpathInfo
		}
	}
	return
}
