// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xoshiro256"
	"github.com/OneOfOne/xxhash"
)

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)
func Bo2Uname(bucket, objName string) string { return path.Join(bucket, objName) }
func Uname2Bo(uname string) (bucket, objName string) {
	i := strings.Index(uname, "/")
	bucket, objName = uname[:i], uname[i+1:]
	return
}

// Requires elements of smap.Tmap to have their idDigest initialized
func HrwTarget(bucket, objName string, smap *Smap) (si *Snode, err error) {
	var (
		max    uint64
		name   = Bo2Uname(bucket, objName)
		digest = xxhash.ChecksumString64S(name, cmn.MLCG32)
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
		err = errors.New("cluster map is empty: no targets")
	}
	return
}

// Returns count number of first targets with highest random weight. The list
// of targets is sorted from the greatest to least.
// Returns error if the cluster does not have enough targets
func HrwTargetList(bucket, objName string, smap *Smap, count int) (si []*Snode, err error) {
	if count <= 0 {
		return nil, fmt.Errorf("invalid number of targets requested: %d", count)
	}
	if smap.CountTargets() < count {
		err = fmt.Errorf("number of targets %d is fewer than requested %d", smap.CountTargets(), count)
		return
	}

	type tsi struct {
		node *Snode
		hash uint64
	}
	arr := make([]tsi, smap.CountTargets())
	si = make([]*Snode, count)
	name := Bo2Uname(bucket, objName)
	digest := xxhash.ChecksumString64S(name, cmn.MLCG32)

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

func hrwMpath(bucket, objName string) (mi *fs.MountpathInfo, digest uint64, err error) {
	availablePaths, _ := fs.Mountpaths.Get()
	if len(availablePaths) == 0 {
		err = fmt.Errorf("%s: cannot hrw(%s/%s)", cmn.NoMountpaths, bucket, objName)
		return
	}
	var (
		max  uint64
		name = Bo2Uname(bucket, objName)
	)
	digest = xxhash.ChecksumString64S(name, cmn.MLCG32)
	for _, mpathInfo := range availablePaths {
		cs := xoshiro256.Hash(mpathInfo.PathDigest ^ digest)
		if cs > max {
			max = cs
			mi = mpathInfo
		}
	}
	return
}
