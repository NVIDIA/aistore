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
	ErrNoTargets = errors.New("no targets registered in the cluster")
)

type tsi struct {
	node *Snode
	hash uint64
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
		err = ErrNoTargets
	}
	return
}

// Sorts all targets in a cluster by their respective HRW (weights) in a descending order;
// returns resulting subset (aka slice) that has the requested length = count.
// Returns error if the cluster does not have enough targets.
func HrwTargetList(uname string, smap *Smap, count int) (sis Nodes, err error) {
	cmn.Assert(count > 0)
	cnt := smap.CountTargets()
	if cnt < count {
		err = fmt.Errorf("insufficient targets (%d > %d)", count, smap.CountTargets())
		return
	}
	var (
		arr    = make([]tsi, cnt)
		digest = xxhash.ChecksumString64S(uname, cmn.MLCG32)
		i      int
	)
	sis = make(Nodes, count)
	for _, sinfo := range smap.Tmap {
		cs := xoshiro256.Hash(sinfo.idDigest ^ digest)
		arr[i] = tsi{sinfo, cs}
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
		err = fmt.Errorf("insufficient proxies (%d, %d)", smap.CountProxies(), skipped)
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
		err = ErrNoTargets
	}
	return
}

func HrwMpath(uname string) (mi *fs.MountpathInfo, digest uint64, err error) {
	var (
		max               uint64
		availablePaths, _ = fs.Mountpaths.Get()
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

func HrwIterMatchingObjects(t Target, bck *Bck, template cmn.ParsedTemplate, apply func(lom *LOM) error) error {
	var (
		iter   = template.Iter()
		config = cmn.GCO.Get()
		smap   = t.GetSowner().Get()
	)

	for objName, hasNext := iter(); hasNext; objName, hasNext = iter() {
		lom := &LOM{T: t, ObjName: objName}
		if err := lom.Init(bck.Bck, config); err != nil {
			return err
		}
		si, err := HrwTarget(lom.Uname(), smap)
		if err != nil {
			return err
		}

		if si.ID() != t.Snode().ID() {
			continue
		}
		if err := lom.Load(); err != nil {
			if !cmn.IsObjNotExist(err) {
				return err
			}
		}

		if err := apply(lom); err != nil {
			return err
		}
	}

	return nil
}
