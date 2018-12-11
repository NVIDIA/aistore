// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"

	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/xoshiro256"
	"github.com/OneOfOne/xxhash"
)

const MLCG32 = 1103515245

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)
// FIXME: use path.Join
func Uname(bucket, objname string) string {
	return bucket + "/" + objname
}

func HrwTarget(bucket, objname string, smap *Smap) (si *Snode, errstr string) {
	if smap.CountTargets() == 0 {
		errstr = "cluster map is empty: no targets"
		return
	}
	name := Uname(bucket, objname)
	digest := xxhash.ChecksumString64S(name, MLCG32)
	var max uint64
	for _, sinfo := range smap.Tmap {
		cs := xoshiro256.Hash(sinfo.idDigest ^ digest)
		if cs > max {
			max = cs
			si = sinfo
		}
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

func hrwMpath(bucket, objname string) (mpath string, errstr string) {
	availablePaths, _ := fs.Mountpaths.Get()
	if len(availablePaths) == 0 {
		errstr = fmt.Sprintf("Cannot select mountpath for %s/%s", bucket, objname)
		return
	}

	var max uint64
	name := Uname(bucket, objname)
	digest := xxhash.ChecksumString64S(name, MLCG32)
	for _, mpathInfo := range availablePaths {
		cs := xoshiro256.Hash(mpathInfo.PathDigest ^ digest)
		if cs > max {
			max = cs
			mpath = mpathInfo.Path
		}
	}
	return
}
