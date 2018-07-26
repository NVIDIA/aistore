// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"

	"github.com/OneOfOne/xxhash"
)

const mLCG32 = 1103515245

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)
func uniquename(bucket, objname string) string {
	return bucket + "/" + objname
}

func HrwTarget(bucket, objname string, smap *Smap) (si *daemonInfo, errstr string) {
	if smap.countTargets() == 0 {
		errstr = "DFC cluster map is empty: no targets"
		return
	}
	name := uniquename(bucket, objname)
	var max uint64
	for id, sinfo := range smap.Tmap {
		cs := xxhash.ChecksumString64S(id+":"+name, mLCG32)
		if cs > max {
			max = cs
			si = sinfo
		}
	}
	return
}

func HrwProxy(smap *Smap, idToSkip string) (pi *daemonInfo, errstr string) {
	if smap.countProxies() == 0 {
		errstr = "DFC cluster map is empty: no proxies"
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
		cs := xxhash.ChecksumString64S(id, mLCG32)
		if cs > max {
			max = cs
			pi = sinfo
		}
	}
	if pi == nil {
		errstr = fmt.Sprintf("Cannot HRW-select proxy: current count=%d, skipped=%d", smap.countProxies(), skipped)
	}
	return
}

func hrwMpath(bucket, objname string) (mpath string) {
	var max uint64
	name := uniquename(bucket, objname)
	availablePaths, _ := ctx.mountpaths.Mountpaths()
	for _, mpathInfo := range availablePaths {
		cs := xxhash.ChecksumString64S(mpathInfo.Path+":"+name, mLCG32)
		if cs > max {
			max = cs
			mpath = mpathInfo.Path
		}
	}
	return
}
