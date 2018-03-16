// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"github.com/OneOfOne/xxhash"
)

const mLCG32 = 1103515245

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)

func hrwTarget(name string, smap *Smap) (si *daemonInfo, errstr string) {
	// NOTE: commented out on purpose - trading off read access to unlocked map
	//       smap.lock.Lock(); defer smap.lock.Unlock()
	if len(smap.Smap) == 0 {
		errstr = "DFC cluster map is empty: no targets"
		return
	}
	var max uint64
	for id, sinfo := range smap.Smap {
		cs := xxhash.ChecksumString64S(id+":"+name, mLCG32)
		if cs > max {
			max = cs
			si = sinfo
		}
	}
	return
}

func hrwMpath(name string) (mpath string) {
	var max uint64
	for path := range ctx.mountpaths {
		cs := xxhash.ChecksumString64S(path+":"+name, mLCG32)
		if cs > max {
			max = cs
			mpath = path
		}
	}
	return
}
