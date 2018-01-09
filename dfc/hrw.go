/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"github.com/OneOfOne/xxhash"
)

const LCG32 = 1103515245

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)

// NOTE: read access to Smap - see sync.Map comment
func hrwTarget(name string) (sid string) {
	var max uint32
	for id, _ := range ctx.smap.Smap {
		cs := xxhash.ChecksumString32S(name+id, LCG32)
		if cs > max {
			max = cs
			sid = id
		}
	}
	return
}
func hrwMpath(name string) (mpath string) {
	var max uint32
	for path, _ := range ctx.mountpaths {
		cs := xxhash.ChecksumString32S(name+path, LCG32)
		if cs > max {
			max = cs
			mpath = path
		}
	}
	return
}
