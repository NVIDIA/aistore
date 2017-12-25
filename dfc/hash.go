/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"hash/crc32"
	"math"

	"github.com/golang/glog"
)

// It will do hash on Normalized Path +Port+ ID and will pick storage server with Max Hash value.
func doHashfindServer(url string) string {
	var sid string
	var max uint32
	for _, smap := range ctx.smap {
		if glog.V(3) {
			glog.Infof("Id = %s Port = %s \n", smap.id, smap.port)
		}
		cs := crc32.Checksum([]byte(url+smap.id+smap.port), crc32.IEEETable)
		if cs > max {
			max = cs
			sid = smap.id
		}
	}
	return sid
}

func doHashfindMountPath(key string) (mpath string) {
	var min uint32 = math.MaxUint32

	assert(len(ctx.mountpaths) > 0, "mp count = 0 (zero)")
	if len(ctx.mountpaths) == 1 {
		mpath = ctx.mountpaths[0].Path
		return
	}
	for _, mountpath := range ctx.mountpaths {
		if !mountpath.Usable {
			continue
		}
		if glog.V(3) {
			glog.Infof("mpath %q key %s", mountpath.Path, key)
		}
		cs := crc32.Checksum([]byte(key+mountpath.Path), crc32.IEEETable)
		if cs < min {
			min = cs
			mpath = mountpath.Path
		}
	}
	return
}
