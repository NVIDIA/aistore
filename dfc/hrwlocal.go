// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"

	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/xoshiro256"
	"github.com/OneOfOne/xxhash"
)

const MLCG32 = 1103515245

func hrwTarget(bucket, objname string, smap *SmapX) (si *cluster.Snode, errstr string) {
	return cluster.HrwTarget(bucket, objname, &smap.Smap)
}

func hrwProxy(smap *SmapX, idToSkip string) (pi *cluster.Snode, errstr string) {
	return cluster.HrwProxy(&smap.Smap, idToSkip)
}

func hrwMpath(bucket, objname string) (mpath string, errstr string) {
	availablePaths, _ := ctx.mountpaths.Mountpaths()
	if len(availablePaths) == 0 {
		errstr = fmt.Sprintf("Cannot select mountpath for %s/%s", bucket, objname)
		return
	}

	var max uint64
	name := cluster.Uname(bucket, objname)
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
