// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"github.com/NVIDIA/dfcpub/cluster"
)

func hrwTarget(bucket, objname string, smap *smapX) (si *cluster.Snode, errstr string) {
	return cluster.HrwTarget(bucket, objname, &smap.Smap)
}

func hrwProxy(smap *smapX, idToSkip string) (pi *cluster.Snode, errstr string) {
	return cluster.HrwProxy(&smap.Smap, idToSkip)
}
