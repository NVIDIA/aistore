// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/cluster"
)

func hrwTarget(bucket, objname string, smap *smapX) (si *cluster.Snode, errstr string) {
	return cluster.HrwTarget(bucket, objname, &smap.Smap)
}

func hrwProxy(smap *smapX, idToSkip string) (pi *cluster.Snode, errstr string) {
	return cluster.HrwProxy(&smap.Smap, idToSkip)
}
