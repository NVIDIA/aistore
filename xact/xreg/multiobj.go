// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

func RenewPutArchive(uuid string, t cluster.Target, bckFrom *cluster.Bck) RenewRes {
	return RenewBucketXact(apc.ActArchive, bckFrom, Args{T: t, UUID: uuid})
}

func RenewEvictDelete(uuid string, t cluster.Target, kind string, bck *cluster.Bck, msg *cmn.SelectObjsMsg) RenewRes {
	return RenewBucketXact(kind, bck, Args{T: t, UUID: uuid, Custom: msg})
}

func RenewPrefetch(uuid string, t cluster.Target, bck *cluster.Bck, msg *cmn.SelectObjsMsg) RenewRes {
	return RenewBucketXact(apc.ActPrefetchObjects, bck, Args{T: t, UUID: uuid, Custom: msg})
}
