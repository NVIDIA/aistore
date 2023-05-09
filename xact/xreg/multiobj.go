// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
)

func RenewPutArchive(uuid string, t cluster.Target, bckFrom, bckTo *meta.Bck) RenewRes {
	return RenewBucketXact(
		apc.ActArchive,
		bckFrom,
		Args{T: t, UUID: uuid},
		bckFrom, bckTo,
	)
}

func RenewEvictDelete(uuid string, t cluster.Target, kind string, bck *meta.Bck, msg *apc.ListRange) RenewRes {
	return RenewBucketXact(kind, bck, Args{T: t, UUID: uuid, Custom: msg})
}

func RenewPrefetch(uuid string, t cluster.Target, bck *meta.Bck, msg *apc.ListRange) RenewRes {
	return RenewBucketXact(apc.ActPrefetchObjects, bck, Args{T: t, UUID: uuid, Custom: msg})
}
