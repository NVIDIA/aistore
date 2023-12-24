// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core/meta"
)

func RenewPutArchive(bckFrom, bckTo *meta.Bck) RenewRes {
	return RenewBucketXact(apc.ActArchive, bckFrom, Args{Custom: bckTo}, bckFrom, bckTo)
}

func RenewEvictDelete(uuid, kind string, bck *meta.Bck, msg *apc.ListRange) RenewRes {
	return RenewBucketXact(kind, bck, Args{UUID: uuid, Custom: msg})
}

func RenewPrefetch(uuid string, bck *meta.Bck, msg *apc.ListRange) RenewRes {
	return RenewBucketXact(apc.ActPrefetchObjects, bck, Args{UUID: uuid, Custom: msg})
}

// kind: (apc.ActCopyObjects | apc.ActETLObjects)
func RenewTCObjs(kind string, custom *TCObjsArgs) RenewRes {
	return RenewBucketXact(kind, custom.BckFrom, Args{Custom: custom}, custom.BckFrom, custom.BckTo)
}
