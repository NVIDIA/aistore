// Package registry provides core functionality for the AIStore extended actions xreg.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"context"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type ListRangeMsg struct {
	RangeMsg *cmn.RangeMsg
	ListMsg  *cmn.ListMsg
}

func RenewPutArchive(uuid string, t cluster.Target, bckFrom *cluster.Bck) RenewRes {
	return defaultReg.renewPutArchive(uuid, t, bckFrom)
}

func (r *registry) renewPutArchive(uuid string, t cluster.Target, bckFrom *cluster.Bck) RenewRes {
	return r.renewBucketXact(cmn.ActArchive, bckFrom, &XactArgs{T: t, UUID: uuid})
}

func RenewEvictDelete(uuid string, t cluster.Target, kind string, bck *cluster.Bck, msg *ListRangeMsg) RenewRes {
	return defaultReg.renewEvictDelete(uuid, t, kind, bck, msg)
}

func (r *registry) renewEvictDelete(uuid string, t cluster.Target, kind string, bck *cluster.Bck, msg *ListRangeMsg) RenewRes {
	ctx := context.Background()
	return r.renewBucketXact(kind, bck, &XactArgs{Ctx: ctx, T: t, UUID: uuid, Custom: msg})
}

func RenewPrefetch(uuid string, t cluster.Target, bck *cluster.Bck, msg *ListRangeMsg) RenewRes {
	return defaultReg.renewPrefetch(uuid, t, bck, msg)
}

func (r *registry) renewPrefetch(uuid string, t cluster.Target, bck *cluster.Bck, msg *ListRangeMsg) RenewRes {
	ctx := context.Background()
	return r.renewBucketXact(cmn.ActPrefetch, bck, &XactArgs{Ctx: ctx, T: t, UUID: uuid, Custom: msg})
}
