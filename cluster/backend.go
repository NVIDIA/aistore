// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"io"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// TODO: git mv => ais/backend as `backend.Provider` (with t.backend updated accordingly)

type BackendProvider interface {
	Provider() string
	MaxPageSize() uint
	CreateBucket(bck *meta.Bck) (errCode int, err error)
	ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoResult) (errCode int, err error)
	ListBuckets(qbck cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error)
	PutObj(r io.ReadCloser, lom *LOM) (errCode int, err error)
	DeleteObj(lom *LOM) (errCode int, err error)

	// with context
	HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, errCode int, err error)
	HeadObj(ctx context.Context, lom *LOM) (objAttrs *cmn.ObjAttrs, errCode int, err error)
	GetObj(ctx context.Context, lom *LOM, owt cmn.OWT) (errCode int, err error)
	GetObjReader(ctx context.Context, lom *LOM) (r io.ReadCloser, expectedCksum *cos.Cksum, errCode int, err error)
}
