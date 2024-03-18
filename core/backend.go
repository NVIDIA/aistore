// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"context"
	"io"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
)

type (
	GetReaderResult struct {
		R        io.ReadCloser
		Err      error
		ExpCksum *cos.Cksum
		Size     int64
		ErrCode  int
	}
	LsoInvCtx struct {
		Name   string
		ID     string
		Schema []string
		Offset int64
		Size   int64
	}

	BackendProvider interface {
		Provider() string

		CreateBucket(bck *meta.Bck) (ecode int, err error)
		ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (ecode int, err error)
		ListObjectsInv(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes, ctx *LsoInvCtx) (ecode int, err error)
		ListBuckets(qbck cmn.QueryBcks) (bcks cmn.Bcks, ecode int, err error)
		PutObj(r io.ReadCloser, lom *LOM, origReq *http.Request) (ecode int, err error)
		DeleteObj(lom *LOM) (ecode int, err error)

		// head
		HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, ecode int, err error)
		HeadObj(ctx context.Context, lom *LOM, origReq *http.Request) (objAttrs *cmn.ObjAttrs, ecode int, err error)

		// get
		GetObj(ctx context.Context, lom *LOM, owt cmn.OWT, origReq *http.Request) (ecode int, err error) // calls GetObjReader
		GetObjReader(ctx context.Context, lom *LOM, offset, length int64) GetReaderResult
	}
)
