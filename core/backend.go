// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/memsys"
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
		Lmfh   cos.LomReader
		Lom    *LOM
		SGL    *memsys.SGL
		Name   string
		ID     string
		Schema []string
		Size   int64
		EOF    bool
	}

	Backend interface {
		Provider() string
		MetricName(string) string

		CreateBucket(bck *meta.Bck) (ecode int, err error)
		ListBuckets(qbck cmn.QueryBcks) (bcks cmn.Bcks, ecode int, err error)

		// list-objects
		ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (ecode int, err error)
		ListObjectsInv(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes, ctx *LsoInvCtx) error

		PutObj(ctx context.Context, r io.ReadCloser, lom *LOM, origReq *http.Request) (ecode int, err error)
		DeleteObj(ctx context.Context, lom *LOM) (ecode int, err error)

		// head
		HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, ecode int, err error)
		HeadObj(ctx context.Context, lom *LOM, origReq *http.Request) (objAttrs *cmn.ObjAttrs, ecode int, err error)

		// get (exclusively via GetCold; calls GetObjReader)
		GetObj(ctx context.Context, lom *LOM, owt cmn.OWT, origReq *http.Request) (ecode int, err error)
		// get (jobs; REST)
		GetObjReader(ctx context.Context, lom *LOM, offset, length int64) GetReaderResult

		// bucket inventory
		GetBucketInv(bck *meta.Bck, ctx *LsoInvCtx) (ecode int, err error)

		// multipart upload
		StartMpt(lom *LOM, r *http.Request) (uploadID string, ecode int, err error)
		PutMptPart(lom *LOM, reader cos.ReadOpenCloser, r *http.Request, uploadID string, size int64, partNum int32) (etag string, ecode int, err error)
		CompleteMpt(lom *LOM, r *http.Request, uploadID string, body []byte, parts apc.MptCompletedParts) (version, etag string, ecode int, err error)
		AbortMpt(lom *LOM, r *http.Request, uploadID string) (ecode int, err error)
	}
)
