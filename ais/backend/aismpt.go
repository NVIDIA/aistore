// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"net/http"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
)

func (m *AISbp) StartMpt(ctx context.Context, lom *core.LOM, _ *http.Request) (id string, ecode int, err error) {
	ctx = mptContext(ctx)
	var (
		remAis    *remAis
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return "", http.StatusInternalServerError, err
	}
	unsetUUID(&remoteBck)

	uploadID, err := api.CreateMultipartUpload(&api.MptArgs{
		Context:    ctx,
		BaseParams: remAis.bpL,
		Bck:        remoteBck,
		ObjName:    lom.ObjName,
	})
	if err != nil {
		return "", http.StatusInternalServerError, err
	}

	return uploadID, http.StatusOK, err
}

func (m *AISbp) PutMptPart(ctx context.Context, lom *core.LOM, r cos.ReadOpenCloser, _ *http.Request, uploadID string, size int64, partNum int32) (string, int, error) {
	ctx = mptContext(ctx)
	var (
		remAis    *remAis
		remoteBck = lom.Bck().Clone()
		err       error
	)
	remAis, err = m.getRemAis(remoteBck.Ns.UUID)
	if err != nil {
		cos.Close(r)
		return "", http.StatusInternalServerError, err
	}
	unsetUUID(&remoteBck)

	err = api.UploadPart(&api.PutPartArgs{
		PutArgs: api.PutArgs{
			Context:    ctx,
			BaseParams: remAis.bpL,
			Bck:        remoteBck,
			ObjName:    lom.ObjName,
			Reader:     r,
			Size:       uint64(size),
		},
		PartNumber: int(partNum),
		UploadID:   uploadID,
	})
	if err != nil {
		return "", http.StatusInternalServerError, err
	}

	// The remote AIS native UploadPart API exposes no part ETag;
	// leave it empty so the caller can generate the S3 ETag.
	return "", http.StatusOK, nil
}

func (m *AISbp) CompleteMpt(ctx context.Context, lom *core.LOM, _ *http.Request, uploadID string, _ []byte, parts apc.MptCompletedParts) (version, etag string, _ int, _ error) {
	ctx = mptContext(ctx)
	var (
		remAis    *remAis
		remoteBck = lom.Bck().Clone()
		err       error
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return "", "", http.StatusInternalServerError, err
	}
	unsetUUID(&remoteBck)

	pns := make([]int, len(parts))
	for i, part := range parts {
		pns[i] = part.PartNumber
	}

	err = api.CompleteMultipartUpload(&api.CompleteMptArgs{
		MptArgs: api.MptArgs{
			Context:    ctx,
			BaseParams: remAis.bpL,
			Bck:        remoteBck,
			ObjName:    lom.ObjName,
		},
		UploadID:    uploadID,
		PartNumbers: pns,
	})
	if err != nil {
		return "", "", http.StatusInternalServerError, err
	}

	return "", "", http.StatusOK, nil
}

func (m *AISbp) AbortMpt(ctx context.Context, lom *core.LOM, _ *http.Request, uploadID string) (ecode int, err error) {
	ctx = mptContext(ctx)
	var (
		remAis    *remAis
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return http.StatusInternalServerError, err
	}
	unsetUUID(&remoteBck)

	err = api.AbortMultipartUpload(&api.AbortMptArgs{
		MptArgs: api.MptArgs{
			Context:    ctx,
			BaseParams: remAis.bpL,
			Bck:        remoteBck,
			ObjName:    lom.ObjName},
		UploadID: uploadID,
	})
	if err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}
