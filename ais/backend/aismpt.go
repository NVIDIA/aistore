// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"net/http"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
)

func (m *AISbp) StartMpt(lom *core.LOM, _ *http.Request) (id string, ecode int, err error) {
	var (
		remAis    *remAis
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return "", http.StatusInternalServerError, err
	}
	unsetUUID(&remoteBck)

	uploadID, err := api.CreateMultipartUpload(remAis.bpL, remoteBck, lom.ObjName)
	if err != nil {
		return "", http.StatusInternalServerError, err
	}

	return uploadID, http.StatusOK, err
}

func (m *AISbp) PutMptPart(lom *core.LOM, r cos.ReadOpenCloser, _ *http.Request, uploadID string, size int64, partNum int32) (string, int, error) {
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

	return uploadID, http.StatusOK, nil
}

func (m *AISbp) CompleteMpt(lom *core.LOM, _ *http.Request, uploadID string, _ []byte, parts apc.MptCompletedParts) (version, etag string, _ int, _ error) {
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

	err = api.CompleteMultipartUpload(remAis.bpL, remoteBck, lom.ObjName, uploadID, pns)
	if err != nil {
		return "", "", http.StatusInternalServerError, err
	}

	return "", "", http.StatusOK, nil
}

func (m *AISbp) AbortMpt(lom *core.LOM, _ *http.Request, uploadID string) (ecode int, err error) {
	var (
		remAis    *remAis
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return http.StatusInternalServerError, err
	}
	unsetUUID(&remoteBck)

	err = api.AbortMultipartUpload(remAis.bpL, remoteBck, lom.ObjName, uploadID)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}
