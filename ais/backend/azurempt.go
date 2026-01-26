//go:build azure

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
)

// azureBlockIDFmt defines the format used to generate a unique block ID for each part of a multipart upload to Azure Blob Storage.
// It combines the uploadID (string) and the part number (zero-padded to 6 digits), e.g., "upload-000001".
const azureBlockIDFmt = "%s-%06d"

// Azure uses Block Blobs for multipart uploads:
// - StartMpt: No explicit "start" - just begin staging blocks
// - PutMptPart: StageBlock with a block ID
// - CompleteMpt: CommitBlockList with all block IDs
// - AbortMpt: No explicit abort - uncommitted blocks are garbage collected after 7 days

// azureBlockID generates a base64-encoded block ID from uploadID and part number
// Azure requires block IDs to be base64 encoded strings
func azureBlockID(uploadID string, partNum int) string {
	return base64.StdEncoding.EncodeToString(fmt.Appendf(nil, azureBlockIDFmt, uploadID, partNum))
}

func (*azbp) StartMpt(lom *core.LOM, _ *http.Request) (id string, ecode int, err error) {
	// Azure doesn't have an explicit "start multipart upload" operation.
	// Multipart upload is initiated by staging blocks, and we use a generated
	// upload ID to track the block IDs for this upload session.
	uploadID := cos.GenUUID() // Generate a unique ID for this multipart upload session

	if cmn.Rom.V(5, cos.ModBackend) {
		cloudBck := lom.Bck().RemoteBck()
		nlog.Infof("[start_mpt] %s, upload_id: %s", cloudBck.Cname(lom.ObjName), uploadID)
	}

	return uploadID, 0, nil
}

func (azbp *azbp) PutMptPart(lom *core.LOM, r cos.ReadOpenCloser, _ *http.Request, uploadID string, _ int64, partNum int32) (string, int, error) {
	var (
		cloudBck = lom.Bck().RemoteBck()
		blURL    = azbp.u + "/" + cloudBck.Name + "/" + lom.ObjName
	)

	client, err := blockblob.NewClientWithSharedKeyCredential(blURL, azbp.creds, nil)
	if err != nil {
		cos.Close(r)
		ecode, err := azureErrorToAISError(err, cloudBck, lom.ObjName)
		return "", ecode, err
	}

	blockID := azureBlockID(uploadID, int(partNum))

	// StageBlock requires io.ReadSeekCloser
	rsc, ok := r.(io.ReadSeekCloser)
	debug.Assertf(ok, "Azure backend requires io.ReadSeekCloser, but got %T", r)

	_, err = client.StageBlock(context.Background(), blockID, rsc, nil)

	if err != nil {
		ecode, err := azureErrorToAISError(err, cloudBck, lom.ObjName)
		return "", ecode, err
	}

	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("[put_mpt_part] %s, part: %d, block_id: %s", cloudBck.Cname(lom.ObjName), partNum, blockID)
	}

	// Return empty string - block ID can be reconstructed from part number in CompleteMpt
	return "", 0, nil
}

func (azbp *azbp) CompleteMpt(lom *core.LOM, _ *http.Request, uploadID string, _ []byte, parts apc.MptCompletedParts) (version, etag string, _ int, _ error) {
	var (
		cloudBck = lom.Bck().RemoteBck()
		blURL    = azbp.u + "/" + cloudBck.Name + "/" + lom.ObjName
	)

	client, err := blockblob.NewClientWithSharedKeyCredential(blURL, azbp.creds, nil)
	if err != nil {
		ecode, err := azureErrorToAISError(err, cloudBck, lom.ObjName)
		return "", "", ecode, err
	}

	// Build the list of block IDs by reconstructing them from part numbers
	blockIDs := make([]string, len(parts))
	for i, part := range parts {
		debug.Assertf(part.PartNumber == i+1, "parts must be sorted without gaps: expected part %d, got %d", i+1, part.PartNumber)
		blockIDs[i] = azureBlockID(uploadID, part.PartNumber)
	}

	// Commit the block list to create the final blob
	resp, err := client.CommitBlockList(context.Background(), blockIDs, nil)
	if err != nil {
		ecode, err := azureErrorToAISError(err, cloudBck, lom.ObjName)
		return "", "", ecode, err
	}

	// Get version and ETag from response
	if resp.ETag != nil {
		if e, ok := cmn.BackendHelpers.Azure.EncodeETag(string(*resp.ETag)); ok {
			etag = e
			version = e // Azure uses ETag as version
		}
	}

	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("[complete_mpt] %s, version: %s, etag: %s, blocks: %d",
			cloudBck.Cname(lom.ObjName), version, etag, len(blockIDs))
	}

	return version, etag, 0, nil
}

func (*azbp) AbortMpt(lom *core.LOM, _ *http.Request, uploadID string) (ecode int, err error) {
	// Azure doesn't have an explicit "abort multipart upload" operation.
	// Uncommitted blocks are automatically garbage collected after 7 days.
	// We could optionally try to list and delete uncommitted blocks here,
	// but it's not strictly necessary and adds complexity.
	//
	// For now, just return success and let Azure's GC handle cleanup.

	if cmn.Rom.V(5, cos.ModBackend) {
		cloudBck := lom.Bck().RemoteBck()
		nlog.Infof("[abort_mpt] %s, upload_id: %s (uncommitted blocks will be GC'd by Azure)",
			cloudBck.Cname(lom.ObjName), uploadID)
	}

	return 0, nil
}
