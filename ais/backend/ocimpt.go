//go:build oci

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"

	ocios "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// StartMptOCI initiates multipart upload for OCI backend via S3 API compatibility.
//
// Background
// ==========
// AIStore provides a unified backend abstraction where all operations route through
// t.Backend(bck) to get the appropriate backend implementation (AWS, OCI, GCP, etc.).
// Regular S3 operations (PUT/GET/LIST) work with any backend through this
// abstraction, but multipart uploads are currently not universal (yet).
//
// This source enables S3 clients (boto3, s3cmd, aws cli, etc.) to perform multipart uploads
// against OCI backend without code changes, providing a migration path from
// AWS S3 to OCI and multi-cloud compatibility.
//
// S3 Client App → S3 API calls → AIStore /v1/s3 endpoint → OCI backend
//
// Stateful backend instance
// =========================
//
// Unlike AWS S3 MPU which can create sessions on-demand, OCI requires
// a pre-configured backend instance containing stateful components:
// - namespace (OCI-specific namespace identifier)
// - configurationProvider (authentication setup)
// - client (_single_ instance shared across all oc:// buckets
//        expensive to create (DNS, auth handshake)))
// - compartmentOCID and various MPU settings
//
// The backend parameter is guaranteed to be *ocibp by the call path
// routing logic (see ais/tgts3mpt)

func (bp *ocibp) StartMpt(lom *core.LOM, _ *http.Request) (string, int, error) {
	var (
		cloudBck                     = lom.Bck().RemoteBck()
		createMultipartUploadRequest = ocios.CreateMultipartUploadRequest{
			NamespaceName: &bp.namespace,
			BucketName:    &cloudBck.Name,
			CreateMultipartUploadDetails: ocios.CreateMultipartUploadDetails{
				Object: &lom.ObjName,
			},
		}
		createMultipartUploadResponse ocios.CreateMultipartUploadResponse
		err                           error
		uploadID                      string
		ecode                         int
	)

	createMultipartUploadResponse, err = bp.client.CreateMultipartUpload(context.Background(), createMultipartUploadRequest)

	if err == nil {
		uploadID = *createMultipartUploadResponse.MultipartUpload.UploadId
	} else {
		ecode, err = ociErrorToAISError("CreateMultipartUpload()", cloudBck.Name, lom.ObjName, "", err, createMultipartUploadResponse.RawResponse)
	}

	return uploadID, ecode, err
}

func (bp *ocibp) PutMptPart(lom *core.LOM, r cos.ReadOpenCloser, _ *http.Request, uploadID string, size int64, partNum int32) (string, int, error) {
	var (
		cloudBck          = lom.Bck().RemoteBck()
		partNumInt        = int(partNum)
		uploadPartRequest = ocios.UploadPartRequest{
			NamespaceName:  &bp.namespace,
			BucketName:     &cloudBck.Name,
			ObjectName:     &lom.ObjName,
			UploadId:       &uploadID,
			UploadPartNum:  &partNumInt,
			ContentLength:  &size,
			UploadPartBody: r,
		}
		uploadPartResponse ocios.UploadPartResponse
		err                error
		etag               string
		ecode              int
	)

	uploadPartResponse, err = bp.client.UploadPart(context.Background(), uploadPartRequest)

	if err == nil {
		etag = *uploadPartResponse.ETag
	} else {
		ecode, err = ociErrorToAISError(fmt.Sprintf("UploadPart(%s:%v)", uploadID, partNumInt), cloudBck.Name, lom.ObjName, "", err, uploadPartResponse.RawResponse)
	}

	return etag, ecode, err
}

func (bp *ocibp) CompleteMpt(lom *core.LOM, _ *http.Request, uploadID string, _ []byte, parts apc.MptCompletedParts) (string, string, int, error) {
	var (
		cloudBck                     = lom.Bck().RemoteBck()
		commitMultipartUploadRequest = ocios.CommitMultipartUploadRequest{
			NamespaceName: &bp.namespace,
			BucketName:    &cloudBck.Name,
			ObjectName:    &lom.ObjName,
			UploadId:      &uploadID,
			CommitMultipartUploadDetails: ocios.CommitMultipartUploadDetails{
				PartsToCommit: make([]ocios.CommitMultipartUploadPartDetails, 0, len(parts)),
			},
		}
		commitMultipartUploadResponse ocios.CommitMultipartUploadResponse
		err                           error
		etag                          string
		ecode                         int
	)

	// Convert apc.MptCompletedParts to OCI types
	for _, completedPart := range parts {
		commitMultipartUploadRequest.CommitMultipartUploadDetails.PartsToCommit = append(
			commitMultipartUploadRequest.CommitMultipartUploadDetails.PartsToCommit,
			ocios.CommitMultipartUploadPartDetails{
				PartNum: &completedPart.PartNumber,
				Etag:    &completedPart.ETag,
			})
	}

	commitMultipartUploadResponse, err = bp.client.CommitMultipartUpload(context.Background(), commitMultipartUploadRequest)

	if err == nil {
		etag = *commitMultipartUploadResponse.ETag
	} else {
		ecode, err = ociErrorToAISError(fmt.Sprintf("CommitMultipartUpload(%s)", uploadID), cloudBck.Name, lom.ObjName, "", err, commitMultipartUploadResponse.RawResponse)
	}

	return "", etag, ecode, err
}

func (bp *ocibp) AbortMpt(lom *core.LOM, _ *http.Request, uploadID string) (int, error) {
	var (
		cloudBck                    = lom.Bck().RemoteBck()
		abortMultipartUploadRequest = ocios.AbortMultipartUploadRequest{
			NamespaceName: &bp.namespace,
			BucketName:    &cloudBck.Name,
			ObjectName:    &lom.ObjName,
			UploadId:      &uploadID,
		}
		abortMultipartUploadResponse ocios.AbortMultipartUploadResponse
		err                          error
		ecode                        int
	)

	abortMultipartUploadResponse, err = bp.client.AbortMultipartUpload(context.Background(), abortMultipartUploadRequest)

	if err != nil {
		ecode, err = ociErrorToAISError(fmt.Sprintf("AbortMultipartUpload(%s)", uploadID), cloudBck.Name, lom.ObjName, "", err, abortMultipartUploadResponse.RawResponse)
	}

	return ecode, err
}
