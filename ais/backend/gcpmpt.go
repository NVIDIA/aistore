//go:build gcp

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
)

// NOTE: Google's XML API is a compatibility layer for S3 clients and implements a subset of S3's control plane — buckets, ACLs, multipart, versioning.
// This API exists separately from the Google's Go client library (cloud.google.com/go/storage) that implements regular GET, PUT, HEAD, etc.

type (
	// XML response structures for GCP multipart upload
	initiateMptUploadResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}

	completeMptUploadResult struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
	}

	completedPart struct {
		PartNumber int32  `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}

	completeMultipartUpload struct {
		XMLName xml.Name        `xml:"CompleteMultipartUpload"`
		Parts   []completedPart `xml:"Part"`
	}
)

func (gsbp *gsbp) StartMpt(ctx context.Context, lom *core.LOM, _ *http.Request) (string, int, error) {
	ctx = mptContext(ctx)
	var (
		cloudBck = lom.Bck().RemoteBck()
		sess, e  = gsbp.getSess(ctx, cloudBck)
	)
	if e != nil {
		return "", http.StatusInternalServerError, e
	}

	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPost
		reqArgs.Base = gcpXMLEndpoint
		reqArgs.Path = cos.JoinPath(cloudBck.Name, lom.ObjName)
		reqArgs.Header = http.Header{
			cos.HdrContentLength: []string{"0"},
		}
		reqArgs.Query = url.Values{
			apc.QparamMptUploads: []string{""},
		}
	}

	req, err := reqArgs.Req()
	if err != nil {
		cmn.FreeHra(reqArgs)
		return "", http.StatusInternalServerError, fmt.Errorf("gcp: failed to create request: %w", err)
	}

	resp, err := sess.httpClient.Do(req.WithContext(ctx))
	cmn.FreeHra(reqArgs)
	cmn.HreqFree(req)

	if err != nil {
		return "", http.StatusInternalServerError, fmt.Errorf("gcp: failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if err := checkMptResponse(resp, http.StatusOK); err != nil {
		return "", resp.StatusCode, fmt.Errorf("gcp: failed to initiate multipart upload: %w", err)
	}

	var result initiateMptUploadResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", http.StatusInternalServerError, fmt.Errorf("gcp: failed to decode response: %w", err)
	}

	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("[start_mpt] %s, upload_id: %s", cloudBck.Cname(lom.ObjName), result.UploadID)
	}

	return result.UploadID, 0, nil
}

func (gsbp *gsbp) PutMptPart(ctx context.Context, lom *core.LOM, reader cos.ReadOpenCloser, _ *http.Request, uploadID string, size int64, partNum int32) (string, int, error) {
	ctx = mptContext(ctx)
	var (
		cloudBck = lom.Bck().RemoteBck()
		sess, e  = gsbp.getSess(ctx, cloudBck)
	)
	if e != nil {
		cos.Close(reader)
		return "", http.StatusInternalServerError, e
	}

	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPut
		reqArgs.Base = gcpXMLEndpoint
		reqArgs.Path = cos.JoinPath(cloudBck.Name, lom.ObjName)
		reqArgs.BodyR = reader
		reqArgs.Query = url.Values{
			apc.QparamMptPartNo:   []string{strconv.FormatInt(int64(partNum), 10)},
			apc.QparamMptUploadID: []string{uploadID},
		}
		reqArgs.Header = http.Header{
			cos.HdrContentLength: []string{strconv.FormatInt(size, 10)},
		}
	}

	req, err := reqArgs.Req()
	if err != nil {
		cmn.FreeHra(reqArgs)
		cos.Close(reader)
		return "", http.StatusInternalServerError, fmt.Errorf("gcp: failed to create request for part %d: %w", partNum, err)
	}
	req.ContentLength = size

	resp, err := sess.httpClient.Do(req.WithContext(ctx))
	cmn.FreeHra(reqArgs)
	cmn.HreqFree(req)
	cos.Close(reader)

	if err != nil {
		return "", http.StatusInternalServerError, fmt.Errorf("gcp: failed to upload part %d: %w", partNum, err)
	}
	defer resp.Body.Close()

	if err := checkMptResponse(resp, http.StatusOK); err != nil {
		return "", resp.StatusCode, fmt.Errorf("gcp: failed to upload part %d: %w", partNum, err)
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", http.StatusInternalServerError, fmt.Errorf("gcp: no ETag in response for part %d", partNum)
	}

	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("[put_mpt_part] %s, part: %d, etag: %s", cloudBck.Cname(lom.ObjName), partNum, etag)
	}

	return etag, 0, nil
}

func (gsbp *gsbp) CompleteMpt(ctx context.Context, lom *core.LOM, _ *http.Request, uploadID string, _ []byte, parts apc.MptCompletedParts) (version, etag string, _ int, _ error) {
	ctx = mptContext(ctx)
	var (
		cloudBck = lom.Bck().RemoteBck()
		sess, e  = gsbp.getSess(ctx, cloudBck)
	)
	if e != nil {
		return "", "", http.StatusInternalServerError, e
	}

	// Build XML body with completed parts
	completeMpt := completeMultipartUpload{
		Parts: make([]completedPart, len(parts)),
	}
	for i, part := range parts {
		completeMpt.Parts[i] = completedPart{
			PartNumber: int32(part.PartNumber),
			ETag:       part.ETag,
		}
	}

	xmlBody, err := xml.Marshal(completeMpt)
	if err != nil {
		return "", "", http.StatusInternalServerError, fmt.Errorf("gcp: failed to marshal XML body: %w", err)
	}

	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPost
		reqArgs.Base = gcpXMLEndpoint
		reqArgs.Path = cos.JoinPath(cloudBck.Name, lom.ObjName)
		reqArgs.Body = xmlBody
		reqArgs.Header = http.Header{
			cos.HdrContentType:   []string{cos.ContentXML},
			cos.HdrContentLength: []string{strconv.Itoa(len(xmlBody))},
		}
		reqArgs.Query = url.Values{
			apc.QparamMptUploadID: []string{uploadID},
		}
	}

	req, err := reqArgs.Req()
	if err != nil {
		cmn.FreeHra(reqArgs)
		return "", "", http.StatusInternalServerError, fmt.Errorf("gcp: failed to create request: %w", err)
	}

	resp, err := sess.httpClient.Do(req.WithContext(ctx))
	cmn.FreeHra(reqArgs)
	cmn.HreqFree(req)

	if err != nil {
		return "", "", http.StatusInternalServerError, fmt.Errorf("gcp: failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if err := checkMptResponse(resp, http.StatusOK); err != nil {
		return "", "", resp.StatusCode, fmt.Errorf("gcp: failed to complete multipart upload: %w", err)
	}

	var result completeMptUploadResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", http.StatusInternalServerError, fmt.Errorf("gcp: failed to decode response: %w", err)
	}

	// ETag
	if result.ETag != "" {
		if encoded, ok := cmn.BackendHelpers.Google.EncodeETag(result.ETag); ok {
			etag = encoded
		} else {
			etag = result.ETag
		}
	}

	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("[complete_mpt] %s, version: %s, etag: %s", cloudBck.Cname(lom.ObjName), version, etag)
	}

	return version, etag, 0, nil
}

// Go storage client has no XML multipart API (issue): https://github.com/googleapis/google-cloud-go/issues/11609
// Therefore, use Google's documented DELETE endpoint: https://cloud.google.com/storage/docs/xml-api/delete-multipart
func (gsbp *gsbp) AbortMpt(ctx context.Context, lom *core.LOM, _ *http.Request, uploadID string) (int, error) {
	ctx = mptContext(ctx)
	cloudBck := lom.Bck().RemoteBck()
	sess, err := gsbp.getSess(ctx, cloudBck)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodDelete
		reqArgs.Base = gcpXMLEndpoint
		reqArgs.Path = cos.JoinPath(cloudBck.Name, lom.ObjName)
		reqArgs.Query = url.Values{apc.QparamMptUploadID: []string{uploadID}}
	}
	req, err := reqArgs.Req()
	cmn.FreeHra(reqArgs)
	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("gcp: failed to create abort request: %w", err)
	}
	resp, err := sess.httpClient.Do(req.WithContext(ctx))
	cmn.HreqFree(req)
	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("gcp: failed to abort multipart upload: %w", err)
	}
	defer resp.Body.Close()
	if err := checkMptResponse(resp, http.StatusNoContent); err != nil {
		return resp.StatusCode, fmt.Errorf("gcp: failed to abort multipart upload: %w", err)
	}
	return 0, nil
}

func checkMptResponse(resp *http.Response, expectedStatus int) error {
	if resp.StatusCode == expectedStatus {
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unexpected status %d (expected %d), failed to read response body: %w",
			resp.StatusCode, expectedStatus, err)
	}
	return fmt.Errorf("unexpected status %d (expected %d): %s", resp.StatusCode, expectedStatus, string(body))
}
