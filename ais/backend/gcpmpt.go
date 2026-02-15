//go:build gcp

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"encoding/xml"
	"errors"
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

func (gsbp *gsbp) StartMpt(lom *core.LOM, _ *http.Request) (string, int, error) {
	cloudBck := lom.Bck().RemoteBck()
	sess, e := gsbp.getSess(context.Background(), cloudBck)
	if e != nil {
		return "", 0, e
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
		return "", http.StatusInternalServerError, err
	}

	resp, err := sess.httpClient.Do(req)
	cmn.FreeHra(reqArgs)
	cmn.HreqFree(req)

	if err != nil {
		return "", http.StatusInternalServerError, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err = fmt.Errorf("gcp: failed to initiate multipart upload: %s (status: %d)", string(body), resp.StatusCode)
		return "", resp.StatusCode, err
	}

	var result initiateMptUploadResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", http.StatusInternalServerError, fmt.Errorf("failed to decode response: %w", err)
	}

	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("[start_mpt] %s, upload_id: %s", cloudBck.Cname(lom.ObjName), result.UploadID)
	}

	return result.UploadID, 0, nil
}

func (gsbp *gsbp) PutMptPart(lom *core.LOM, r cos.ReadOpenCloser, _ *http.Request, uploadID string, size int64, partNum int32) (string, int, error) {
	cloudBck := lom.Bck().RemoteBck()
	sess, e := gsbp.getSess(context.Background(), cloudBck)
	if e != nil {
		return "", 0, e
	}

	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPut
		reqArgs.Base = gcpXMLEndpoint
		reqArgs.Path = cos.JoinPath(cloudBck.Name, lom.ObjName)
		reqArgs.BodyR = r
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
		cos.Close(r)
		return "", http.StatusInternalServerError, err
	}
	req.ContentLength = size

	resp, err := sess.httpClient.Do(req)
	cmn.FreeHra(reqArgs)
	cmn.HreqFree(req)
	cos.Close(r)

	if err != nil {
		return "", http.StatusInternalServerError, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err = fmt.Errorf("gcp: failed to upload part: %s (status: %d)", string(body), resp.StatusCode)
		return "", resp.StatusCode, err
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", http.StatusInternalServerError, errors.New("gcp: no ETag in response")
	}

	if cmn.Rom.V(5, cos.ModBackend) {
		nlog.Infof("[put_mpt_part] %s, part: %d, etag: %s", cloudBck.Cname(lom.ObjName), partNum, etag)
	}

	return etag, 0, nil
}

func (gsbp *gsbp) CompleteMpt(lom *core.LOM, _ *http.Request, uploadID string, _ []byte, parts apc.MptCompletedParts) (version, etag string, _ int, _ error) {
	cloudBck := lom.Bck().RemoteBck()
	sess, e := gsbp.getSess(context.Background(), cloudBck)
	if e != nil {
		return "", "", 0, e
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
		return "", "", http.StatusInternalServerError, fmt.Errorf("failed to marshal XML: %w", err)
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
		return "", "", http.StatusInternalServerError, err
	}

	resp, err := sess.httpClient.Do(req)
	cmn.FreeHra(reqArgs)
	cmn.HreqFree(req)

	if err != nil {
		return "", "", http.StatusInternalServerError, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err = fmt.Errorf("gcp: failed to complete multipart upload: %s (status: %d)", string(body), resp.StatusCode)
		return "", "", resp.StatusCode, err
	}

	var result completeMptUploadResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", http.StatusInternalServerError, fmt.Errorf("failed to decode response: %w", err)
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
