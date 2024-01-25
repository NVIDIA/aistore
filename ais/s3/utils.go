// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core"
)

const (
	signatureV4 = "AWS4-HMAC-SHA256"
)

// FIXME: We should handle error cases.
func parseSignatureV4(query url.Values, header http.Header) (region string) {
	if credentials := query.Get(HeaderCredentials); credentials != "" {
		region = strings.Split(credentials, "/")[2]
	} else if credentials := header.Get(apc.HdrAuthorization); strings.HasPrefix(credentials, signatureV4) {
		credentials = strings.TrimPrefix(credentials, signatureV4)
		credentials = strings.TrimSpace(credentials)
		credentials = strings.Split(credentials, ", ")[0]
		credentials = strings.TrimPrefix(credentials, "Credential=")
		region = strings.Split(credentials, "/")[2]
	}
	return region
}

type PassThroughSignedResp struct {
	Body       []byte
	Header     http.Header
	StatusCode int
}

func PassThroughSignedReq(client *http.Client, req *http.Request, lom *core.LOM, body io.ReadCloser) (*PassThroughSignedResp, error) {
	if req == nil {
		return nil, nil
	}

	query := req.URL.Query()

	region := parseSignatureV4(query, req.Header)
	if region == "" {
		return nil, nil
	}

	// We must delete extra query since S3 signs query parameters as well.
	// Otherwise, S3 will reject our request due to signature mismatch.
	query.Del(apc.QparamProxyID)
	query.Del(apc.QparamUnixTime)
	queryEncoded := query.Encode()

	r, err := http.NewRequest(req.Method, makeS3URL(region, lom.Bucket().Name, lom.ObjName, queryEncoded), body)
	if err != nil {
		return &PassThroughSignedResp{StatusCode: http.StatusInternalServerError}, err
	}
	r.Header = req.Header.Clone()
	// FIXME: Hopefully we can avoid this hacking and simply do `r.ContentLength = req.ContentLength`.
	if r.Body != nil {
		contentLength := req.ContentLength
		if contentLength == -1 {
			contentLength = lom.SizeBytes()
		}
		r.ContentLength = contentLength
	}

	resp, err := client.Do(r)
	if err != nil {
		return &PassThroughSignedResp{StatusCode: http.StatusInternalServerError}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		output, _ := io.ReadAll(resp.Body)
		return &PassThroughSignedResp{StatusCode: resp.StatusCode}, fmt.Errorf("invalid status: %d, output: %s", resp.StatusCode, string(output))
	}

	output, err := io.ReadAll(resp.Body)
	if err != nil {
		return &PassThroughSignedResp{StatusCode: http.StatusBadRequest}, fmt.Errorf("failed to read response body, err: %v", err)
	}

	return &PassThroughSignedResp{
		Body:       output,
		Header:     resp.Header,
		StatusCode: resp.StatusCode,
	}, nil
}
