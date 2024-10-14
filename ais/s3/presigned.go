// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
)

const (
	signatureV4 = "AWS4-HMAC-SHA256"
)

type (
	PresignedReq struct {
		oreq  *http.Request
		lom   *core.LOM
		body  io.ReadCloser
		query url.Values
	}
	PresignedResp struct {
		Body       []byte
		BodyR      io.ReadCloser // Set when invoked `Do` with `async` option.
		Size       int64
		Header     http.Header
		StatusCode int
	}
)

//////////////////
// PresignedReq //
//////////////////

func NewPresignedReq(oreq *http.Request, lom *core.LOM, body io.ReadCloser, q url.Values) *PresignedReq {
	return &PresignedReq{oreq, lom, body, q}
}

// See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
func parseSignatureV4(query url.Values, header http.Header) (region string) {
	if credentials := query.Get(HeaderCredentials); credentials != "" {
		region = parseCredentialHeader(credentials)
	} else if authorization := header.Get(apc.HdrAuthorization); strings.HasPrefix(authorization, signatureV4) {
		authorization = strings.TrimPrefix(authorization, signatureV4)
		authorization = strings.TrimSpace(authorization)
		credentials := strings.Split(authorization, ", ")[0]
		region = parseCredentialHeader(credentials)
	}
	return region
}

// See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
func parseCredentialHeader(hdr string) (region string) {
	credentials := strings.TrimPrefix(strings.TrimSpace(hdr), "Credential=")
	parts := strings.Split(credentials, "/")
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

// (used by ais/backend/aws)
func (pts *PresignedReq) DoHead(client *http.Client) (*PresignedResp, error) {
	resp, err := pts.DoReader(client)
	if err != nil || resp == nil {
		return resp, err
	}
	return &PresignedResp{Header: resp.Header, StatusCode: resp.StatusCode}, nil
}

// (used by ais/backend/aws)
func (pts *PresignedReq) Do(client *http.Client) (*PresignedResp, error) {
	resp, err := pts.DoReader(client)
	if err != nil || resp == nil {
		return resp, err
	}

	var output []byte
	output, err = cos.ReadAllN(resp.BodyR, resp.Size)
	resp.BodyR.Close()
	if err != nil {
		return &PresignedResp{StatusCode: http.StatusBadRequest}, fmt.Errorf("failed to read response body: %v", err)
	}

	nsize := int64(len(output)) // == ContentLength == resp.Size
	return &PresignedResp{Body: output, Size: nsize, Header: resp.Header, StatusCode: resp.StatusCode}, nil
}

// DoReader sends request and returns opened body/reader if successful.
// Caller is responsible for closing the reader.
// (used by ais/backend/aws)
func (pts *PresignedReq) DoReader(client *http.Client) (*PresignedResp, error) {
	region := parseSignatureV4(pts.query, pts.oreq.Header)
	if region == "" {
		return nil, nil
	}

	// S3 checks every single query param
	pts.query.Del(apc.QparamProxyID)
	pts.query.Del(apc.QparamUnixTime)
	queryEncoded := pts.query.Encode()

	// produce a new request (nreq) from the old/original one (oreq)
	s3url := makeS3URL(region, pts.lom.Bck().Name, pts.lom.ObjName, queryEncoded)
	nreq, err := http.NewRequest(pts.oreq.Method, s3url, pts.body)
	if err != nil {
		return &PresignedResp{StatusCode: http.StatusInternalServerError}, err
	}
	nreq.Header = pts.oreq.Header // NOTE: _not_ cloning
	if nreq.Body != nil {
		nreq.ContentLength = pts.oreq.ContentLength
		if nreq.ContentLength == -1 {
			debug.Assert(false) // FIXME: remove, or catch in debug mode
			nreq.ContentLength = pts.lom.Lsize()
		}
	}

	resp, err := client.Do(nreq)
	if err != nil {
		return &PresignedResp{StatusCode: http.StatusInternalServerError}, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		output, _ := cos.ReadAll(resp.Body)
		resp.Body.Close()
		return &PresignedResp{StatusCode: resp.StatusCode}, fmt.Errorf("invalid status: %d, output: %s", resp.StatusCode, string(output))
	}

	return &PresignedResp{BodyR: resp.Body, Size: resp.ContentLength, Header: resp.Header, StatusCode: resp.StatusCode}, nil
}

///////////////////
// PresignedResp //
///////////////////

// (compare w/ cmn/objattrs FromHeader)
func (resp *PresignedResp) ObjAttrs() (oa *cmn.ObjAttrs) {
	oa = &cmn.ObjAttrs{}
	oa.CustomMD = make(cos.StrKVs, 3)

	oa.SetCustomKey(cmn.SourceObjMD, apc.AWS)
	etag := cmn.UnquoteCEV(resp.Header.Get(cos.HdrETag))
	debug.Assert(etag != "")
	oa.SetCustomKey(cmn.ETag, etag)
	if !cmn.IsS3MultipartEtag(etag) {
		oa.SetCustomKey(cmn.MD5ObjMD, etag)
	}
	if sz := resp.Header.Get(cos.HdrContentLength); sz != "" {
		size, err := strconv.ParseInt(sz, 10, 64)
		debug.AssertNoErr(err)
		oa.Size = size
	}
	return oa
}
