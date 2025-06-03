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

const (
	virtualHostedRequestStyle = "virtual-hosted"
	pathRequestStyle          = "path"
)

type (
	PresignedReq struct {
		oreq  *http.Request
		lom   *core.LOM
		body  io.ReadCloser
		query url.Values
	}
	PresignedResp struct {
		BodyR      io.ReadCloser // Set when invoked `Do` with `async` option.
		Header     http.Header
		Body       []byte
		Size       int64
		StatusCode int
	}
)

//////////////////
// PresignedReq //
//////////////////

func NewPresignedReq(oreq *http.Request, lom *core.LOM, body io.ReadCloser, q url.Values) *PresignedReq {
	return &PresignedReq{oreq, lom, body, q}
}

func makeS3URL(requestStyle, region, bucketName, objName, query string) (string, error) {
	requestStyle = strings.TrimSpace(strings.ToLower(requestStyle))
	switch requestStyle {
	// `virtual-hosted` style is used by default as this is default in S3.
	case virtualHostedRequestStyle, "":
		b := &strings.Builder{}
		b.Grow(8 + len(bucketName) + 4 + len(region) + 15 + len(objName) + 1 + len(query))
		b.WriteString("https://")
		b.WriteString(bucketName)
		b.WriteString(".s3.")
		b.WriteString(region)
		b.WriteString(".amazonaws.com/")
		b.WriteString(objName)
		b.WriteByte('?')
		b.WriteString(query)
		return b.String(), nil
	case pathRequestStyle:
		b := &strings.Builder{}
		b.Grow(11 + len(region) + 15 + len(bucketName) + 1 + len(objName) + 1 + len(query))
		b.WriteString("https://s3.")
		b.WriteString(region)
		b.WriteString(".amazonaws.com/")
		b.WriteString(bucketName)
		b.WriteByte('/')
		b.WriteString(objName)
		b.WriteByte('?')
		b.WriteString(query)
		return b.String(), nil
	default:
		return "", fmt.Errorf("unrecognized request style provided: %v", requestStyle)
	}
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

func (pts *PresignedReq) DoHead(client *http.Client) (*PresignedResp, error) {
	canOptimize := pts.oreq.Method == http.MethodGet
	canOptimize = canOptimize && pts.oreq.Header.Get(cos.S3HdrContentSHA256) == ""
	canOptimize = canOptimize && !strings.Contains(pts.oreq.Header.Get(cos.S3HdrSignedHeaders), strings.ToLower(cos.HdrRange))
	if canOptimize {
		// Temporarily override `Range` header value.
		//
		// This method could be executed in context of GET request in which case we
		// don't want to retrieve the object but just the metadata. Note that we cannot
		// simply use HEAD here since the request might not be signed for this method.
		hdrRangeValue, hdrRangeExists := pts.oreq.Header[cos.HdrRange]
		pts.oreq.Header.Set(cos.HdrRange, cos.HdrRangeValPrefix+"0-0")
		defer func() {
			if hdrRangeExists {
				pts.oreq.Header[cos.HdrRange] = hdrRangeValue
			} else {
				pts.oreq.Header.Del(cos.HdrRange)
			}
		}()
	}

	resp, err := pts.DoReader(client)
	if err != nil || resp == nil {
		return resp, err
	}
	// note: usually, we cos.Close() and ignore
	if err := resp.BodyR.Close(); err != nil {
		return &PresignedResp{StatusCode: http.StatusBadRequest}, fmt.Errorf("failed to close response, err: %w", err)
	}
	return &PresignedResp{Header: resp.Header, StatusCode: resp.StatusCode}, nil
}

// Do request/response.
// Always return `PresignedResp` (on error - with `StatusCode = 400`)
func (pts *PresignedReq) Do(client *http.Client) (*PresignedResp, error) {
	resp, err := pts.DoReader(client)
	if err != nil || resp == nil {
		return resp, err
	}

	output, errN := cos.ReadAllN(resp.BodyR, resp.Size)
	errClose := resp.BodyR.Close()
	if errN != nil {
		return &PresignedResp{StatusCode: http.StatusBadRequest}, fmt.Errorf("failed to read response: %w", errN)
	}
	// note: usually, we cos.Close() and ignore
	if errClose != nil {
		return &PresignedResp{StatusCode: http.StatusBadRequest}, fmt.Errorf("failed to close response: %w", errClose)
	}

	nsize := int64(len(output)) // == ContentLength == resp.Size
	return &PresignedResp{Body: output, Size: nsize, Header: resp.Header, StatusCode: resp.StatusCode}, nil
}

// DoReader sends request and returns opened body/reader if successful.
// Caller is responsible for closing the reader.
//
// NOTE: If error occurs `PresignedResp` with `StatusCode` will be set.
// NOTE: `BodyR` will be set only if `err` is `nil`.
func (pts *PresignedReq) DoReader(client *http.Client) (*PresignedResp, error) {
	region := parseSignatureV4(pts.query, pts.oreq.Header)
	if region == "" {
		return nil, nil
	}

	// S3 checks every single query param
	pts.query.Del(apc.QparamPID)
	pts.query.Del(apc.QparamUnixTime)
	queryEncoded := pts.query.Encode()

	signedRequestStyle := pts.oreq.Header.Get(apc.HdrSignedRequestStyle)
	s3url, err := makeS3URL(signedRequestStyle, region, pts.lom.Bck().Name, pts.lom.ObjName, queryEncoded)
	if err != nil {
		return &PresignedResp{StatusCode: http.StatusBadRequest}, err
	}

	// produce a new request (nreq) from the old/original one (oreq)
	// NOTE: The original request's context includes tracing attributes.
	// It is essential to pass the original context to new request to ensure traces are correctly linked.
	nreq, err := http.NewRequestWithContext(pts.oreq.Context(), pts.oreq.Method, s3url, pts.body)
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
	h := cmn.BackendHelpers.Amazon

	oa = &cmn.ObjAttrs{}
	oa.CustomMD = make(cos.StrKVs, 3)
	oa.SetCustomKey(cmn.SourceObjMD, apc.AWS)
	if v, ok := h.EncodeETag(resp.Header.Get(cos.HdrETag)); ok {
		oa.SetCustomKey(cmn.ETag, v)
	}
	if v, ok := h.EncodeCksum(resp.Header.Get(cos.S3CksumHeader)); ok {
		oa.SetCustomKey(cmn.MD5ObjMD, v)
	}
	if sz := resp.Header.Get(cos.HdrContentLength); sz != "" {
		size, err := strconv.ParseInt(sz, 10, 64)
		debug.AssertNoErr(err)
		oa.Size = size
	}
	return oa
}
