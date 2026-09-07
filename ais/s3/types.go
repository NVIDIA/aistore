// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"encoding/xml"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/memsys"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// NOTE: for structs without an `XMLName` field, the Go struct name becomes the
// top-level tag of the resulting XML, and those tags S3-compatible clients require.
// Do not rename such structs.
//
// Where the spec's root element name differs from our struct name, we pin it
// explicitly via `XMLName xml.Name` (below) so the wire format stays AWS-spec
// compliant regardless of the Go identifier. Strict-parsing clients (e.g. the
// AWS Rust SDK, used by s3dlio) reject non-spec root tags.
type (
	// Response for object copy request — emits <CopyObjectResult> per AWS S3 CopyObject spec
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html#API_CopyObject_ResponseSyntax
	CopyObjectResult struct {
		LastModified string `xml:"LastModified"` // e.g. <LastModified>2009-10-12T17:50:30.000Z</LastModified>
		ETag         string `xml:"ETag"`
	}

	// Multipart upload start response — emits <InitiateMultipartUploadResult> per AWS S3 spec
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html#API_CreateMultipartUpload_ResponseSyntax
	InitiateMptUploadResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}

	// Multipart upload completion request
	CompleteMptUpload struct {
		Parts []types.CompletedPart `xml:"Part"`
	}

	// Multipart upload completion response — emits <CompleteMultipartUploadResult> per AWS S3 spec
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html#API_CompleteMultipartUpload_ResponseSyntax
	CompleteMptUploadResult struct {
		XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
		Bucket  string   `xml:"Bucket"`
		Key     string   `xml:"Key"`
		ETag    string   `xml:"ETag"`
	}

	// Multipart uploaded parts response — emits <ListPartsResult> per AWS S3 ListParts spec
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html#API_ListParts_ResponseSyntax
	ListPartsResult struct {
		Bucket   string                `xml:"Bucket"`
		Key      string                `xml:"Key"`
		UploadID string                `xml:"UploadId"`
		Parts    []types.CompletedPart `xml:"Part"`
	}

	// Active upload info
	UploadInfoResult struct {
		Initiated time.Time `xml:"Initiated"`
		Key       string    `xml:"Key"`
		UploadID  string    `xml:"UploadId"`
	}

	// List of active multipart uploads response — emits <ListMultipartUploadsResult> per AWS S3 spec
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html#API_ListMultipartUploads_ResponseSyntax
	ListMptUploadsResult struct {
		XMLName        xml.Name           `xml:"ListMultipartUploadsResult"`
		Bucket         string             `xml:"Bucket"`
		UploadIDMarker string             `xml:"UploadIdMarker"`
		Uploads        []UploadInfoResult `xml:"Upload"`
		MaxUploads     int                `xml:"MaxUploads"`
		IsTruncated    bool               `xml:"IsTruncated"`
	}

	// Deleted result: list of deleted objects and errors
	DeletedObjInfo struct {
		Key string `xml:"Key"`
	}
	// Multiple object delete response — emits <DeleteResult> per AWS S3 DeleteObjects spec
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html#API_DeleteObjects_ResponseSyntax
	DeleteResult struct {
		Objs []DeletedObjInfo `xml:"Deleted"`
	}
)

func ObjName(items []string) string { return path.Join(items[1:]...) }

// join S3 object-key path components and validate the resulting object name;
// on failure: write an S3 error response
func JoinValidateOname(w http.ResponseWriter, r *http.Request, items []string) (oname string, err error) {
	oname = ObjName(items)
	if err = cos.ValidateOname(oname); err != nil {
		WriteErr(w, r, ErrInfo{Err: err})
	}
	return
}

func SetS3Headers(hdr http.Header, lom *core.LOM) {
	// 1. Last-Modified
	var (
		mtime    time.Time
		mtimeStr string
	)
	if mtimeStr, mtime = lom.LastModifiedStr(); mtimeStr != "" {
		hdr.Set(cos.HdrLastModified, mtimeStr)
	}

	// 2. ETag (must be a quoted string: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag)
	if etag := hdr.Get(cos.HdrETag); etag != "" {
		debug.AssertFunc(func() bool {
			return etag[0] == '"' && etag[len(etag)-1] == '"'
		})
	} else if etag := lom.ETag(mtime, true /*allow syscall*/); etag != "" {
		debug.AssertFunc(func() bool { return etag[0] != '"' }, etag)
		hdr.Set(cos.HdrETag, cmn.QuoteETag(etag))
	}

	// 3. x-amz-version-id
	if hdr.Get(cos.S3VersionHeader) == "" {
		if v, ok := lom.GetCustomKey(cmn.VersionObjMD); ok {
			hdr.Set(cos.S3VersionHeader, v)
		}
	}

	// 4. finally, user metadata (X-Amz-Meta-...)
	for k, v := range lom.GetCustomMD() {
		if strings.HasPrefix(k, HeaderMetaPrefix) {
			hdr.Set(k, v)
		}
	}
}

func (r *CopyObjectResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write(cos.UnsafeB(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *InitiateMptUploadResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write(cos.UnsafeB(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *CompleteMptUploadResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write(cos.UnsafeB(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *ListPartsResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write(cos.UnsafeB(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *ListMptUploadsResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write(cos.UnsafeB(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *DeleteResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write(cos.UnsafeB(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func DecodeXML[T any](body []byte) (result T, _ error) {
	if err := xml.Unmarshal(body, &result); err != nil {
		return result, err
	}
	return result, nil
}
