// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// NOTE: do not rename structs that have `xml` tags. The names of those structs
// become a top level tag of resulting XML, and those tags S3-compatible
// clients require.
type (
	// List objects response
	ListObjectResult struct {
		Name                  string          `xml:"Name"`
		Ns                    string          `xml:"xmlns,attr"`
		Prefix                string          `xml:"Prefix"`
		KeyCount              int             `xml:"KeyCount"`                 // number of object names in the response
		MaxKeys               int             `xml:"MaxKeys"`                  // "The maximum number of keys returned ..." (s3)
		IsTruncated           bool            `xml:"IsTruncated"`              // true if there are more pages to read
		ContinuationToken     string          `xml:"ContinuationToken"`        // original ContinuationToken
		NextContinuationToken string          `xml:"NextContinuationToken"`    // NextContinuationToken to read the next page
		Contents              []*ObjInfo      `xml:"Contents"`                 // list of objects
		CommonPrefixes        []*CommonPrefix `xml:"CommonPrefixes,omitempty"` // list of dirs (used with `apc.LsNoRecursion`)
	}
	ObjInfo struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		Class        string `xml:"StorageClass"`
	}
	CommonPrefix struct {
		Prefix string `xml:"Prefix"`
	}

	// Response for object copy request
	CopyObjectResult struct {
		LastModified string `xml:"LastModified"` // e.g. <LastModified>2009-10-12T17:50:30.000Z</LastModified>
		ETag         string `xml:"ETag"`
	}

	// Multipart upload start response
	InitiateMptUploadResult struct {
		Bucket   string `xml:"Bucket"`
		Key      string `xml:"Key"`
		UploadID string `xml:"UploadId"`
	}

	// Multipart upload completion request
	CompleteMptUpload struct {
		Parts []types.CompletedPart `xml:"Part"`
	}

	// Multipart upload completion response
	CompleteMptUploadResult struct {
		Bucket string `xml:"Bucket"`
		Key    string `xml:"Key"`
		ETag   string `xml:"ETag"`
	}

	// Multipart uploaded parts response
	ListPartsResult struct {
		Bucket   string                `xml:"Bucket"`
		Key      string                `xml:"Key"`
		UploadID string                `xml:"UploadId"`
		Parts    []types.CompletedPart `xml:"Part"`
	}

	// Active upload info
	UploadInfoResult struct {
		Key       string    `xml:"Key"`
		UploadID  string    `xml:"UploadId"`
		Initiated time.Time `xml:"Initiated"`
	}

	// List of active multipart uploads response
	ListMptUploadsResult struct {
		Bucket         string             `xml:"Bucket"`
		UploadIDMarker string             `xml:"UploadIdMarker"`
		Uploads        []UploadInfoResult `xml:"Upload"`
		MaxUploads     int
		IsTruncated    bool
	}

	// Deleted result: list of deleted objects and errors
	DeletedObjInfo struct {
		Key string `xml:"Key"`
	}
	DeleteResult struct {
		Objs []DeletedObjInfo `xml:"Deleted"`
	}
)

func ObjName(items []string) string { return path.Join(items[1:]...) }

func FillLsoMsg(query url.Values, msg *apc.LsoMsg) {
	mxStr := query.Get(QparamMaxKeys)
	if pageSize, err := strconv.Atoi(mxStr); err == nil && pageSize > 0 {
		msg.PageSize = int64(pageSize)
	}
	if prefix := query.Get(QparamPrefix); prefix != "" {
		msg.Prefix = prefix
	}
	var token string
	if token = query.Get(QparamContinuationToken); token != "" {
		// base64 encoded, as in: base64.StdEncoding.DecodeString(token)
		msg.ContinuationToken = token
	}
	// `start-after` is used only when starting to list pages, subsequent next-page calls
	// utilize `continuation-token`
	if after := query.Get(QparamStartAfter); after != "" && token == "" {
		msg.StartAfter = after
	}
	// TODO: check that the delimiter is '/' and raise an error otherwise
	if delimiter := query.Get(QparamDelimiter); delimiter != "" {
		msg.SetFlag(apc.LsNoRecursion)
	}
}

func NewListObjectResult(bucket string) *ListObjectResult {
	return &ListObjectResult{
		Name:     bucket,
		Ns:       s3Namespace,
		MaxKeys:  apc.MaxPageSizeAWS,
		Contents: make([]*ObjInfo, 0, apc.MaxPageSizeAWS),
	}
}

func (r *ListObjectResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *ListObjectResult) Add(entry *cmn.LsoEnt, lsmsg *apc.LsoMsg) {
	if entry.Flags&apc.EntryIsDir == 0 {
		r.Contents = append(r.Contents, entryToS3(entry, lsmsg))
	} else {
		prefix := entry.Name
		if !cos.IsLastB(entry.Name, '/') {
			prefix += "/"
		}
		r.CommonPrefixes = append(r.CommonPrefixes, &CommonPrefix{Prefix: prefix})
	}
}

func entryToS3(entry *cmn.LsoEnt, lsmsg *apc.LsoMsg) (oi *ObjInfo) {
	// [NOTE]: as we do not track mtime we make a debatable choice here to "prefer" atime
	// even when mtime (a.k.a. "LastModified") exists. Which is not always true (e.g.,
	// when using S3 compatibility API to access non-S3 buckets)
	oi = &ObjInfo{Key: entry.Name, Size: entry.Size, LastModified: entry.Atime}
	if oi.LastModified == "" && entry.Custom != "" {
		oi.LastModified = cmn.S2CustomVal(entry.Custom, cmn.LastModified)
	}
	if oi.LastModified == "" && lsmsg.TimeFormat != "" {
		oi.LastModified = cos.FormatNanoTime(0, lsmsg.TimeFormat) // 1970-01-01 epoch
	}
	return oi
}

func (r *ListObjectResult) FromLsoResult(lst *cmn.LsoRes, lsmsg *apc.LsoMsg) {
	r.KeyCount = len(lst.Entries)
	r.IsTruncated = lst.ContinuationToken != ""
	r.NextContinuationToken = lst.ContinuationToken
	for _, e := range lst.Entries {
		r.Add(e, lsmsg)
	}
}

func SetEtag(hdr http.Header, lom *core.LOM) {
	if hdr.Get(cos.S3CksumHeader) != "" {
		return
	}
	if v, exists := lom.GetCustomKey(cmn.ETag); exists && !cmn.IsS3MultipartEtag(v) {
		hdr.Set(cos.S3CksumHeader /*"ETag"*/, v)
		return
	}
	if cksum := lom.Checksum(); cksum.Type() == cos.ChecksumMD5 {
		hdr.Set(cos.S3CksumHeader, cksum.Value())
	}
}

func (r *CopyObjectResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *InitiateMptUploadResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *CompleteMptUploadResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *ListPartsResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *ListMptUploadsResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *DeleteResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}
