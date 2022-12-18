// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

const defaultLastModified = 0 // When an object was not accessed yet

// NOTE: do not rename structs that have `xml` tags. The names of those structs
// become a top level tag of resulting XML, and those tags S3-compatible
// clients require.
type (
	// List objects response
	ListObjectResult struct {
		Ns                    string     `xml:"xmlns,attr"`
		Prefix                string     `xml:"Prefix"`
		KeyCount              int        `xml:"KeyCount"` // number of objects in the response
		MaxKeys               int        `xml:"MaxKeys"`
		IsTruncated           bool       `xml:"IsTruncated"`           // true if there are more pages to read
		ContinuationToken     string     `xml:"ContinuationToken"`     // original ContinuationToken
		NextContinuationToken string     `xml:"NextContinuationToken"` // NextContinuationToken to read the next page
		Contents              []*ObjInfo `xml:"Contents"`              // list of objects
	}
	ObjInfo struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		Class        string `xml:"StorageClass"`
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

	// Multipart uploaded part
	PartInfo struct {
		ETag       string `xml:"ETag"`
		PartNumber int64  `xml:"PartNumber"`
		Size       int64  `xml:"Size,omitempty"`
	}

	// Multipart upload completion request
	CompleteMptUpload struct {
		Parts []*PartInfo `xml:"Part"`
	}

	// Multipart upload completion response
	CompleteMptUploadResult struct {
		Bucket string `xml:"Bucket"`
		Key    string `xml:"Key"`
		ETag   string `xml:"ETag"`
	}

	// Multipart uploaded parts response
	ListPartsResult struct {
		Bucket   string      `xml:"Bucket"`
		Key      string      `xml:"Key"`
		UploadID string      `xml:"UploadId"`
		Parts    []*PartInfo `xml:"Part"`
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
)

func ObjName(items []string) string { return path.Join(items[1:]...) }

func FillMsgFromS3Query(query url.Values, msg *apc.LsoMsg) {
	mxStr := query.Get("max-keys")
	if pageSize, err := strconv.Atoi(mxStr); err == nil && pageSize > 0 {
		msg.PageSize = uint(pageSize)
	}
	if prefix := query.Get("prefix"); prefix != "" {
		msg.Prefix = prefix
	}
	var token string
	if token = query.Get("continuation-token"); token != "" {
		msg.ContinuationToken = token
	}
	// start-after makes sense only on first call. For the next call,
	// when continuation-token is set, start-after is ignored
	if after := query.Get("start-after"); after != "" && token == "" {
		msg.StartAfter = after
	}
}

func NewListObjectResult() *ListObjectResult {
	return &ListObjectResult{
		Ns:       s3Namespace,
		MaxKeys:  1000,
		Contents: make([]*ObjInfo, 0),
	}
}

func (r *ListObjectResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *ListObjectResult) Add(entry *cmn.LsoEntry, lsmsg *apc.LsoMsg) {
	r.Contents = append(r.Contents, entryToS3(entry, lsmsg))
}

func entryToS3(entry *cmn.LsoEntry, lsmsg *apc.LsoMsg) *ObjInfo {
	objInfo := &ObjInfo{
		Key:          entry.Name,
		LastModified: entry.Atime,
		ETag:         entry.Checksum,
		Size:         entry.Size,
	}
	// Some S3 clients do not tolerate empty or missing LastModified, so fill it
	// with a zero time if the object was not accessed yet
	if objInfo.LastModified == "" {
		objInfo.LastModified = cos.FormatNanoTime(defaultLastModified, lsmsg.TimeFormat)
	}
	return objInfo
}

func (r *ListObjectResult) FillFromAisBckList(bckList *cmn.LsoResult, lsmsg *apc.LsoMsg) {
	r.KeyCount = len(bckList.Entries)
	r.IsTruncated = bckList.ContinuationToken != ""
	r.ContinuationToken = bckList.ContinuationToken
	for _, e := range bckList.Entries {
		r.Add(e, lsmsg)
	}
}

func lomMD5(lom *cluster.LOM) string {
	if v, exists := lom.GetCustomKey(cmn.SourceObjMD); exists && v == apc.AWS {
		if v, exists := lom.GetCustomKey(cmn.MD5ObjMD); exists {
			return v
		}
	}
	if cksum := lom.Checksum(); cksum.Type() == cos.ChecksumMD5 {
		return cksum.Value()
	}
	return ""
}

func SetETag(header http.Header, lom *cluster.LOM) {
	if md5val := lomMD5(lom); md5val != "" {
		header.Set(cos.S3CksumHeader, md5val)
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
