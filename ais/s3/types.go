// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
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
		ContinuationToken     string          `xml:"ContinuationToken"`               // original
		NextContinuationToken string          `xml:"NextContinuationToken,omitempty"` // to read the next page
		Contents              []*ObjInfo      `xml:"Contents"`                        // list of object
		CommonPrefixes        []*CommonPrefix `xml:"CommonPrefixes,omitempty"`        // list of dirs (used with `apc.LsNoRecursion`)
		KeyCount              int             `xml:"KeyCount"`                        // number of object names in the response
		MaxKeys               int             `xml:"MaxKeys"`                         // "The maximum number of keys returned ..."
		IsTruncated           bool            `xml:"IsTruncated"`                     // true if there are more pages to read
	}
	ObjInfo struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Class        string `xml:"StorageClass"`
		Size         int64  `xml:"Size"`
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
		Initiated time.Time `xml:"Initiated"`
		Key       string    `xml:"Key"`
		UploadID  string    `xml:"UploadId"`
	}

	// List of active multipart uploads response
	ListMptUploadsResult struct {
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
	sgl.Write(cos.UnsafeB(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
}

func (r *ListObjectResult) add(entry *cmn.LsoEnt) {
	if entry.Flags&apc.EntryIsDir == 0 {
		r.Contents = append(r.Contents, entryToS3(entry))
	} else {
		prefix := entry.Name
		if !cos.IsLastB(entry.Name, '/') {
			prefix += "/"
		}
		r.CommonPrefixes = append(r.CommonPrefixes, &CommonPrefix{Prefix: prefix})
	}
}

// Note: in S3 listings, xs/wanted_lso populates entry.Custom with ETag/LastModified
// but only if the latter is (or are) missing
// here, if Custom is empty, we fall back to Atime for LastModified and omit ETag
// (see related: `apc.LsIsS3`)
func entryToS3(entry *cmn.LsoEnt) (oi *ObjInfo) {
	oi = &ObjInfo{Key: entry.Name, Size: entry.Size, LastModified: entry.Atime}

	if entry.Custom != "" {
		md := make(cos.StrKVs, 4)
		cmn.S2CustomMD(md, entry.Custom, "")
		if v, ok := md[cmn.LsoLastModified]; ok {
			oi.LastModified = v
		}
		oi.ETag = md[cmn.ETag]
	}
	return oi
}

func (r *ListObjectResult) FromLsoResult(lst *cmn.LsoRes) {
	r.KeyCount = len(lst.Entries)
	r.IsTruncated = lst.ContinuationToken != ""
	r.NextContinuationToken = lst.ContinuationToken
	for _, e := range lst.Entries {
		r.add(e)
	}
}

func SetS3Headers(hdr http.Header, lom *core.LOM) {
	// 1. ETag (must be a quoted string: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag)
	if etag := hdr.Get(cos.HdrETag); etag != "" {
		debug.AssertFunc(func() bool {
			return etag[0] == '"' && etag[len(etag)-1] == '"'
		})
	} else {
		if v, exists := lom.GetCustomKey(cmn.ETag); exists {
			hdr.Set(cos.HdrETag, cmn.QuoteETag(v))
		} else if cksum := lom.Checksum(); cksum.Type() == cos.ChecksumMD5 {
			debug.Assert(cksum.Val()[0] != '"', cksum.Val())
			// NOTE: could this object be multipart?
			hdr.Set(cos.HdrETag, cmn.QuoteETag(cksum.Value()))
		}
	}

	// 2. Last-Modified
	if v, ok := lom.GetCustomKey(cos.HdrLastModified); ok {
		hdr.Set(cos.HdrLastModified, v)
	} else if lom.AtimeUnix() != 0 {
		// - see "as we do not track mtime we choose to _prefer_ atime" comment above
		// - http.TimeFormat (string) is GMT, hence UTC
		atime := lom.Atime()
		hdr.Set(cos.HdrLastModified, atime.UTC().Format(http.TimeFormat))
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
