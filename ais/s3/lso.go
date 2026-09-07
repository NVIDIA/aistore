// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	// List objects response — emits <ListBucketResult> per AWS S3 ListObjectsV2 spec
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_ResponseSyntax
	ListObjectResult struct {
		XMLName               xml.Name        `xml:"ListBucketResult"`
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
)

// parse s3 list-objects query params
func FillLsoMsg(query url.Values, msg *apc.LsoMsg, maxPageSize int64) (int64, error) {
	maxKeys, err := _maxKeys(query, maxPageSize)
	if err != nil {
		return 0, err
	}
	msg.PageSize = maxKeys

	if prefix := query.Get(QparamPrefix); prefix != "" {
		// validate but do not normalize - compare with the native flow (ais/proxy.go),
		// which additionally calls cos.TrimPrefix (trailing '*' is AIS wildcard with
		// no S3 counterpart: on the S3 wire '*' is an ordinary prefix character and must stay literal)
		if err := cos.ValidatePrefix(apc.BadLsoRequest, prefix); err != nil {
			return 0, err
		}
		msg.Prefix = prefix
	}

	debug.Assert(msg.ContinuationToken == "", "expecting it empty in FillLsoMsg")
	msg.ContinuationToken = query.Get(QparamContinuationToken)

	// `start-after` is used only when starting to list pages, subsequent next-page calls
	// utilize `continuation-token`
	if after := query.Get(QparamStartAfter); after != "" && msg.ContinuationToken == "" {
		msg.StartAfter = after
	}

	// apc.LsNoRecursion: S3 permits an arbitrary delimiter; AIS implements '/' only
	if delimiter := query.Get(QparamDelimiter); delimiter != "" {
		if delimiter != "/" {
			return 0, fmt.Errorf("invalid %q=%q: expecting '/' (only slash-delimited listing is supported)",
				QparamDelimiter, delimiter)
		}
		msg.SetFlag(apc.LsNoRecursion)
	}

	return maxKeys, nil
}

// `max-keys` is the effective page size, following S3 (ListObjectsV2) convention:
// - absent: default to the max (AWS: 1000)
// - greater than the max: silently capped without an error
// - negative or non-numeric: 400 InvalidArgument
// ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
func _maxKeys(query url.Values, maxPageSize int64) (int64, error) {
	maxPageSize = min(maxPageSize, apc.MaxPageSizeAWS)

	mxStr := query.Get(QparamMaxKeys)
	if mxStr == "" {
		return maxPageSize, nil
	}
	maxKeys, err := strconv.ParseInt(mxStr, 10, 64)
	if err != nil || maxKeys < 0 {
		return 0, fmt.Errorf("invalid %q=%q: expecting a non-negative integer", QparamMaxKeys, mxStr)
	}
	return min(maxKeys, maxPageSize), nil
}

func NewListObjectResult(bucket string, maxKeys int64) *ListObjectResult {
	return &ListObjectResult{
		Name:    bucket,
		Ns:      s3Namespace,
		MaxKeys: int(maxKeys),
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

func (r *ListObjectResult) FromLsoResult(lst *cmn.LsoRes, token string) {
	r.ContinuationToken = token
	if lst == nil {
		return
	}
	r.KeyCount = len(lst.Entries)
	r.IsTruncated = lst.ContinuationToken != ""
	r.NextContinuationToken = lst.ContinuationToken
	r.Contents = make([]*ObjInfo, 0, len(lst.Entries)) // upper bound: some entries are dirs
	for _, e := range lst.Entries {
		r.add(e)
	}
}

// S3 list-objects: compound continuation token.
//
// S3 treats the continuation token as opaque (AWS itself returns base64 blobs), which lets us
// carry the x-lso UUID alongside the token proper:
//
//	base64url( [ver] uuid \x00 orig )
//
// The UUID is x-lso ID - it is what makes a paged S3 listing a _single_ listing: without it
// the proxy starts a brand-new xaction for every page (see ais/plstcx.go, `newls`).
//
// `orig` is the original token as produced by the flow underneath;
// on the A-flow it is an object name, on the R-flow the remote backend's own opaque string.
//
// lsoTokSep = NUL as the separator: `uuid` is drawn from `cos.uuidABC` and cannot contain one,
// so splitting on the first NUL is unambiguous whatever `orig` holds.
//
// Anything that does not parse as the above is taken for pre-v5.1 token
// and handed back as-is with an empty UUID (i.e. the legacy path - new xaction per page).
//
// The reverse - v5.1 token reaching an older-version node - is not supported.
//
// TODO:
// - add a disclaimer on not-supporting mixed-version clusters.
// - consider sbAlloc/Free - see e.g., ais/signverify

const (
	lsoTokVer = 0x01 // format version; bump on any layout change
	lsoTokSep = 0x00 // separator (see above)
)

// combine `uuid` and `orig` into a single opaque compound token;
// return "" when the `orig` is empty
func EncodeToken(uuid, orig string) string {
	if orig == "" {
		return ""
	}
	debug.Func(func() { debug.Assert(cos.IsValidUUID(uuid), uuid) })

	var sb cos.SB
	sb.Init(1 + len(uuid) + 1 + len(orig))
	sb.WriteUint8(lsoTokVer)
	sb.WriteString(uuid)
	sb.WriteUint8(lsoTokSep)
	sb.WriteString(orig)

	return base64.RawURLEncoding.EncodeToString(sb.Bytes())
}

// split a client-supplied continuation token back into (uuid, orig);
// failure to parse is a fallback to legacy (empty uuid, token as-is)
func DecodeToken(s string) (uuid, orig string) {
	if s == "" {
		return "", ""
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) == 0 || b[0] != lsoTokVer {
		return "", s // legacy
	}
	i := bytes.IndexByte(b[1:], lsoTokSep)
	if i <= 0 {
		return "", s // ditto
	}
	uuid = string(b[1 : i+1])
	if !cos.IsValidUUID(uuid) {
		return "", s // legacy
	}
	return uuid, string(b[i+2:])
}
