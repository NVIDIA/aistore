// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

// SetBucketProps sets the properties of a bucket.
// Validation of the properties passed in is performed by AIStore Proxy.
func SetBucketProps(baseParams BaseParams, bck cmn.Bck, props *cmn.BucketPropsToUpdate, query ...url.Values) (string, error) {
	b := cos.MustMarshal(apc.ActionMsg{Action: apc.ActSetBprops, Value: props})
	return patchBucketProps(baseParams, bck, b, query...)
}

// ResetBucketProps resets the properties of a bucket to the global configuration.
func ResetBucketProps(baseParams BaseParams, bck cmn.Bck, query ...url.Values) (string, error) {
	b := cos.MustMarshal(apc.ActionMsg{Action: apc.ActResetBprops})
	return patchBucketProps(baseParams, bck, b, query...)
}

func patchBucketProps(baseParams BaseParams, bck cmn.Bck, body []byte, query ...url.Values) (xactID string, err error) {
	var q url.Values
	if len(query) > 0 {
		q = query[0]
	}
	q = bck.AddToQuery(q)
	baseParams.Method = http.MethodPatch
	path := apc.URLPathBuckets.Join(bck.Name)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = path
		reqParams.Body = body
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err = reqParams.DoHTTPReqResp(&xactID)
	FreeRp(reqParams)
	return
}

// HeadBucket returns the properties of a bucket specified by its name.
// Converts the string type fields returned from the HEAD request to their
// corresponding counterparts in the cmn.BucketProps struct.
func HeadBucket(baseParams BaseParams, bck cmn.Bck) (p *cmn.BucketProps, err error) {
	var (
		q    url.Values
		resp *wrappedResp
		path = apc.URLPathBuckets.Join(bck.Name)
	)
	baseParams.Method = http.MethodHead
	q = bck.AddToQuery(q)

	reqParams := AllocRp()
	defer FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = path
		reqParams.Query = q
	}
	resp, err = reqParams.doResp(nil)
	if err == nil {
		p = &cmn.BucketProps{}
		err = jsoniter.Unmarshal([]byte(resp.Header.Get(apc.HdrBucketProps)), p)
		return
	}
	// try to fill in error message (HEAD response will never contain one)
	if httpErr := cmn.Err2HTTPErr(err); httpErr != nil {
		switch httpErr.Status {
		case http.StatusUnauthorized:
			httpErr.Message = fmt.Sprintf("Bucket %q unauthorized access", bck)
		case http.StatusForbidden:
			httpErr.Message = fmt.Sprintf("Bucket %q access denied", bck)
		case http.StatusNotFound:
			httpErr.Message = fmt.Sprintf("Bucket %q not found", bck)
		case http.StatusGone:
			httpErr.Message = fmt.Sprintf("Bucket %q has been removed from the backend", bck)
		default:
			httpErr.Message = fmt.Sprintf(
				"Failed to access bucket %q (code: %d)",
				bck, httpErr.Status,
			)
		}
		err = httpErr
	}
	return
}

// ListBuckets returns buckets for provided query.
// (not to confuse with `ListObjects()` and friends below)
func ListBuckets(baseParams BaseParams, qbck cmn.QueryBcks) (cmn.Bcks, error) {
	var (
		bcks  = cmn.Bcks{}
		path  = apc.URLPathBuckets.S
		body  = cos.MustMarshal(apc.ActionMsg{Action: apc.ActList})
		query = qbck.AddToQuery(nil)
	)
	baseParams.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = path
		reqParams.Body = body
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = query
	}
	err := reqParams.DoHTTPReqResp(&bcks)
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	return bcks, nil
}

// GetBucketsSummaries returns bucket summaries for the specified backend provider
// (and all bucket summaries for unspecified ("") provider).
func GetBucketsSummaries(baseParams BaseParams, qbck cmn.QueryBcks, msg *apc.BckSummMsg) (cmn.BckSummaries, error) {
	if msg == nil {
		msg = &apc.BckSummMsg{}
	}
	baseParams.Method = http.MethodGet
	reqParams := AllocRp()
	defer FreeRp(reqParams)
	summaries := cmn.BckSummaries{}
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathBuckets.Join(qbck.Name)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = qbck.AddToQuery(nil)
	}
	if err := reqParams.waitSummary(msg, &summaries); err != nil {
		return nil, err
	}
	sort.Sort(summaries)
	return summaries, nil
}

// CreateBucket sends request to create an AIS bucket with the given name and,
// optionally, specific non-default properties (via cmn.BucketPropsToUpdate).
//
// See also:
//   - github.com/NVIDIA/aistore/blob/master/docs/bucket.md#default-bucket-properties
//   - cmn.BucketPropsToUpdate (cmn/api.go)
//
// Bucket properties can be also changed at any time via SetBucketProps (above).
func CreateBucket(baseParams BaseParams, bck cmn.Bck, props *cmn.BucketPropsToUpdate) error {
	if err := bck.Validate(); err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActCreateBck, Value: props})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(nil)
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// DestroyBucket sends request to remove an AIS bucket with the given name.
func DestroyBucket(baseParams BaseParams, bck cmn.Bck) error {
	baseParams.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActDestroyBck})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(nil)
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// DoesBucketExist queries a proxy or target to get a list of all AIS buckets,
// returns true if the bucket is present in the list.
func DoesBucketExist(baseParams BaseParams, qbck cmn.QueryBcks) (bool, error) {
	bcks, err := ListBuckets(baseParams, qbck)
	if err != nil {
		return false, err
	}
	return bcks.Contains(qbck), nil
}

// CopyBucket copies existing `fromBck` bucket to the destination `toBck` thus,
// effectively, creating a copy of the `fromBck`.
//   - AIS will create `toBck` on the fly but only if the destination bucket does not
//     exist and _is_ provided by AIStore; 3rd party backend destination must exist -
//     otherwise the copy operation won't be successful.
//   - There are no limitations on copying buckets across Backend providers:
//     you can copy AIS bucket to (or from) AWS bucket, and the latter to Google or Azure
//     bucket, etc.
//   - Copying multiple buckets to the same destination bucket is also permitted.
func CopyBucket(baseParams BaseParams, fromBck, toBck cmn.Bck, msg *apc.CopyBckMsg) (xactID string, err error) {
	if err = toBck.Validate(); err != nil {
		return
	}
	q := fromBck.AddToQuery(nil)
	_ = toBck.AddUnameToQuery(q, apc.QparamBucketTo)
	baseParams.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathBuckets.Join(fromBck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActCopyBck, Value: msg})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err = reqParams.DoHTTPReqResp(&xactID)
	FreeRp(reqParams)
	return
}

// RenameBucket renames fromBck as toBck.
func RenameBucket(baseParams BaseParams, fromBck, toBck cmn.Bck) (xactID string, err error) {
	if err = toBck.Validate(); err != nil {
		return
	}
	baseParams.Method = http.MethodPost
	q := fromBck.AddToQuery(nil)
	_ = toBck.AddUnameToQuery(q, apc.QparamBucketTo)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathBuckets.Join(fromBck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMoveBck})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err = reqParams.DoHTTPReqResp(&xactID)
	FreeRp(reqParams)
	return
}

// EvictRemoteBucket sends request to evict an entire remote bucket from the AIStore
// - keepMD: evict objects but keep bucket metadata
func EvictRemoteBucket(baseParams BaseParams, bck cmn.Bck, keepMD bool) error {
	var q url.Values
	baseParams.Method = http.MethodDelete
	if keepMD {
		q = url.Values{apc.QparamKeepBckMD: []string{"true"}}
	}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActEvictRemoteBck})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(q)
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// ListObjects returns list of objects in a bucket. `numObjects` is the
// maximum number of objects to be returned (0 - return all objects in a bucket).
//
// This API supports numerous options and flags. In particular, `apc.ListObjsMsg`
// supports "opening" objects formatted as one of the supported
// archival types and include contents of archived directories in generated
// result sets.
// In addition, `apc.ListObjsMsg` provides options (flags) to optimize ListObjects
// performance, to list anonymous public-access Cloud buckets, and more.
// For detals, see: `api/apc/lsmsg.go` source.
//
// AIS fully supports listing buckets that may have millions of objects.
// For large and very large buckets, it is strongly recommended to use ListObjectsPage
// that will return the very first (listed) page and a so called "continuation token".
// See ListObjectsPage for details.
//
// For usage examples, see CLI docs under docs/cli.
func ListObjects(baseParams BaseParams, bck cmn.Bck, lsmsg *apc.ListObjsMsg, numObjects uint) (*cmn.BucketList, error) {
	return ListObjectsWithOpts(baseParams, bck, lsmsg, numObjects, nil)
}

// additional argument may include "progress-bar" context
func ListObjectsWithOpts(baseParams BaseParams, bck cmn.Bck, lsmsg *apc.ListObjsMsg, numObjects uint,
	progress *ProgressContext) (bckList *cmn.BucketList, err error) {
	var (
		q    url.Values
		path = apc.URLPathBuckets.Join(bck.Name)
		hdr  = http.Header{
			cos.HdrAccept:      []string{cos.ContentMsgPack},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
		nextPage = &cmn.BucketList{}
		toRead   = numObjects
		listAll  = numObjects == 0
	)
	baseParams.Method = http.MethodGet
	if lsmsg == nil {
		lsmsg = &apc.ListObjsMsg{}
	}
	q = bck.AddToQuery(q)
	bckList = &cmn.BucketList{}
	lsmsg.UUID = ""
	lsmsg.ContinuationToken = ""

	// `rem` holds the remaining number of objects to list (that is, unless we are listing
	// the entire bucket). Each iteration lists a page of objects and reduces the `rem`
	// counter accordingly. When the latter gets below page size, we perform the final
	// iteration for the reduced page.
	reqParams := AllocRp()
	defer FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = path
		reqParams.Header = hdr
		reqParams.Query = q
	}
	for pageNum := 1; listAll || toRead > 0; pageNum++ {
		if !listAll {
			lsmsg.PageSize = toRead
		}
		actMsg := apc.ActionMsg{Action: apc.ActList, Value: lsmsg}
		reqParams.Body = cos.MustMarshal(actMsg)
		page := nextPage

		if pageNum == 1 {
			page = bckList
		} else {
			// Do not try to optimize by reusing allocated page as `Unmarshaler`/`Decoder`
			// will reuse the entry pointers what will result in duplications.
			page.Entries = nil
		}

		// Retry with increasing timeout.
		for i := 0; i < 5; i++ {
			if err = reqParams.DoHTTPReqResp(page); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					client := *reqParams.BaseParams.Client
					client.Timeout = 2 * client.Timeout
					reqParams.BaseParams.Client = &client
					continue
				}
				return nil, err
			}
			break
		}
		if err != nil {
			return nil, err
		}

		bckList.Flags |= page.Flags
		// The first iteration uses the `bckList` directly so there is no need to append.
		if pageNum > 1 {
			bckList.Entries = append(bckList.Entries, page.Entries...)
			bckList.ContinuationToken = page.ContinuationToken
		}

		if progress != nil && progress.mustFire() {
			progress.info.Count = len(bckList.Entries)
			if page.ContinuationToken == "" {
				progress.finish()
			}
			progress.callback(progress)
		}

		if page.ContinuationToken == "" { // Listed all objects.
			lsmsg.ContinuationToken = ""
			break
		}

		toRead = uint(cos.Max(int(toRead)-len(page.Entries), 0))
		cos.Assert(cos.IsValidUUID(page.UUID))
		lsmsg.UUID = page.UUID
		lsmsg.ContinuationToken = page.ContinuationToken
	}

	return bckList, err
}

// ListObjectsPage returns the first page of bucket objects.
// On success the function updates `lsmsg.ContinuationToken` which client then can reuse
// to fetch the next page.
// See also: CLI and CLI usage examples
// See also: `apc.ListObjsMsg`
// See also: `api.ListObjectsInvalidateCache`
// See also: `api.ListObjects`
func ListObjectsPage(baseParams BaseParams, bck cmn.Bck, lsmsg *apc.ListObjsMsg) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodGet
	if lsmsg == nil {
		lsmsg = &apc.ListObjsMsg{}
	}
	actMsg := apc.ActionMsg{Action: apc.ActList, Value: lsmsg}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Header = http.Header{
			cos.HdrAccept:      []string{cos.ContentMsgPack},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
		reqParams.Query = bck.AddToQuery(url.Values{})
		reqParams.Body = cos.MustMarshal(actMsg)
	}

	// NOTE: No need to preallocate bucket entries slice, we use msgpack so it will do it for us!
	page := &cmn.BucketList{}
	err := reqParams.DoHTTPReqResp(page)
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	lsmsg.UUID = page.UUID
	lsmsg.ContinuationToken = page.ContinuationToken
	return page, nil
}

// TODO: obsolete this function after introducing mechanism to detect remote bucket changes.
func ListObjectsInvalidateCache(baseParams BaseParams, bck cmn.Bck) error {
	var (
		path = apc.URLPathBuckets.Join(bck.Name)
		q    = url.Values{}
	)
	baseParams.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.Query = bck.AddToQuery(q)
		reqParams.BaseParams = baseParams
		reqParams.Path = path
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActInvalListCache})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// MakeNCopies starts an extended action (xaction) to bring a given bucket to a
// certain redundancy level (num copies).
func MakeNCopies(baseParams BaseParams, bck cmn.Bck, copies int) (xactID string, err error) {
	baseParams.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMakeNCopies, Value: copies})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(nil)
	}
	err = reqParams.DoHTTPReqResp(&xactID)
	FreeRp(reqParams)
	return
}

func ECEncodeBucket(baseParams BaseParams, bck cmn.Bck, data, parity int) (xactID string, err error) {
	baseParams.Method = http.MethodPost
	// Without `string` conversion it makes base64 from []byte in `Body`.
	ecConf := string(cos.MustMarshal(&cmn.ECConfToUpdate{
		DataSlices:   &data,
		ParitySlices: &parity,
		Enabled:      Bool(true),
	}))
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActECEncode, Value: ecConf})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(nil)
	}
	err = reqParams.DoHTTPReqResp(&xactID)
	FreeRp(reqParams)
	return
}
