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
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
)

// SetBucketProps sets the properties of a bucket.
// Validation of the properties passed in is performed by AIStore Proxy.
func SetBucketProps(bp BaseParams, bck cmn.Bck, props *cmn.BucketPropsToUpdate, query ...url.Values) (string, error) {
	b := cos.MustMarshal(apc.ActionMsg{Action: apc.ActSetBprops, Value: props})
	return patchBucketProps(bp, bck, b, query...)
}

// ResetBucketProps resets the properties of a bucket to the global configuration.
func ResetBucketProps(bp BaseParams, bck cmn.Bck, query ...url.Values) (string, error) {
	b := cos.MustMarshal(apc.ActionMsg{Action: apc.ActResetBprops})
	return patchBucketProps(bp, bck, b, query...)
}

func patchBucketProps(bp BaseParams, bck cmn.Bck, body []byte, query ...url.Values) (xactID string, err error) {
	var q url.Values
	if len(query) > 0 {
		q = query[0]
	}
	q = bck.AddToQuery(q)
	bp.Method = http.MethodPatch
	path := apc.URLPathBuckets.Join(bck.Name)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
		reqParams.Body = body
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err = reqParams.DoHTTPReqResp(&xactID)
	FreeRp(reqParams)
	return
}

// HEAD(bucket): apc.HdrBucketProps => cmn.BucketProps{} and apc.HdrBucketInfo => BucketInfo{}
//
// Converts the string type fields returned from the HEAD request to their
// corresponding counterparts in the cmn.BucketProps struct.
//
// By default, AIStore adds remote buckets to the cluster metadata on the fly.
// Remote bucket that was never accessed before just "shows up" when user performs
// HEAD, PUT, GET, SET-PROPS, and a variety of other operations.
// This is done only once (and after confirming the bucket's existence and accessibility)
// and doesn't require any action from the user.
// Use `dontAddBckMD` to override the default behavior: as the name implies, setting
// `dontAddBckMD = true` prevents AIS from adding remote bucket to the cluster's metadata.
func HeadBucket(bp BaseParams, bck cmn.Bck, dontAddBckMD bool) (p *cmn.BucketProps, err error) {
	var (
		resp *wrappedResp
		path = apc.URLPathBuckets.Join(bck.Name)
		q    = make(url.Values, 4)
	)
	if dontAddBckMD {
		q.Set(apc.QparamDontAddBckMD, "true")
	}
	q = bck.AddToQuery(q)

	bp.Method = http.MethodHead
	reqParams := AllocRp()
	defer FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
		reqParams.Query = q
	}
	resp, err = reqParams.doResp(nil)
	if err == nil {
		p = &cmn.BucketProps{}
		err = jsoniter.Unmarshal([]byte(resp.Header.Get(apc.HdrBucketProps)), p)
		return
	}
	err = headerr2msg(bck, err)
	return
}

// Bucket information - a runtime addendum to `BucketProps`.
// Unlike `cmn.BucketProps` properties (which are user configurable), bucket runtime info:
// - includes usage, capacity, other statistics
// - is obtained via GetBucketInfo() API
// - delivered via apc.HdrBucketInfo header (compare with GetBucketSummary)
// The API utilizes HEAD method (compare with HeadBucket above)
func GetBucketInfo(bp BaseParams, bck cmn.Bck, getSummary bool) (p *cmn.BucketProps, info *cmn.BckSumm, err error) {
	var (
		resp *wrappedResp
		path = apc.URLPathBuckets.Join(bck.Name)
		q    = make(url.Values, 4)
	)
	q = bck.AddToQuery(q)
	q.Set(apc.QparamDontAddBckMD, "true") // don't add md (ie., is a "pure" query)
	if getSummary {
		q.Set(apc.QparamFltPresence, strconv.Itoa(apc.FltPresent)) // presence + bck summary
	} else {
		q.Set(apc.QparamFltPresence, strconv.Itoa(apc.FltPresentOmitProps)) // just presence
	}
	bp.Method = http.MethodHead
	reqParams := AllocRp()
	defer FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
		reqParams.Query = q
	}
	resp, err = reqParams.doResp(nil)
	if err == nil {
		p = &cmn.BucketProps{}
		err = jsoniter.Unmarshal([]byte(resp.Header.Get(apc.HdrBucketProps)), p)
		if err == nil {
			info = &cmn.BckSumm{}
			err = jsoniter.Unmarshal([]byte(resp.Header.Get(apc.HdrBucketSumm)), info)
		}
		return
	}
	err = headerr2msg(bck, err)
	return
}

// fill-in error message (HEAD response will never contain one)
func headerr2msg(bck cmn.Bck, err error) error {
	httpErr := cmn.Err2HTTPErr(err)
	if httpErr == nil {
		debug.FailTypeCast(err)
		return err
	}
	switch httpErr.Status {
	case http.StatusUnauthorized:
		httpErr.Message = fmt.Sprintf("Bucket %q unauthorized access", bck)
	case http.StatusForbidden:
		httpErr.Message = fmt.Sprintf("Bucket %q access denied", bck)
	case http.StatusGone:
		httpErr.Message = fmt.Sprintf("Bucket %q has been removed from the backend", bck)
	}
	return httpErr
}

// ListBuckets returns buckets for provided query, where
// `fltPresence` is one of { apc.FltExists, apc.FltPresent, ... }
// (ListBuckets must not be confused with `ListObjects()` and friends below).
func ListBuckets(bp BaseParams, qbck cmn.QueryBcks, fltPresence int) (cmn.Bcks, error) {
	var (
		bcks = cmn.Bcks{}
		path = apc.URLPathBuckets.S
		body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActList})
		q    = make(url.Values, 4)
	)
	debug.Assert(fltPresence >= apc.FltExists && fltPresence <= apc.FltExistsOutside)
	q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence))
	q = qbck.AddToQuery(q)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
		reqParams.Body = body
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err := reqParams.DoHTTPReqResp(&bcks)
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	return bcks, nil
}

// QueryBuckets queries cluster for buckets that satisfy the (`qbck`) criteria,
// and returns true if the selection contains at least one bucket that does.
func QueryBuckets(bp BaseParams, qbck cmn.QueryBcks, fltPresence int) (bool, error) {
	bcks, err := ListBuckets(bp, qbck, fltPresence)
	if err != nil {
		return false, err
	}
	return bcks.Contains(qbck), nil
}

// GetBucketSummary returns bucket summaries for the specified `cmn.QueryBcks` query
// (e.g., empty query corresponds to all buckets present in the cluster's BMD)
func GetBucketSummary(bp BaseParams, qbck cmn.QueryBcks, msg *apc.BckSummMsg, fltPresence int) (cmn.BckSummaries, error) {
	if msg == nil {
		msg = &apc.BckSummMsg{}
	}
	q := make(url.Values, 4)
	debug.Assert(fltPresence >= apc.FltExists && fltPresence <= apc.FltExistsOutside)
	q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence))
	q = qbck.AddToQuery(q)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	defer FreeRp(reqParams)
	summaries := cmn.BckSummaries{}
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(qbck.Name)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	if err := reqParams.waitBsumm(msg, &summaries); err != nil {
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
func CreateBucket(bp BaseParams, bck cmn.Bck, props *cmn.BucketPropsToUpdate) error {
	if err := bck.Validate(); err != nil {
		return err
	}
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
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
func DestroyBucket(bp BaseParams, bck cmn.Bck) error {
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActDestroyBck})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(nil)
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
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
func CopyBucket(bp BaseParams, fromBck, toBck cmn.Bck, msg *apc.CopyBckMsg) (xactID string, err error) {
	if err = toBck.Validate(); err != nil {
		return
	}
	q := fromBck.AddToQuery(nil)
	_ = toBck.AddUnameToQuery(q, apc.QparamBckTo)
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
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
func RenameBucket(bp BaseParams, fromBck, toBck cmn.Bck) (xactID string, err error) {
	if err = toBck.Validate(); err != nil {
		return
	}
	bp.Method = http.MethodPost
	q := fromBck.AddToQuery(nil)
	_ = toBck.AddUnameToQuery(q, apc.QparamBckTo)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
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
func EvictRemoteBucket(bp BaseParams, bck cmn.Bck, keepMD bool) error {
	var q url.Values
	if keepMD {
		q = url.Values{apc.QparamKeepBckMD: []string{"true"}}
	}

	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
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
func ListObjects(bp BaseParams, bck cmn.Bck, lsmsg *apc.ListObjsMsg, numObjects uint) (*cmn.BucketList, error) {
	return ListObjectsWithOpts(bp, bck, lsmsg, numObjects, nil)
}

// additional argument may include "progress-bar" context
func ListObjectsWithOpts(bp BaseParams, bck cmn.Bck, lsmsg *apc.ListObjsMsg, numObjects uint,
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
	bp.Method = http.MethodGet
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
		reqParams.BaseParams = bp
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
func ListObjectsPage(bp BaseParams, bck cmn.Bck, lsmsg *apc.ListObjsMsg) (*cmn.BucketList, error) {
	bp.Method = http.MethodGet
	if lsmsg == nil {
		lsmsg = &apc.ListObjsMsg{}
	}
	actMsg := apc.ActionMsg{Action: apc.ActList, Value: lsmsg}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
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
func ListObjectsInvalidateCache(bp BaseParams, bck cmn.Bck) error {
	var (
		path = apc.URLPathBuckets.Join(bck.Name)
		q    = url.Values{}
	)
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.Query = bck.AddToQuery(q)
		reqParams.BaseParams = bp
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
func MakeNCopies(bp BaseParams, bck cmn.Bck, copies int) (xactID string, err error) {
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMakeNCopies, Value: copies})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(nil)
	}
	err = reqParams.DoHTTPReqResp(&xactID)
	FreeRp(reqParams)
	return
}

func ECEncodeBucket(bp BaseParams, bck cmn.Bck, data, parity int) (xactID string, err error) {
	bp.Method = http.MethodPost
	// Without `string` conversion it makes base64 from []byte in `Body`.
	ecConf := string(cos.MustMarshal(&cmn.ECConfToUpdate{
		DataSlices:   &data,
		ParitySlices: &parity,
		Enabled:      Bool(true),
	}))
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActECEncode, Value: ecConf})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(nil)
	}
	err = reqParams.DoHTTPReqResp(&xactID)
	FreeRp(reqParams)
	return
}
