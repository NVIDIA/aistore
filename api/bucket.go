// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	initialPollInterval = 50 * time.Millisecond
	maxPollInterval     = 10 * time.Second
)

type (
	// ProgressInfo to notify a caller about an operation progress. The operation
	// returns negative values for data that unavailable(e.g, Total or Percent for ListObjects).
	ProgressInfo struct {
		Percent float64
		Count   int
		Total   int
	}
	ProgressContext struct {
		startTime time.Time // time when operation was started
		callAfter time.Time // call a callback only after this point in time
		callback  ProgressCallback

		info ProgressInfo
	}
	ProgressCallback = func(pi *ProgressContext)
)

// SetBucketProps sets the properties of a bucket.
// Validation of the properties passed in is performed by AIStore Proxy.
func SetBucketProps(baseParams BaseParams, bck cmn.Bck, props cmn.BucketPropsToUpdate, query ...url.Values) (string, error) {
	b := cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActSetBprops, Value: props})
	return patchBucketProps(baseParams, bck, b, query...)
}

// ResetBucketProps resets the properties of a bucket to the global configuration.
func ResetBucketProps(baseParams BaseParams, bck cmn.Bck, query ...url.Values) (string, error) {
	b := cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActResetBprops})
	return patchBucketProps(baseParams, bck, b, query...)
}

func patchBucketProps(baseParams BaseParams, bck cmn.Bck, body []byte, query ...url.Values) (xactID string, err error) {
	var q url.Values
	if len(query) > 0 {
		q = query[0]
	}
	q = cmn.AddBckToQuery(q, bck)
	baseParams.Method = http.MethodPatch
	path := cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name)
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: path, Body: body, Query: q}, &xactID)
	return
}

// HeadBucket returns the properties of a bucket specified by its name.
// Converts the string type fields returned from the HEAD request to their
// corresponding counterparts in the cmn.BucketProps struct.
func HeadBucket(baseParams BaseParams, bck cmn.Bck, query ...url.Values) (p *cmn.BucketProps, err error) {
	var (
		path = cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name)
		q    url.Values
	)
	p = &cmn.BucketProps{}
	baseParams.Method = http.MethodHead
	if len(query) > 0 {
		q = query[0]
	}
	q = cmn.AddBckToQuery(q, bck)

	resp, err := doHTTPRequestGetResp(ReqParams{BaseParams: baseParams, Path: path, Query: q}, nil)
	if err != nil {
		return
	}

	err = cmn.IterFields(p, func(tag string, field cmn.IterField) (error, bool) {
		return field.SetValue(resp.Header.Get(tag), true /*force*/), false
	}, cmn.IterOpts{OnlyRead: false})
	return
}

// ListBuckets returns bucket names for the given provider. Provider takes one
// of Cloud Provider enum names (see cmn/bucket.go). If provider is empty,
// return all names. Otherwise, return cloud or ais bucket names.
func ListBuckets(baseParams BaseParams, queryBcks cmn.QueryBcks) (cmn.BucketNames, error) {
	var (
		bucketNames = cmn.BucketNames{}
		path        = cmn.JoinWords(cmn.Version, cmn.Buckets, cmn.AllBuckets)
		query       = cmn.AddBckToQuery(nil, cmn.Bck(queryBcks))
	)

	baseParams.Method = http.MethodGet
	err := DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: path, Query: query}, &bucketNames)
	if err != nil {
		return nil, err
	}
	return bucketNames, nil
}

// GetBucketsSummaries returns bucket summaries for the specified bucket provider
// (and all bucket summaries for unspecified ("") provider).
func GetBucketsSummaries(baseParams BaseParams, query cmn.QueryBcks, msg *cmn.BucketSummaryMsg) (cmn.BucketsSummaries, error) {
	if msg == nil {
		msg = &cmn.BucketSummaryMsg{}
	}
	baseParams.Method = http.MethodPost

	reqParams := ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Buckets, query.Name),
		Header:     http.Header{cmn.HeaderContentType: []string{cmn.ContentJSON}},
		Query:      cmn.AddBckToQuery(nil, cmn.Bck(query)),
	}
	var summaries cmn.BucketsSummaries
	if err := waitForAsyncReqComplete(reqParams, cmn.ActSummaryBucket, msg, &summaries); err != nil {
		return nil, err
	}
	sort.Sort(summaries)
	return summaries, nil
}

// CreateBucket sends a HTTP request to a proxy to create an AIS bucket with the given name.
func CreateBucket(baseParams BaseParams, bck cmn.Bck, ops ...cmn.BucketPropsToUpdate) error {
	if len(ops) > 1 {
		return fmt.Errorf("only a single BucketPropsToUpdate parameter can be accepted")
	}
	var value interface{}
	if len(ops) == 1 {
		value = ops[0]
	}
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActCreateLB, Value: value}),
		Query:      cmn.AddBckToQuery(nil, bck),
	})
}

// DestroyBucket sends a HTTP request to a proxy to remove an AIS bucket with the given name.
func DestroyBucket(baseParams BaseParams, bck cmn.Bck) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActDestroyLB}),
		Query:      cmn.AddBckToQuery(nil, bck),
	})
}

// DoesBucketExist queries a proxy or target to get a list of all AIS buckets,
// returns true if the bucket is present in the list.
func DoesBucketExist(baseParams BaseParams, query cmn.QueryBcks) (bool, error) {
	bcks, err := ListBuckets(baseParams, query)
	if err != nil {
		return false, err
	}
	return bcks.Contains(query), nil
}

// CopyBucket copies existing `fromBck` bucket to the destination `toBck` thus,
// effectively, creating a copy of the `fromBck`.
// * AIS will create `toBck` on the fly but only if the destination bucket does not
//   exist and is provided by AIStore (note that Cloud-based `toBck` must exist
//   for the copy operation to be successful)
// * There are no limitations on copying buckets across Cloud providers:
//   you can copy AIS bucket to (or from) AWS bucket, and the latter to Google or Azure
//   bucket, etc.
// * Copying multiple buckets to the same destination bucket is also permitted.
func CopyBucket(baseParams BaseParams, fromBck, toBck cmn.Bck, msgs ...*cmn.CopyBckMsg) (xactID string, err error) {
	msg := &cmn.CopyBckMsg{}
	if len(msgs) > 0 && msgs[0] != nil {
		msg = msgs[0]
	}
	msg.BckTo = toBck
	baseParams.Method = http.MethodPost
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Buckets, fromBck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActCopyBucket, Value: msg}),
	}, &xactID)
	return
}

// RenameBucket changes the name of a bucket from `oldBck` to `newBck`.
func RenameBucket(baseParams BaseParams, oldBck, newBck cmn.Bck) (xactID string, err error) {
	baseParams.Method = http.MethodPost
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Buckets, oldBck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActRenameLB, Name: newBck.Name}),
	}, &xactID)
	return
}

// DeleteList sends a HTTP request to remove a list of objects from a bucket.
func DeleteList(baseParams BaseParams, bck cmn.Bck, filesList []string) (string, error) {
	deleteMsg := cmn.ListMsg{ObjNames: filesList}
	return doListRangeRequest(baseParams, bck, cmn.ActDelete, deleteMsg)
}

// DeleteRange sends a HTTP request to remove a range of objects from a bucket.
func DeleteRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	deleteMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActDelete, deleteMsg)
}

// PrefetchList sends a HTTP request to prefetch a list of objects from a cloud bucket.
func PrefetchList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	prefetchMsg := cmn.ListMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActPrefetch, prefetchMsg)
}

// PrefetchRange sends a HTTP request to prefetch a range of objects from a cloud bucket.
func PrefetchRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	prefetchMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActPrefetch, prefetchMsg)
}

// EvictList sends a HTTP request to evict a list of objects from a cloud bucket.
func EvictList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	evictMsg := cmn.ListMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActEvictObjects, evictMsg)
}

// EvictRange sends a HTTP request to evict a range of objects from a cloud bucket.
func EvictRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	evictMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActEvictObjects, evictMsg)
}

// EvictCloudBucket sends a HTTP request to a proxy to evict an entire cloud bucket from the AIStore
// - the operation results in eliminating all traces of the specified cloud bucket in the AIStore
func EvictCloudBucket(baseParams BaseParams, bck cmn.Bck, query ...url.Values) error {
	var q url.Values
	if len(query) > 0 {
		q = query[0]
	}

	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActEvictCB}),
		Query:      cmn.AddBckToQuery(q, bck),
	})
}

// Polling:
// 1. The function sends the requests as is (smsg.UUID should be empty) to initiate
//    asynchronous task. The destination returns ID of a newly created task
// 2. Starts polling: request destination with received UUID in a loop while
//    the destination returns StatusAccepted=task is still running
//	  Time between requests is dynamic: it starts at 200ms and increases
//	  by half after every "not-StatusOK" request. It is limited with 10 seconds
// 3. Breaks loop on error
// 4. If the destination returns status code StatusOK, it means the response
//    contains the real data and the function returns the response to the caller
func waitForAsyncReqComplete(reqParams ReqParams, action string, msg *cmn.BucketSummaryMsg, v interface{}) error {
	cmn.Assert(action == cmn.ActSummaryBucket)
	var (
		uuid   string
		sleep  = initialPollInterval
		actMsg = cmn.ActionMsg{Action: action, Value: msg}
	)
	if reqParams.Query == nil {
		reqParams.Query = url.Values{}
	}
	reqParams.Body = cmn.MustMarshal(actMsg)
	resp, err := doHTTPRequestGetResp(reqParams, &uuid)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusAccepted {
		if resp.StatusCode == http.StatusOK {
			return errors.New("expected 202 response code on first call, got 200")
		}
		return fmt.Errorf("invalid response code: %d", resp.StatusCode)
	}
	if msg.UUID == "" {
		msg.UUID = uuid
	}

	// Poll async task for http.StatusOK completion
	for {
		reqParams.Body = cmn.MustMarshal(actMsg)
		resp, err = doHTTPRequestGetResp(reqParams, v)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(sleep)
		if sleep < maxPollInterval {
			sleep += sleep / 2
		}
	}
	return err
}

// ListObjects returns list of objects in a bucket. `numObjects` is the
// maximum number of objects returned (0 - return all objects in a bucket).
func ListObjects(baseParams BaseParams, bck cmn.Bck, smsg *cmn.SelectMsg, numObjects uint,
	args ...*ProgressContext) (bckList *cmn.BucketList, err error) {
	baseParams.Method = http.MethodPost
	if smsg == nil {
		smsg = &cmn.SelectMsg{}
	}
	var (
		ctx       *ProgressContext
		q         = cmn.AddBckToQuery(url.Values{}, bck)
		path      = cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name)
		hdr       = http.Header{cmn.HeaderAccept: []string{cmn.ContentMsgPack}}
		reqParams = ReqParams{BaseParams: baseParams, Path: path, Header: hdr, Query: q}
		nextPage  = &cmn.BucketList{}
		rem       = numObjects
		listAll   = numObjects == 0
	)
	bckList = &cmn.BucketList{}
	smsg.UUID = ""
	smsg.ContinuationToken = ""
	if len(args) != 0 {
		ctx = args[0]
	}

	// `rem` holds the remaining number of objects to list (that is, unless we are listing
	// the entire bucket). Each iteration lists a page of objects and reduces the `rem`
	// counter accordingly. When the latter gets below page size, we perform the final
	// iteration for the reduced page.
	for pageNum := 1; listAll || rem > 0; pageNum++ {
		if !listAll {
			smsg.PageSize = rem
		}
		actMsg := cmn.ActionMsg{Action: cmn.ActListObjects, Value: smsg}
		reqParams.Body = cmn.MustMarshal(actMsg)
		page := nextPage

		if pageNum == 1 {
			page = bckList
		} else {
			page.Entries = nil
		}

		for i := 0; i < 5; i++ {
			if _, err = doHTTPRequestGetResp(reqParams, page); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					client := *reqParams.BaseParams.Client
					client.Timeout = 2 * client.Timeout // retry with increasing timeout
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

		if pageNum > 1 {
			bckList.Entries = append(bckList.Entries, page.Entries...)
			bckList.ContinuationToken = page.ContinuationToken
		}

		if ctx != nil && ctx.mustFire() {
			ctx.info.Count = len(bckList.Entries)
			if page.ContinuationToken == "" {
				ctx.finish()
			}
			ctx.callback(ctx)
		}

		if page.ContinuationToken == "" { // listed all
			smsg.ContinuationToken = ""
			break
		}

		rem = uint(cmn.Max(int(rem)-len(page.Entries), 0))
		cmn.Assert(page.UUID != "")
		smsg.UUID = page.UUID
		smsg.ContinuationToken = page.ContinuationToken
	}

	return bckList, err
}

// ListObjectsPage returns the first page of bucket objects.
// On success the function updates `smsg.ContinuationToken`, so a client can reuse
// the message to fetch the next page.
func ListObjectsPage(baseParams BaseParams, bck cmn.Bck, smsg *cmn.SelectMsg) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodPost
	if smsg == nil {
		smsg = &cmn.SelectMsg{}
	}

	var (
		actMsg    = cmn.ActionMsg{Action: cmn.ActListObjects, Value: smsg}
		reqParams = ReqParams{
			BaseParams: baseParams,
			Path:       cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name),
			Header:     http.Header{cmn.HeaderAccept: []string{cmn.ContentMsgPack}},
			Query:      cmn.AddBckToQuery(url.Values{}, bck),
			Body:       cmn.MustMarshal(actMsg),
		}
	)

	// NOTE: No need to preallocate bucket entries slice, we use msgpack so it will do it for us!
	page := &cmn.BucketList{}
	if _, err := doHTTPRequestGetResp(reqParams, page); err != nil {
		return nil, err
	}
	smsg.UUID = page.UUID
	smsg.ContinuationToken = page.ContinuationToken
	return page, nil
}

// TODO: remove this function after introducing mechanism detecting bucket changes.
func ListObjectsInvalidateCache(params BaseParams, bck cmn.Bck) error {
	params.Method = http.MethodPost
	var (
		path = cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name)
		q    = url.Values{}
	)
	return DoHTTPRequest(ReqParams{
		Query:      cmn.AddBckToQuery(q, bck),
		BaseParams: params,
		Path:       path,
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActInvalListCache}),
	})
}

// Handles the List/Range operations (delete, prefetch)
func doListRangeRequest(baseParams BaseParams, bck cmn.Bck, action string, listRangeMsg interface{}) (xactID string, err error) {
	switch action {
	case cmn.ActDelete, cmn.ActEvictObjects:
		baseParams.Method = http.MethodDelete
	case cmn.ActPrefetch:
		baseParams.Method = http.MethodPost
	default:
		err = fmt.Errorf("invalid action %q", action)
		return
	}
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: action, Value: listRangeMsg}),
		Header: http.Header{
			cmn.HeaderContentType: []string{cmn.ContentJSON},
		},
		Query: cmn.AddBckToQuery(nil, bck),
	}, &xactID)
	return
}

func ECEncodeBucket(baseParams BaseParams, bck cmn.Bck, data, parity int) (xactID string, err error) {
	baseParams.Method = http.MethodPost
	// Without `string` conversion it makes base64 from []byte in `Body`.
	ecConf := string(cmn.MustMarshal(&cmn.ECConfToUpdate{DataSlices: &data, ParitySlices: &parity}))
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActECEncode, Value: ecConf}),
		Query:      cmn.AddBckToQuery(nil, bck),
	}, &xactID)
	return
}

func NewProgressContext(cb ProgressCallback, after time.Duration) *ProgressContext {
	ctx := &ProgressContext{
		info:      ProgressInfo{Count: -1, Total: -1, Percent: -1.0},
		startTime: time.Now(),
		callback:  cb,
	}
	if after != 0 {
		ctx.callAfter = ctx.startTime.Add(after)
	}
	return ctx
}

func (ctx *ProgressContext) finish() {
	ctx.info.Percent = 100.0
	if ctx.info.Total > 0 {
		ctx.info.Count = ctx.info.Total
	}
}

func (ctx *ProgressContext) IsFinished() bool {
	return ctx.info.Percent >= 100.0 ||
		(ctx.info.Total != 0 && ctx.info.Total == ctx.info.Count)
}

func (ctx *ProgressContext) Elapsed() time.Duration {
	return time.Since(ctx.startTime)
}

func (ctx *ProgressContext) mustFire() bool {
	return ctx.callAfter.IsZero() ||
		ctx.callAfter.Before(time.Now())
}

func (ctx *ProgressContext) Info() ProgressInfo {
	return ctx.info
}
