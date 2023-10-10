// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	maxListPageRetries = 4

	msgpBufSize = 16 * cos.KiB
)

// additional and optional list-objets args (see also: GetArgs, PutArgs)
type ListArgs struct {
	Progress *ProgressContext // with a callback
	Num      uint             // aka limit
}

// ListBuckets returns buckets for provided query, where
//   - `fltPresence` is one of { apc.FltExists, apc.FltPresent, ... } - see api/apc/query.go
//   - ListBuckets utiizes `cmn.QueryBcks` - control structure that's practically identical to `cmn.Bck`,
//     except for the fact that some or all its fields can be empty (to facilitate the corresponding
//     query).
//
// See also: QueryBuckets, ListObjects
func ListBuckets(bp BaseParams, qbck cmn.QueryBcks, fltPresence int) (cmn.Bcks, error) {
	q := make(url.Values, 4)
	q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence))
	q = qbck.AddToQuery(q)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.S
		// NOTE: bucket name
		// - qbck.IsBucket() to differentiate between list-objects and list-buckets (operations)
		// - list-buckets own correctness (see QueryBuckets below)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActList, Name: qbck.Name})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	bcks := cmn.Bcks{}
	_, err := reqParams.DoReqAny(&bcks)
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	return bcks, nil
}

// QueryBuckets is a little convenience helper. It returns true if the selection contains
// at least one bucket that satisfies the (qbck) criteria.
// - `fltPresence` - as per QparamFltPresence enum (see api/apc/query.go)
func QueryBuckets(bp BaseParams, qbck cmn.QueryBcks, fltPresence int) (bool, error) {
	bcks, err := ListBuckets(bp, qbck, fltPresence)
	return len(bcks) > 0, err
}

// GetBucketSummary returns bucket summaries (capacity ulitization percentages, sizes, and
// numbers of objects) for the specified bucket or buckets, as per `cmn.QueryBcks` query.
// E.g., an empty bucket query corresponds to all buckets present in the cluster's metadata.
func GetBucketSummary(bp BaseParams, qbck cmn.QueryBcks, msg *apc.BsummCtrlMsg) (cmn.AllBsummResults, error) {
	if msg == nil {
		msg = &apc.BsummCtrlMsg{ObjCached: true, BckPresent: true} // NOTE the defaults
	}
	bp.Method = http.MethodGet

	reqParams := AllocRp()
	summaries := cmn.AllBsummResults{}
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(qbck.Name)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = qbck.NewQuery()
	}
	// execute `apc.ActSummaryBck` and poll for results
	if err := reqParams.waitBsumm(msg, &summaries); err != nil {
		FreeRp(reqParams)
		return nil, err
	}
	sort.Sort(summaries)
	FreeRp(reqParams)
	return summaries, nil
}

// ListObjects returns a list of objects in a bucket - a slice of structures in the
// `cmn.LsoResult` that look like `cmn.LsoEntry`.
//
// The `numObjects` argument is the maximum number of objects to be returned
// (where 0 (zero) means returning all objects in the bucket).
//
// This API supports numerous options and flags. In particular, `apc.LsoMsg`
// structure supports "opening" objects formatted as one of the supported
// archival types and include contents of archived directories in generated
// result sets.
//
// In addition, `lsmsg` (`apc.LsoMsg`) provides options (flags) to optimize
// the request's latency, to list anonymous public-access Cloud buckets, and more.
// Further details at `api/apc/lsmsg.go` source.
//
// AIS supports listing buckets that have millions of objects.
// For large and very large buckets, it is strongly recommended to use the
// `ListObjectsPage` API - effectively, an iterator returning _next_
// listed page along with associated _continuation token_.
//
// See also:
// - `ListObjectsPage`
// - usage examples in CLI docs under docs/cli.
func ListObjects(bp BaseParams, bck cmn.Bck, lsmsg *apc.LsoMsg, args ListArgs) (*cmn.LsoResult, error) {
	var (
		q    url.Values
		path = apc.URLPathBuckets.Join(bck.Name)
		hdr  = http.Header{
			cos.HdrAccept:      []string{cos.ContentMsgPack},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
	)
	bp.Method = http.MethodGet
	if lsmsg == nil {
		lsmsg = &apc.LsoMsg{}
	}
	q = bck.AddToQuery(q)
	lsmsg.UUID = ""
	lsmsg.ContinuationToken = ""
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
		reqParams.Header = hdr
		reqParams.Query = q

		reqParams.buf = allocMbuf() // mem-pool msgpack
	}
	lst, err := lso(reqParams, lsmsg, args)

	freeMbuf(reqParams.buf)
	FreeRp(reqParams)
	return lst, err
}

// `toRead` holds the remaining number of objects to list (that is, unless we are listing
// the entire bucket). Each iteration lists a page of objects and reduces `toRead`
// accordingly. When the latter gets below page size, we perform the final
// iteration for the reduced page.
func lso(reqParams *ReqParams, lsmsg *apc.LsoMsg, args ListArgs) (*cmn.LsoResult, error) {
	var (
		lst      = &cmn.LsoResult{}
		nextPage = &cmn.LsoResult{}
		progress = args.Progress
		toRead   = args.Num
		listAll  = args.Num == 0
	)
	for pageNum := 1; listAll || toRead > 0; pageNum++ {
		if !listAll {
			lsmsg.PageSize = toRead
		}
		actMsg := apc.ActMsg{Action: apc.ActList, Value: lsmsg}
		reqParams.Body = cos.MustMarshal(actMsg)
		page := nextPage
		if pageNum == 1 {
			page = lst
		} else {
			page.Entries = nil
		}

		// w/ limited retry and increasing timeout
		for i := 0; i < maxListPageRetries; i++ {
			_, err := reqParams.DoReqAny(page)
			if err == nil {
				break
			}
			if errors.Is(err, context.DeadlineExceeded) && i < maxListPageRetries-1 {
				client := *reqParams.BaseParams.Client
				client.Timeout = 2 * client.Timeout
				reqParams.BaseParams.Client = &client
				continue
			}
			return nil, err
		}

		lst.Flags |= page.Flags
		// first page appends to an empty `lst`, otherwise:
		if pageNum > 1 {
			lst.Entries = append(lst.Entries, page.Entries...)
			lst.ContinuationToken = page.ContinuationToken
		}
		if progress != nil && progress.mustFire() {
			progress.info.Count = len(lst.Entries)
			if page.ContinuationToken == "" {
				progress.finish()
			}
			progress.callback(progress)
		}
		if page.ContinuationToken == "" { // listed all pages
			lsmsg.ContinuationToken = ""
			break
		}
		toRead = uint(max(int(toRead)-len(page.Entries), 0))
		debug.Assert(cos.IsValidUUID(page.UUID))
		lsmsg.UUID = page.UUID
		lsmsg.ContinuationToken = page.ContinuationToken
	}
	return lst, nil
}

// ListObjectsPage returns the first page of bucket objects.
// On success the function updates `lsmsg.ContinuationToken` which client then can reuse
// to fetch the next page.
// See also: CLI and CLI usage examples
// See also: `apc.LsoMsg`
// See also: `api.ListObjectsInvalidateCache`
// See also: `api.ListObjects`
func ListObjectsPage(bp BaseParams, bck cmn.Bck, lsmsg *apc.LsoMsg) (*cmn.LsoResult, error) {
	bp.Method = http.MethodGet
	if lsmsg == nil {
		lsmsg = &apc.LsoMsg{}
	}
	actMsg := apc.ActMsg{Action: apc.ActList, Value: lsmsg}
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

		reqParams.buf = allocMbuf() // mem-pool msgpack
	}
	// no need to preallocate bucket entries slice (msgpack does it)
	page := &cmn.LsoResult{}
	_, err := reqParams.DoReqAny(page)
	freeMbuf(reqParams.buf)
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
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActInvalListCache})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}
