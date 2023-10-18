// Package api provides Go based AIStore API/SDK over HTTP(S)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
)

type (
	BinfoArgs struct {
		Callback      BsummCB
		CallAfter     time.Duration
		FltPresence   int
		Summarize     bool
		WithRemote    bool
		DontAddRemote bool
	}
)

// SetBucketProps sets the properties of a bucket.
// Validation of the properties passed in is performed by AIStore Proxy.
func SetBucketProps(bp BaseParams, bck cmn.Bck, props *cmn.BucketPropsToUpdate) (string, error) {
	b := cos.MustMarshal(apc.ActMsg{Action: apc.ActSetBprops, Value: props})
	return patchBprops(bp, bck, b)
}

// ResetBucketProps resets the properties of a bucket to the global configuration.
func ResetBucketProps(bp BaseParams, bck cmn.Bck) (string, error) {
	b := cos.MustMarshal(apc.ActMsg{Action: apc.ActResetBprops})
	return patchBprops(bp, bck, b)
}

func patchBprops(bp BaseParams, bck cmn.Bck, body []byte) (xid string, err error) {
	bp.Method = http.MethodPatch
	path := apc.URLPathBuckets.Join(bck.Name)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
		reqParams.Body = body
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.NewQuery()
	}
	_, err = reqParams.doReqStr(&xid)
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
// Use `dontAddRemote` to override the default behavior: as the name implies, setting
// `dontAddRemote = true` prevents AIS from adding remote bucket to the cluster's metadata.
func HeadBucket(bp BaseParams, bck cmn.Bck, dontAddRemote bool) (p *cmn.BucketProps, err error) {
	var (
		hdr    http.Header
		path   = apc.URLPathBuckets.Join(bck.Name)
		q      = make(url.Values, 4)
		status int
	)
	if dontAddRemote {
		q.Set(apc.QparamDontAddRemote, "true")
	}
	q = bck.AddToQuery(q)

	bp.Method = http.MethodHead
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
		reqParams.Query = q
	}
	if hdr, status, err = reqParams.doReqHdr(); err == nil {
		p = &cmn.BucketProps{}
		err = jsoniter.Unmarshal([]byte(hdr.Get(apc.HdrBucketProps)), p)
	} else {
		err = hdr2msg(bck, status, err)
	}
	FreeRp(reqParams)
	return
}

// Bucket information - a runtime addendum to `BucketProps`.
// In addition to `cmn.BucketProps` properties (which are user configurable), bucket runtime info:
// - includes usage, capacity, other statistics
// - is obtained via GetBucketInfo() API
// - and delivered via apc.HdrBucketInfo header (compare with GetBucketSummary)
// The API uses http.MethodHead and can be considered an extension of HeadBucket (above)
func GetBucketInfo(bp BaseParams, bck cmn.Bck, args BinfoArgs) (*cmn.BucketProps, *cmn.BsummResult, error) {
	q := make(url.Values, 4)
	q = bck.AddToQuery(q)
	q.Set(apc.QparamFltPresence, strconv.Itoa(args.FltPresence))
	if args.DontAddRemote {
		q.Set(apc.QparamDontAddRemote, "true")
	}
	if args.Summarize {
		if args.WithRemote {
			q.Set(apc.QparamBsummRemote, "true")
		} else {
			q.Set(apc.QparamBsummRemote, "false")
		}
	}
	bp.Method = http.MethodHead
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Query = q
	}
	p, info, err := _binfo(reqParams, bck, args)
	FreeRp(reqParams)
	return p, info, err
}

// compare w/ _bsumm
func _binfo(reqParams *ReqParams, bck cmn.Bck, args BinfoArgs) (p *cmn.BucketProps, info *cmn.BsummResult, err error) {
	var (
		hdr          http.Header
		status       int
		start, after int64
		sleep        = xact.MinPollTime
	)
	if hdr, status, err = reqParams.doReqHdr(); err != nil {
		err = hdr2msg(bck, status, err)
		return
	}

	hdrProps := hdr.Get(apc.HdrBucketProps)
	if hdrProps != "" {
		p = &cmn.BucketProps{}
		if err = jsoniter.Unmarshal([]byte(hdrProps), p); err != nil {
			return
		}
	}

	uuid := hdr.Get(apc.HdrXactionID)
	if uuid == "" {
		debug.Assert(status == http.StatusOK && !args.Summarize)
		return
	}

	if status != http.StatusAccepted {
		err = fmt.Errorf("invalid response code: expecting %d, got %d", http.StatusAccepted, status)
		return
	}
	if args.Callback != nil {
		start = mono.NanoTime()
		after = start + args.CallAfter.Nanoseconds()
	}

	reqParams.Query.Set(apc.QparamUUID, uuid)

	time.Sleep(sleep)
	for i := 0; ; i++ {
		if hdr, status, err = reqParams.doReqHdr(); err != nil {
			err = hdr2msg(bck, status, err)
			return
		}

		hdrSumm := hdr.Get(apc.HdrBucketSumm)
		if hdrSumm != "" {
			info = &cmn.BsummResult{}
			err = jsoniter.Unmarshal([]byte(hdrSumm), info)
		}
		if err != nil {
			return // unlikely
		}
		debug.Assertf(hdr.Get(apc.HdrXactionID) == uuid, "%q vs %q", hdr.Get(apc.HdrXactionID), uuid)

		// callback w/ partial results
		if args.Callback != nil && (status == http.StatusPartialContent || status == http.StatusOK) {
			if after == start || mono.NanoTime() >= after {
				res := cmn.AllBsummResults{info}
				args.Callback(&res, status == http.StatusOK)
			}
		}
		if status == http.StatusOK {
			return
		}

		time.Sleep(sleep)
		// inc. sleep time if there's nothing at all
		if i == 8 && status != http.StatusPartialContent {
			debug.Assert(status == http.StatusAccepted)
			sleep *= 2
		} else if i == 16 && status != http.StatusPartialContent {
			sleep *= 2
		}
	}
}

// fill-in error message (HEAD response will never contain one)
func hdr2msg(bck cmn.Bck, status int, err error) error {
	herr := cmn.Err2HTTPErr(err)
	if herr == nil {
		debug.FailTypeCast(err)
		return err
	}
	debug.Assertf(herr.Status == status, "status %d vs %d", herr.Status, status) // TODO -- FIXME: test, refactor
	switch herr.Status {
	case http.StatusUnauthorized:
		herr.Message = fmt.Sprintf("Bucket %q unauthorized access", bck)
	case http.StatusForbidden:
		herr.Message = fmt.Sprintf("Bucket %q access denied", bck)
	case http.StatusGone:
		herr.Message = fmt.Sprintf("Bucket %q has been removed from the backend", bck)
	}
	return herr
}

// CreateBucket sends request to create an AIS bucket with the given name and,
// optionally, specific non-default properties (via cmn.BucketPropsToUpdate).
//
// See also:
//   - github.com/NVIDIA/aistore/blob/master/docs/bucket.md#default-bucket-properties
//   - cmn.BucketPropsToUpdate (cmn/api.go)
//
// Bucket properties can be also changed at any time via SetBucketProps (above).
func CreateBucket(bp BaseParams, bck cmn.Bck, props *cmn.BucketPropsToUpdate, dontHeadRemote ...bool) error {
	if err := bck.Validate(); err != nil {
		return err
	}
	q := make(url.Values, 4)
	if len(dontHeadRemote) > 0 && dontHeadRemote[0] {
		q.Set(apc.QparamDontHeadRemote, "true")
	}
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActCreateBck, Value: props})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(q)
	}
	err := reqParams.DoRequest()
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
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActDestroyBck})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.NewQuery()
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// CopyBucket copies existing `bckFrom` bucket to the destination `bckTo` thus,
// effectively, creating a copy of the `bckFrom`.
//   - AIS will create `bckTo` on the fly but only if the destination bucket does not
//     exist and _is_ provided by AIStore; 3rd party backend destination must exist -
//     otherwise the copy operation won't be successful.
//   - There are no limitations on copying buckets across Backend providers:
//     you can copy AIS bucket to (or from) AWS bucket, and the latter to Google or Azure
//     bucket, etc.
//   - Copying multiple buckets to the same destination bucket is also permitted.
//
// `fltPresence` applies exclusively to remote `bckFrom` and is ignored if the source is ais://
// The value is enum { apc.FltExists, apc.FltPresent, ... } - for complete enum, see api/apc/query.go
// Namely:
// * apc.FltExists        - copy all objects, including those that are not (present) in AIS
// * apc.FltPresent 	  - copy the current `bckFrom` content in the cluster (default)
// * apc.FltExistsOutside - copy only those remote objects that are not (present) in AIS
//
// msg.Prefix, if specified, applies always and regardless.
//
// Returns xaction ID if successful, an error otherwise. See also closely related api.ETLBucket
func CopyBucket(bp BaseParams, bckFrom, bckTo cmn.Bck, msg *apc.CopyBckMsg, fltPresence ...int) (xid string, err error) {
	if err = bckTo.Validate(); err != nil {
		return
	}
	q := bckFrom.NewQuery()
	_ = bckTo.AddUnameToQuery(q, apc.QparamBckTo)
	if len(fltPresence) > 0 {
		q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence[0]))
	}
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bckFrom.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActCopyBck, Value: msg})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return
}

// RenameBucket renames bckFrom as bckTo.
// Returns xaction ID if successful, an error otherwise.
func RenameBucket(bp BaseParams, bckFrom, bckTo cmn.Bck) (xid string, err error) {
	if err = bckTo.Validate(); err != nil {
		return
	}
	bp.Method = http.MethodPost
	q := bckFrom.NewQuery()
	_ = bckTo.AddUnameToQuery(q, apc.QparamBckTo)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bckFrom.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMoveBck})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return
}

// EvictRemoteBucket sends request to evict an entire remote bucket from the AIStore
// - keepMD: evict objects but keep bucket metadata
func EvictRemoteBucket(bp BaseParams, bck cmn.Bck, keepMD bool) error {
	var q url.Values
	if keepMD {
		q = url.Values{apc.QparamKeepRemote: []string{"true"}}
	}

	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActEvictRemoteBck})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(q)
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// MakeNCopies starts an extended action (xaction) to bring a given bucket to a
// certain redundancy level (num copies).
// Returns xaction ID if successful, an error otherwise.
func MakeNCopies(bp BaseParams, bck cmn.Bck, copies int) (xid string, err error) {
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMakeNCopies, Value: copies})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.NewQuery()
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return
}

// Erasure-code entire `bck` bucket at a given `data`:`parity` redundancy.
// The operation requires at least (`data + `parity` + 1) storage targets in the cluster.
// Returns xaction ID if successful, an error otherwise.
func ECEncodeBucket(bp BaseParams, bck cmn.Bck, data, parity int) (xid string, err error) {
	bp.Method = http.MethodPost
	// Without `string` conversion it makes base64 from []byte in `Body`.
	ecConf := string(cos.MustMarshal(&cmn.ECConfToUpdate{
		DataSlices:   &data,
		ParitySlices: &parity,
		Enabled:      apc.Bool(true),
	}))
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActECEncode, Value: ecConf})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.NewQuery()
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return
}
