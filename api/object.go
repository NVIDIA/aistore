// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	httpMaxRetries = 5                      // maximum number of retries for an HTTP request
	httpRetrySleep = 100 * time.Millisecond // a sleep between HTTP request retries
	// Sleep between HTTP retries for error[rate of change requests exceeds limit] - must be > 1s:
	// From https://cloud.google.com/storage/quotas#objects
	//    "There is an update limit on each object of once per second..."
	httpRetryRateSleep = 1500 * time.Millisecond
)

type (
	NewRequestCB func(args *cmn.HreqArgs) (*http.Request, error)

	GetObjectInput struct {
		// If not specified otherwise, the Writer field defaults to io.Discard
		Writer io.Writer
		// Map of strings as keys and string slices as values used for url formulation
		Query url.Values
		// Custom header values passed with GET request
		Header http.Header
	}
	PutObjectArgs struct {
		Reader cos.ReadOpenCloser
		Cksum  *cos.Cksum

		BaseParams BaseParams
		Bck        cmn.Bck

		Object string
		Size   uint64 // optional

		// Skip loading existing object's metadata in order to
		// compare its Checksum and update its existing Version (if exists);
		// can be used to reduce PUT latency when:
		// - we massively write a new content into a bucket, and/or
		// - we simply don't care.
		SkipVC bool
	}
	AppendToArchArgs struct {
		ArchPath string
		PutObjectArgs
	}
	PromoteArgs struct {
		BaseParams BaseParams
		Bck        cmn.Bck
		cluster.PromoteArgs
	}
	AppendArgs struct {
		Reader     cos.ReadOpenCloser
		BaseParams BaseParams
		Bck        cmn.Bck
		Object     string
		Handle     string
		Size       int64
	}
	FlushArgs struct {
		Cksum      *cos.Cksum
		BaseParams BaseParams
		Bck        cmn.Bck
		Object     string
		Handle     string
	}
)

///////////////////
// PutObjectArgs //
///////////////////

func (args *PutObjectArgs) getBody() (io.ReadCloser, error) { return args.Reader.Open() }

func (args *PutObjectArgs) put(reqArgs *cmn.HreqArgs) (*http.Request, error) {
	req, err := reqArgs.Req()
	if err != nil {
		return nil, newErrCreateHTTPRequest(err)
	}
	// Go http doesn't automatically set this for files, so to handle redirect we do it here.
	req.GetBody = args.getBody
	if args.Cksum != nil && args.Cksum.Ty() != cos.ChecksumNone {
		req.Header.Set(apc.HdrObjCksumType, args.Cksum.Ty())
		ckVal := args.Cksum.Value()
		if ckVal == "" {
			_, ckhash, err := cos.CopyAndChecksum(io.Discard, args.Reader, nil, args.Cksum.Ty())
			if err != nil {
				return nil, newErrCreateHTTPRequest(err)
			}
			ckVal = hex.EncodeToString(ckhash.Sum())
		}
		req.Header.Set(apc.HdrObjCksumVal, ckVal)
	}
	if args.Size != 0 {
		req.ContentLength = int64(args.Size) // as per https://tools.ietf.org/html/rfc7230#section-3.3.2
	}
	SetAuxHeaders(req, &args.BaseParams)
	return req, nil
}

////////////////
// AppendArgs //
////////////////

func (args *AppendArgs) getBody() (io.ReadCloser, error) { return args.Reader.Open() }

func (args *AppendArgs) _append(reqArgs *cmn.HreqArgs) (*http.Request, error) {
	req, err := reqArgs.Req()
	if err != nil {
		return nil, newErrCreateHTTPRequest(err)
	}
	// The HTTP package doesn't automatically set this for files, so it has to be done manually
	// If it wasn't set, we would need to deal with the redirect manually.
	req.GetBody = args.getBody
	if args.Size != 0 {
		req.ContentLength = args.Size // as per https://tools.ietf.org/html/rfc7230#section-3.3.2
	}
	SetAuxHeaders(req, &args.BaseParams)
	return req, nil
}

//
// object-level API
//

// HeadObject returns the object properties; can be conventionally used to establish
// local (in cluster) presence.
func HeadObject(bp BaseParams, bck cmn.Bck, object string, checkExists ...bool) (*cmn.ObjectProps, error) {
	var (
		q             url.Values
		checkIsCached bool
	)
	if len(checkExists) > 0 {
		checkIsCached = checkExists[0]
	}
	bp.Method = http.MethodHead

	if checkIsCached {
		q = make(url.Values, 4)
		q = bck.AddToQuery(q)
		q.Set(apc.QparamHeadObj, strconv.Itoa(apc.HeadObjAvoidRemote)) // TODO: support the entire enum
		q.Set(apc.QparamSilent, "true")
	} else {
		q = bck.AddToQuery(nil)
	}

	reqParams := AllocRp()
	defer FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, object)
		reqParams.Query = q
	}
	resp, err := reqParams.doResp(nil)
	if err != nil {
		return nil, err
	}
	if checkIsCached {
		return nil, err
	}

	// NOTE: compare with `headObject()` in target.go
	// first, cnm.ObjAttrs
	op := &cmn.ObjectProps{}
	op.Cksum = op.ObjAttrs.FromHeader(resp.Header)
	// second, all the rest
	err = cmn.IterFields(op, func(tag string, field cmn.IterField) (error, bool) {
		headerName := cmn.PropToHeader(tag)
		// skip the missing ones
		if _, ok := resp.Header[textproto.CanonicalMIMEHeaderKey(headerName)]; !ok {
			return nil, false
		}
		// single-value
		return field.SetValue(resp.Header.Get(headerName), true /*force*/), false
	}, cmn.IterOpts{OnlyRead: false})
	if err != nil {
		return nil, err
	}
	return op, nil
}

// Given cos.SimpleKVs (map[string]string) keys and values, sets object's custom properties.
// By default, adds new or updates existing custom keys.
// Use `setNewCustomMDFlag` to _replace_ all existing keys with the specified (new) ones.
// See also: HeadObject() and apc.HdrObjCustomMD
func SetObjectCustomProps(bp BaseParams, bck cmn.Bck, object string, custom cos.SimpleKVs, setNew bool) error {
	var (
		actMsg = apc.ActionMsg{Value: custom}
		q      url.Values
	)
	if setNew {
		q = make(url.Values, 4)
		q = bck.AddToQuery(q)
		q.Set(apc.QparamNewCustom, "true")
	} else {
		q = bck.AddToQuery(q)
	}
	bp.Method = http.MethodPatch
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, object)
		reqParams.Body = cos.MustMarshal(actMsg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// DeleteObject deletes an object specified by bucket/object.
func DeleteObject(bp BaseParams, bck cmn.Bck, object string) error {
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, object)
		reqParams.Query = bck.AddToQuery(nil)
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// EvictObject evicts an object specified by bucket/object.
func EvictObject(bp BaseParams, bck cmn.Bck, object string) error {
	bp.Method = http.MethodDelete
	actMsg := apc.ActionMsg{Action: apc.ActEvictObjects, Name: cos.JoinWords(bck.Name, object)}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, object)
		reqParams.Body = cos.MustMarshal(actMsg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(nil)
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// GetObject returns the length of the object. Does not validate checksum of the
// object in the response.
//
// Writes the response body to a writer if one is specified in the optional
// `GetObjectInput.Writer`. Otherwise, it discards the response body read.
//
// `io.Copy` is used internally to copy response bytes from the request to the writer.
func GetObject(bp BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (n int64, err error) {
	var (
		w   = io.Discard
		q   url.Values
		hdr http.Header
	)
	if len(options) != 0 {
		w, q, hdr = getObjectOptParams(options[0])
	}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, object)
		reqParams.Query = bck.AddToQuery(q)
		reqParams.Header = hdr
	}
	resp, err := reqParams.doResp(w)
	FreeRp(reqParams)
	if err != nil {
		return 0, err
	}
	return resp.n, nil
}

// GetObjectReader returns reader of the requested object. It does not read body
// bytes, nor validates a checksum. Caller is responsible for closing the reader.
func GetObjectReader(bp BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (r io.ReadCloser, err error) {
	var (
		q   url.Values
		hdr http.Header
	)
	if len(options) != 0 {
		var w io.Writer
		w, q, hdr = getObjectOptParams(options[0])
		cos.Assert(w == nil)
	}
	q = bck.AddToQuery(q)
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, object)
		reqParams.Query = q
		reqParams.Header = hdr
	}
	r, err = reqParams.doReader()
	FreeRp(reqParams)
	return
}

// GetObjectWithValidation has same behavior as GetObject, but performs checksum
// validation of the object by comparing the checksum in the response header
// with the calculated checksum value derived from the returned object.
//
// Similar to GetObject, if a memory manager/slab allocator is not specified, a
// temporary buffer is allocated when reading from the response body to compute
// the object checksum.
//
// Returns `cmn.ErrInvalidCksum` when the expected and actual checksum values
// are different.
func GetObjectWithValidation(bp BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (n int64, err error) {
	var (
		w   = io.Discard
		q   url.Values
		hdr http.Header
	)
	if len(options) != 0 {
		w, q, hdr = getObjectOptParams(options[0])
	}
	bp.Method = http.MethodGet

	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, object)
		reqParams.Query = bck.AddToQuery(q)
		reqParams.Header = hdr
		reqParams.Validate = true
	}
	resp, err := reqParams.doResp(w)
	FreeRp(reqParams)
	if err != nil {
		return 0, err
	}
	hdrCksumValue := resp.Header.Get(apc.HdrObjCksumVal)
	if resp.cksumValue != hdrCksumValue {
		return 0, cmn.NewErrInvalidCksum(hdrCksumValue, resp.cksumValue)
	}
	return resp.n, nil
}

// GetObjectWithResp returns the response and the length of the object.
// It does not validate the checksum of the object in the response.
//
// Writes the response body to a writer if one is specified in the optional
// `GetObjectInput.Writer`. Otherwise, it discards the response body read.
//
// `io.Copy` is used internally to copy response bytes from the request to the writer.
func GetObjectWithResp(bp BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (*http.Response,
	int64, error) {
	var (
		q   url.Values
		hdr http.Header
		w   = io.Discard
	)
	if len(options) != 0 {
		w, q, hdr = getObjectOptParams(options[0])
	}
	q = bck.AddToQuery(q)
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, object)
		reqParams.Query = q
		reqParams.Header = hdr
	}
	resp, err := reqParams.doResp(w)
	FreeRp(reqParams)
	if err != nil {
		return nil, 0, err
	}
	return resp.Response, resp.n, nil
}

// PutObject creates an object from the body of the reader (`args.Reader`) and puts
// it in the specified bucket.
//
// Assumes that `args.Reader` is already opened and ready for usage.
func PutObject(args PutObjectArgs) (err error) {
	query := args.Bck.AddToQuery(nil)
	if args.SkipVC {
		query.Set(apc.QparamSkipVC, "true")
	}
	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPut
		reqArgs.Base = args.BaseParams.URL
		reqArgs.Path = apc.URLPathObjects.Join(args.Bck.Name, args.Object)
		reqArgs.Query = query
		reqArgs.BodyR = args.Reader
	}
	_, err = DoWithRetry(args.BaseParams.Client, args.put, reqArgs) //nolint:bodyclose // is closed inside
	cmn.FreeHra(reqArgs)
	return
}

// Append the content of a reader (`args.Reader` - e.g., an open file) to an existing
// object formatted as one of the supported archives.
// In other words, append to an existing archive.
// For supported archival (mime) types, see cmn/cos/archive.go.
// NOTE: compare with:
//   - `api.CreateArchMultiObj`
//   - `api.AppendObject`
func AppendToArch(args AppendToArchArgs) (err error) {
	m, err := cos.Mime("", args.Object)
	if err != nil {
		return err
	}
	q := make(url.Values, 4)
	q = args.Bck.AddToQuery(q)
	q.Set(apc.QparamArchpath, args.ArchPath)
	q.Set(apc.QparamArchmime, m)
	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPut
		reqArgs.Base = args.BaseParams.URL
		reqArgs.Path = apc.URLPathObjects.Join(args.Bck.Name, args.Object)
		reqArgs.Query = q
		reqArgs.BodyR = args.Reader
	}
	putArgs := &args.PutObjectArgs
	_, err = DoWithRetry(args.BaseParams.Client, putArgs.put, reqArgs) //nolint:bodyclose // is closed inside
	cmn.FreeHra(reqArgs)
	return
}

// AppendObject adds a reader (`args.Reader` - e.g., an open file) to an object.
// The API can be called multiple times - each call returns a handle
// that may be used for subsequent append requests.
// Once all the "appending" is done, the caller must call `api.FlushObject`
// to finalize the object.
// NOTE: object becomes visible and accessible only _after_ the call to `api.FlushObject`.
func AppendObject(args AppendArgs) (string /*handle*/, error) {
	q := make(url.Values, 4)
	q.Set(apc.QparamAppendType, apc.AppendOp)
	q.Set(apc.QparamAppendHandle, args.Handle)
	q = args.Bck.AddToQuery(q)

	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPut
		reqArgs.Base = args.BaseParams.URL
		reqArgs.Path = apc.URLPathObjects.Join(args.Bck.Name, args.Object)
		reqArgs.Query = q
		reqArgs.BodyR = args.Reader
	}
	resp, err := DoWithRetry(args.BaseParams.Client, args._append, reqArgs) //nolint:bodyclose // it's closed inside
	cmn.FreeHra(reqArgs)
	if err != nil {
		return "", fmt.Errorf("failed to %s, err: %v", http.MethodPut, err)
	}
	return resp.Header.Get(apc.HdrAppendHandle), err
}

// FlushObject must be called after all the appends (via `api.AppendObject`).
// To "flush", it uses the handle returned by `api.AppendObject`.
// This call will create a fully operational and accessible object.
func FlushObject(args FlushArgs) error {
	var (
		header http.Header
		q      = make(url.Values, 4)
	)
	q.Set(apc.QparamAppendType, apc.FlushOp)
	q.Set(apc.QparamAppendHandle, args.Handle)
	q = args.Bck.AddToQuery(q)

	if args.Cksum != nil && args.Cksum.Ty() != cos.ChecksumNone {
		header = make(http.Header)
		header.Set(apc.HdrObjCksumType, args.Cksum.Ty())
		header.Set(apc.HdrObjCksumVal, args.Cksum.Val())
	}
	args.BaseParams.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = args.BaseParams
		reqParams.Path = apc.URLPathObjects.Join(args.Bck.Name, args.Object)
		reqParams.Query = q
		reqParams.Header = header
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// RenameObject renames object name from `oldName` to `newName`. Works only
// across single, specified bucket.
func RenameObject(bp BaseParams, bck cmn.Bck, oldName, newName string) error {
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, oldName)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActRenameObject, Name: newName})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.AddToQuery(nil)
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// promote files and directories to ais objects
func Promote(args *PromoteArgs) (xactID string, err error) {
	actMsg := apc.ActionMsg{Action: apc.ActPromote, Name: args.SrcFQN}
	actMsg.Value = &args.PromoteArgs
	args.BaseParams.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = args.BaseParams
		reqParams.Path = apc.URLPathObjects.Join(args.Bck.Name)
		reqParams.Body = cos.MustMarshal(actMsg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = args.Bck.AddToQuery(nil)
	}
	err = reqParams.DoHTTPReqResp(&xactID)
	FreeRp(reqParams)
	return
}

// DoWithRetry executes `http-client.Do` and retries *retriable connection errors*,
// such as "broken pipe" and "connection refused".
//
// This function always closes the `reqArgs.BodR`, even in case of error.
//
// Should be used for PUT requests as it puts reader into a request.
//
// NOTE: always closes request body reader (reqArgs.BodyR) - explicitly or via Do()
// TODO: this code must be totally revised
func DoWithRetry(client *http.Client, cb NewRequestCB, reqArgs *cmn.HreqArgs) (resp *http.Response, err error) {
	var (
		req    *http.Request
		doErr  error
		sleep  = httpRetrySleep
		reader = reqArgs.BodyR.(cos.ReadOpenCloser)
	)
	cleanup := func() {
		if resp != nil && doErr == nil {
			resp.Body.Close() // NOTE: not returning err close
		}
	}
	// first time
	if req, err = cb(reqArgs); err != nil {
		cos.Close(reader)
		return
	}
	resp, doErr = client.Do(req)
	err = doErr
	defer cleanup()
	if !shouldRetryHTTP(doErr, resp) {
		goto exit
	}
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		sleep = httpRetryRateSleep
	}
	// retry
	for i := 0; i < httpMaxRetries; i++ {
		var r io.ReadCloser
		time.Sleep(sleep)
		sleep += sleep / 2
		if r, err = reader.Open(); err != nil {
			return
		}
		reqArgs.BodyR = r

		if req, err = cb(reqArgs); err != nil {
			cos.Close(r)
			return
		}
		cleanup()
		resp, doErr = client.Do(req)
		err = doErr
		if !shouldRetryHTTP(doErr, resp) {
			goto exit
		}
	}
exit:
	if err != nil {
		return nil, fmt.Errorf("failed to %s, err: %v", reqArgs.Method, err)
	}
	reqParams := AllocRp()
	_, err = reqParams.readResp(resp, nil)
	FreeRp(reqParams)
	return
}

func shouldRetryHTTP(err error, resp *http.Response) bool {
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	return err != nil && cos.IsRetriableConnErr(err)
}
