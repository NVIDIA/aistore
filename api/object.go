// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	httpMaxRetries = 5                      // maximum number of retries for an HTTP request
	httpRetrySleep = 100 * time.Millisecond // a sleep between HTTP request retries
	// Sleep between HTTP retries for error[rate of change requests exceeds limit] - must be > 1s:
	// From https://cloud.google.com/storage/quotas#objects
	//   There is an update limit on each object of once per second ...
	httpRetryRateSleep = 1500 * time.Millisecond
)

type (
	GetObjectInput struct {
		// If not specified otherwise, the Writer field defaults to io.Discard
		Writer io.Writer
		// Map of strings as keys and string slices as values used for url formulation
		Query url.Values
		// Custom header values passed with GET request
		Header http.Header
	}
	PutObjectArgs struct {
		BaseParams BaseParams
		Bck        cmn.Bck
		Object     string
		Cksum      *cos.Cksum
		Reader     cos.ReadOpenCloser
		Size       uint64 // optional
	}
	AppendToArchArgs struct {
		PutObjectArgs
		ArchPath string
	}
	PromoteArgs struct {
		BaseParams BaseParams
		Bck        cmn.Bck
		Object     string
		Target     string
		FQN        string
		Recursive  bool
		Overwrite  bool
		KeepOrig   bool
	}
	AppendArgs struct {
		BaseParams BaseParams
		Bck        cmn.Bck
		Object     string
		Handle     string
		Reader     cos.ReadOpenCloser
		Size       int64
	}
	FlushArgs struct {
		BaseParams BaseParams
		Bck        cmn.Bck
		Object     string
		Handle     string
		Cksum      *cos.Cksum
	}
	ReplicateObjectInput struct { // TODO: obsolete - remove
		SourceURL string
	}
)

// HeadObject returns the size and version of the object specified by bucket/object.
func HeadObject(baseParams BaseParams, bck cmn.Bck, object string, checkExists ...bool) (*cmn.ObjectProps, error) {
	var (
		q             url.Values
		checkIsCached bool
	)
	if len(checkExists) > 0 {
		checkIsCached = checkExists[0]
	}
	baseParams.Method = http.MethodHead

	if checkIsCached {
		q = make(url.Values, 4)
		q = cmn.AddBckToQuery(q, bck)
		q.Set(cmn.URLParamCheckExists, "true")
		q.Set(cmn.URLParamSilent, "true")
	} else {
		q = cmn.AddBckToQuery(nil, bck)
	}

	resp, err := doResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathObjects.Join(bck.Name, object),
		Query:      q,
	}, nil)
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
// See also: HeadObject() and cmn.HdrObjCustomMD
func SetObjectCustomProps(baseParams BaseParams, bck cmn.Bck, object string, custom cos.SimpleKVs, setNew bool) error {
	var (
		actMsg = cmn.ActionMsg{Value: custom}
		q      url.Values
	)
	if setNew {
		q = make(url.Values, 4)
		q = cmn.AddBckToQuery(q, bck)
		q.Set(cmn.URLParamNewCustom, "true")
	} else {
		q = cmn.AddBckToQuery(q, bck)
	}
	baseParams.Method = http.MethodPatch
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathObjects.Join(bck.Name, object),
		Body:       cos.MustMarshal(actMsg),
		Query:      q,
	})
}

// DeleteObject deletes an object specified by bucket/object.
func DeleteObject(baseParams BaseParams, bck cmn.Bck, object string) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathObjects.Join(bck.Name, object),
		Query:      cmn.AddBckToQuery(nil, bck),
	})
}

// EvictObject evicts an object specified by bucket/object.
func EvictObject(baseParams BaseParams, bck cmn.Bck, object string) error {
	baseParams.Method = http.MethodDelete
	actMsg := cmn.ActionMsg{Action: cmn.ActEvictObjects, Name: cos.JoinWords(bck.Name, object)}
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathObjects.Join(bck.Name, object),
		Body:       cos.MustMarshal(actMsg),
		Query:      cmn.AddBckToQuery(nil, bck),
	})
}

// GetObject returns the length of the object. Does not validate checksum of the
// object in the response.
//
// Writes the response body to a writer if one is specified in the optional
// `GetObjectInput.Writer`. Otherwise, it discards the response body read.
//
// `io.Copy` is used internally to copy response bytes from the request to the writer.
func GetObject(baseParams BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (n int64, err error) {
	var (
		w   = io.Discard
		q   url.Values
		hdr http.Header
	)
	if len(options) != 0 {
		w, q, hdr = getObjectOptParams(options[0])
	}
	baseParams.Method = http.MethodGet
	resp, err := doResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathObjects.Join(bck.Name, object),
		Query:      cmn.AddBckToQuery(q, bck),
		Header:     hdr,
	}, w)
	if err != nil {
		return 0, err
	}
	return resp.n, nil
}

// GetObjectReader returns reader of the requested object. It does not read body
// bytes, nor validates a checksum. Caller is responsible for closing the reader.
func GetObjectReader(baseParams BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (r io.ReadCloser,
	err error) {
	var (
		q   url.Values
		hdr http.Header
	)
	if len(options) != 0 {
		var w io.Writer
		w, q, hdr = getObjectOptParams(options[0])
		cos.Assert(w == nil)
	}
	q = cmn.AddBckToQuery(q, bck)
	baseParams.Method = http.MethodGet
	return doReader(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathObjects.Join(bck.Name, object),
		Query:      q,
		Header:     hdr,
	})
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
func GetObjectWithValidation(baseParams BaseParams, bck cmn.Bck, object string,
	options ...GetObjectInput) (n int64, err error) {
	var (
		w   = io.Discard
		q   url.Values
		hdr http.Header
	)
	if len(options) != 0 {
		w, q, hdr = getObjectOptParams(options[0])
	}
	baseParams.Method = http.MethodGet

	resp, err := doResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathObjects.Join(bck.Name, object),
		Query:      cmn.AddBckToQuery(q, bck),
		Header:     hdr,
		Validate:   true,
	}, w)
	if err != nil {
		return 0, err
	}

	hdrCksumValue := resp.Header.Get(cmn.HdrObjCksumVal)
	if resp.cksumValue != hdrCksumValue {
		return 0, cmn.NewErrInvalidCksum(hdrCksumValue, resp.cksumValue)
	}
	return resp.n, nil
}

// GetObjectWithResp returns the response of the request and length of the object.
// Does not validate checksum of the object in the response.
//
// Writes the response body to a writer if one is specified in the optional
// `GetObjectInput.Writer`. Otherwise, it discards the response body read.
//
// `io.Copy` is used internally to copy response bytes from the request to the writer.
func GetObjectWithResp(baseParams BaseParams, bck cmn.Bck, object string,
	options ...GetObjectInput) (*http.Response, int64, error) {
	var (
		w   = io.Discard
		q   url.Values
		hdr http.Header
	)
	if len(options) != 0 {
		w, q, hdr = getObjectOptParams(options[0])
	}
	q = cmn.AddBckToQuery(q, bck)
	baseParams.Method = http.MethodGet
	resp, err := doResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathObjects.Join(bck.Name, object),
		Query:      q,
		Header:     hdr,
	}, w)
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
	query := cmn.AddBckToQuery(nil, args.Bck)
	return _putObject(args, query)
}

func _putObject(args PutObjectArgs, query url.Values) (err error) {
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   args.BaseParams.URL,
		Path:   cmn.URLPathObjects.Join(args.Bck.Name, args.Object),
		Query:  query,
		BodyR:  args.Reader,
	}

	newRequest := func(reqArgs cmn.ReqArgs) (*http.Request, error) {
		req, err := reqArgs.Req()
		if err != nil {
			return nil, newErrCreateHTTPRequest(err)
		}

		// The HTTP package doesn't automatically set this for files, so it has to be done manually
		// If it wasn't set, we would need to deal with the redirect manually.
		req.GetBody = func() (io.ReadCloser, error) {
			return args.Reader.Open()
		}
		if args.Cksum != nil && args.Cksum.Ty() != cos.ChecksumNone {
			req.Header.Set(cmn.HdrObjCksumType, args.Cksum.Ty())
			ckVal := args.Cksum.Value()
			if ckVal == "" {
				_, ckhash, err := cos.CopyAndChecksum(io.Discard, args.Reader, nil, args.Cksum.Ty())
				if err != nil {
					return nil, newErrCreateHTTPRequest(err)
				}
				ckVal = hex.EncodeToString(ckhash.Sum())
			}
			req.Header.Set(cmn.HdrObjCksumVal, ckVal)
		}
		if args.Size != 0 {
			req.ContentLength = int64(args.Size) // as per https://tools.ietf.org/html/rfc7230#section-3.3.2
		}

		setAuthToken(req, args.BaseParams)
		return req, nil
	}
	_, err = DoReqWithRetry(args.BaseParams.Client, newRequest, reqArgs) // nolint:bodyclose // is closed inside
	return err
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
	q = cmn.AddBckToQuery(q, args.Bck)
	q.Set(cmn.URLParamArchpath, args.ArchPath)
	q.Set(cmn.URLParamArchmime, m)
	return _putObject(args.PutObjectArgs, q)
}

// AppendObject adds a reader (`args.Reader` - e.g., an open file) to an object.
// The API can be called multiple times - each call returns a handle
// that may be used for subsequent append requests.
// Once all the "appending" is done, the caller must call `api.FlushObject`
// to finalize the object.
// NOTE: object becomes visible and accessible only _after_ the call to `api.FlushObject`.
func AppendObject(args AppendArgs) (handle string, err error) {
	q := make(url.Values, 4)
	q.Set(cmn.URLParamAppendType, cmn.AppendOp)
	q.Set(cmn.URLParamAppendHandle, args.Handle)
	q = cmn.AddBckToQuery(q, args.Bck)

	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   args.BaseParams.URL,
		Path:   cmn.URLPathObjects.Join(args.Bck.Name, args.Object),
		Query:  q,
		BodyR:  args.Reader,
	}

	newRequest := func(reqArgs cmn.ReqArgs) (*http.Request, error) {
		req, err := reqArgs.Req()
		if err != nil {
			return nil, newErrCreateHTTPRequest(err)
		}

		// The HTTP package doesn't automatically set this for files, so it has to be done manually
		// If it wasn't set, we would need to deal with the redirect manually.
		req.GetBody = func() (io.ReadCloser, error) {
			return args.Reader.Open()
		}
		if args.Size != 0 {
			req.ContentLength = args.Size // as per https://tools.ietf.org/html/rfc7230#section-3.3.2
		}

		setAuthToken(req, args.BaseParams)
		return req, nil
	}

	resp, err := DoReqWithRetry(args.BaseParams.Client, newRequest, reqArgs) // nolint:bodyclose // it's closed inside
	if err != nil {
		return "", fmt.Errorf("failed to %s, err: %v", http.MethodPut, err)
	}
	return resp.Header.Get(cmn.HdrAppendHandle), err
}

// FlushObject must be called after all the appends (via `api.AppendObject`).
// To "flush", it uses the handle returned by `api.AppendObject`.
// This call will create a fully operational and accessible object.
func FlushObject(args FlushArgs) (err error) {
	var (
		header http.Header
		q      = make(url.Values, 4)
	)
	q.Set(cmn.URLParamAppendType, cmn.FlushOp)
	q.Set(cmn.URLParamAppendHandle, args.Handle)
	q = cmn.AddBckToQuery(q, args.Bck)

	if args.Cksum != nil && args.Cksum.Ty() != cos.ChecksumNone {
		header = make(http.Header)
		header.Set(cmn.HdrObjCksumType, args.Cksum.Ty())
		header.Set(cmn.HdrObjCksumVal, args.Cksum.Val())
	}
	args.BaseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: args.BaseParams,
		Path:       cmn.URLPathObjects.Join(args.Bck.Name, args.Object),
		Query:      q,
		Header:     header,
	})
}

// RenameObject renames object name from `oldName` to `newName`. Works only
// across single, specified bucket.
func RenameObject(baseParams BaseParams, bck cmn.Bck, oldName, newName string) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathObjects.Join(bck.Name, oldName),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActRenameObject, Name: newName}),
		Query:      cmn.AddBckToQuery(nil, bck),
	})
}

// PromoteFileOrDir promotes AIS-colocated files and directories to objects.
// NOTE: advanced usage.
func PromoteFileOrDir(args *PromoteArgs) error {
	actMsg := cmn.ActionMsg{Action: cmn.ActPromote, Name: args.FQN}
	actMsg.Value = &cmn.ActValPromote{
		Target:    args.Target,
		ObjName:   args.Object,
		Recursive: args.Recursive,
		Overwrite: args.Overwrite,
		KeepOrig:  args.KeepOrig,
	}

	args.BaseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: args.BaseParams,
		Path:       cmn.URLPathObjects.Join(args.Bck.Name),
		Body:       cos.MustMarshal(actMsg),
		Query:      cmn.AddBckToQuery(nil, args.Bck),
	})
}

// DoReqWithRetry makes `client.Do` request and retries it when got "Broken Pipe"
// or "Connection Refused" error.
//
// This function always closes the `reqArgs.BodR`, even in case of error.
//
// Should be used for PUT requests as it puts reader into a request.
//
// NOTE: always closes request body reader (reqArgs.BodyR) - explicitly or via Do()
// TODO: this code must be totally revised
func DoReqWithRetry(client *http.Client, newRequest func(_ cmn.ReqArgs) (*http.Request, error),
	reqArgs cmn.ReqArgs) (resp *http.Response, err error) {
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
	if req, err = newRequest(reqArgs); err != nil {
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

		if req, err = newRequest(reqArgs); err != nil {
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
	_, err = readResp(ReqParams{}, resp, nil)
	return
}

func shouldRetryHTTP(err error, resp *http.Response) bool {
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	return err != nil && cos.IsRetriableConnErr(err)
}
