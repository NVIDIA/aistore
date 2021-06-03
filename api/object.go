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
	"net/url"
	"strconv"
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

// GetObjectInput is used to hold optional parameters for GetObject and GetObjectWithValidation
type GetObjectInput struct {
	// If not specified otherwise, the Writer field defaults to io.Discard
	Writer io.Writer
	// Map of strings as keys and string slices as values used for url formulation
	Query url.Values
	// Custom header values passed with GET request
	Header http.Header
}

// ReplicateObjectInput is used to hold optional parameters for PutObject when it is used for replication
type ReplicateObjectInput struct {
	// Used to set the request header to determine whether PUT object request is for replication in AIStore
	SourceURL string
}

type PutObjectArgs struct {
	BaseParams BaseParams
	Bck        cmn.Bck
	Object     string
	Cksum      *cos.Cksum
	Reader     cos.ReadOpenCloser
	Size       uint64 // optional
}

type PromoteArgs struct {
	BaseParams BaseParams
	Bck        cmn.Bck
	Object     string
	Target     string
	FQN        string
	Recursive  bool
	Overwrite  bool
	KeepOrig   bool
}

type AppendArgs struct {
	BaseParams BaseParams
	Bck        cmn.Bck
	Object     string
	Handle     string
	Reader     cos.ReadOpenCloser
	Size       int64
}

type FlushArgs struct {
	BaseParams BaseParams
	Bck        cmn.Bck
	Object     string
	Handle     string
	Cksum      *cos.Cksum
}

// HeadObject returns the size and version of the object specified by bucket/object.
func HeadObject(baseParams BaseParams, bck cmn.Bck, object string, checkExists ...bool) (*cmn.ObjectProps, error) {
	checkIsCached := false
	if len(checkExists) > 0 {
		checkIsCached = checkExists[0]
	}
	baseParams.Method = http.MethodHead
	query := make(url.Values)
	query.Add(cmn.URLParamCheckExists, strconv.FormatBool(checkIsCached))
	query = cmn.AddBckToQuery(query, bck)

	resp, err := doHTTPRequestGetResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathObjects.Join(bck.Name, object),
		Query:      query,
	}, nil)
	if err != nil {
		return nil, err
	}
	if checkIsCached {
		return nil, err
	}

	objProps := &cmn.ObjectProps{}
	err = cmn.IterFields(objProps, func(tag string, field cmn.IterField) (error, bool) {
		headerName := cmn.PropToHeader(tag)
		return field.SetValue(resp.Header.Get(headerName), true /*force*/), false
	}, cmn.IterOpts{OnlyRead: false})
	if err != nil {
		return nil, err
	}
	return objProps, nil
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
	resp, err := doHTTPRequestGetResp(ReqParams{
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
func GetObjectReader(baseParams BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (r io.ReadCloser, err error) {
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
	return doHTTPRequestGetRespReader(ReqParams{
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
func GetObjectWithValidation(baseParams BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (n int64, err error) {
	var (
		w   = io.Discard
		q   url.Values
		hdr http.Header
	)
	if len(options) != 0 {
		w, q, hdr = getObjectOptParams(options[0])
	}
	baseParams.Method = http.MethodGet

	resp, err := doHTTPRequestGetResp(ReqParams{
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
		return 0, cmn.NewInvalidCksumError(hdrCksumValue, resp.cksumValue)
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
func GetObjectWithResp(baseParams BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (*http.Response, int64, error) {
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
	resp, err := doHTTPRequestGetResp(ReqParams{
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

// PutObject creates an object from the body of the reader argument and puts
// it in the specified bucket.
//
// Assumes that `args.Reader` is already opened and ready for usage.
func PutObject(args PutObjectArgs) (err error) {
	query := cmn.AddBckToQuery(nil, args.Bck)
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
			return nil, cmn.NewFailedToCreateHTTPRequest(err)
		}

		// The HTTP package doesn't automatically set this for files, so it has to be done manually
		// If it wasn't set, we would need to deal with the redirect manually.
		req.GetBody = func() (io.ReadCloser, error) {
			return args.Reader.Open()
		}
		if args.Cksum != nil && args.Cksum.Type() != cos.ChecksumNone {
			req.Header.Set(cmn.HdrObjCksumType, args.Cksum.Type())
			ckVal := args.Cksum.Value()
			if ckVal == "" {
				_, ckhash, err := cos.CopyAndChecksum(io.Discard, args.Reader, nil, args.Cksum.Type())
				if err != nil {
					return nil, cmn.NewFailedToCreateHTTPRequest(err)
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

// AppendObject builds the object which should be finished with `FlushObject` request.
// It returns handle which works as id for subsequent append requests so the
// correct object can be identified.
//
// NOTE: Until `FlushObject` is called one cannot access the object yet as
// it is yet not fully operational.
func AppendObject(args AppendArgs) (handle string, err error) {
	query := make(url.Values)
	query.Add(cmn.URLParamAppendType, cmn.AppendOp)
	query.Add(cmn.URLParamAppendHandle, args.Handle)
	query = cmn.AddBckToQuery(query, args.Bck)

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
			return nil, cmn.NewFailedToCreateHTTPRequest(err)
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

// FlushObject should occur once all appends have finished successfully.
// This call will create a fully operational object and requires handle to be set.
func FlushObject(args FlushArgs) (err error) {
	query := make(url.Values)
	query.Add(cmn.URLParamAppendType, cmn.FlushOp)
	query.Add(cmn.URLParamAppendHandle, args.Handle)
	query = cmn.AddBckToQuery(query, args.Bck)

	var header http.Header
	if args.Cksum != nil && args.Cksum.Type() != cos.ChecksumNone {
		header = make(http.Header)
		header.Set(cmn.HdrObjCksumType, args.Cksum.Type())
		header.Set(cmn.HdrObjCksumVal, args.Cksum.Value())
	}

	args.BaseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: args.BaseParams,
		Path:       cmn.URLPathObjects.Join(args.Bck.Name, args.Object),
		Query:      query,
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
//
// NOTE: Advanced usage only.
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
func DoReqWithRetry(client *http.Client, newRequest func(_ cmn.ReqArgs) (*http.Request, error),
	reqArgs cmn.ReqArgs) (resp *http.Response, err error) {
	var (
		r     io.ReadCloser
		req   *http.Request
		sleep = httpRetrySleep
	)
	reader := reqArgs.BodyR.(cos.ReadOpenCloser)
	if req, err = newRequest(reqArgs); err != nil {
		cos.Close(reader)
		return
	}
	if resp, err = client.Do(req); !shouldRetryHTTP(err, resp) {
		goto exit
	}
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		sleep = httpRetryRateSleep
	}
	for i := 0; i < httpMaxRetries; i++ {
		time.Sleep(sleep)
		sleep += sleep / 2

		if r, err = reader.Open(); err != nil {
			return
		}
		reqArgs.BodyR = r

		if req, err = newRequest(reqArgs); err != nil {
			cos.Close(reader)
			return
		}
		if resp, err = client.Do(req); !shouldRetryHTTP(err, resp) {
			goto exit
		}
	}
exit:
	if err != nil {
		return nil, fmt.Errorf("failed to %s, err: %v", reqArgs.Method, err)
	}
	_, err = readResp(ReqParams{}, resp, nil)
	if errC := resp.Body.Close(); err == nil {
		return resp, errC
	}
	return
}

func shouldRetryHTTP(err error, resp *http.Response) bool {
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	return err != nil && (cmn.IsErrConnectionReset(err) || cmn.IsErrConnectionRefused(err))
}
