// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	httpMaxRetries = 5                      // maximum number of retries for an HTTP request
	httpRetrySleep = 100 * time.Millisecond // a sleep between HTTP request retries

	// Sleep between HTTP retries for error[rate of change requests exceeds limit] - must be > 1s:
	// From https://cloud.google.com/storage/quotas#objects
	// * "There is an update limit on each object of once per second..."
	httpRetryRateSleep = 1500 * time.Millisecond
)

// GET(object)
type (
	GetArgs struct {
		// If not specified (or same: if `nil`), Writer defaults to `io.Discard`
		// (in other words, with no writer the object that is being read will be discarded)
		Writer io.Writer

		// Currently, this optional Query field can (optionally) carry:
		// - `apc.QparamETLName`: named ETL to transform the object (i.e., perform "inline transformation")
		// - `apc.QparamOrigURL`: GET from a vanilla http(s) location (`ht://` bucket with the corresponding `OrigURLBck`)
		// - `apc.QparamSilent`: do not log errors
		// - `apc.QparamLatestVer`: get latest version from the associated Cloud bucket; see also: `ValidateWarmGet`
		// - and also a group of parameters used to read aistore-supported serialized archives ("shards"),
		//   namely:
		//   - `apc.QparamArchpath`
		//   - `apc.QparamArchmime`
		//   - `apc.QparamArchregx`
		//   - `apc.QparamArchmode`
		// - TODO: add `apc.QparamValidateCksum`
		Query url.Values

		// The field is used to facilitate a) range read, and b) blob download
		// E.g. range:
		// * Header.Set(cos.HdrRange, fmt.Sprintf("bytes=%d-%d", fromOffset, toOffset))
		//   For range formatting, see https://www.rfc-editor.org/rfc/rfc7233#section-2.1
		// E.g. blob download:
		// * Header.Set(apc.HdrBlobDownload, "true")
		Header http.Header
	}

	// `ObjAttrs` represents object attributes and can be further used to retrieve
	// the object's size, checksum, version, and other metadata.
	//
	// Note that while `GetObject()` and related GET APIs return `ObjAttrs`,
	// `HeadObject()` API returns `cmn.ObjectProps` - a superset.
	ObjAttrs struct {
		wrespHeader http.Header
		n           int64
	}
)

// PUT(object)
type (
	PutArgs struct {
		Reader cos.ReadOpenCloser

		// optional; if provided:
		// - if object exists: load the object's metadata, compare checksums - skip writing if equal
		// - otherwise, compare the two checksums upon writing (aka, "end-to-end protection")
		Cksum *cos.Cksum

		BaseParams BaseParams

		Bck     cmn.Bck
		ObjName string

		Size uint64 // optional

		// Skip loading existing object's metadata in order to
		// compare its Checksum and update its existing Version (if exists);
		// can be used to reduce PUT latency when:
		// - we massively write a new content into a bucket, and/or
		// - we simply don't care.
		SkipVC bool
	}
)

// HEAD(object)
type (
	// optional
	HeadArgs struct {
		FltPresence   int  // `apc.QparamFltPresence`  - in-cluster vs remote; for enumerated values, see api/apc/query
		Silent        bool // `apc.QparamSilent`       - when true, do not log (not-found) error
		LatestVer     bool // `apc.QparamLatestVer`    - check (with remote backend) whether in-cluster version is the latest
		ValidateCksum bool // `apc.QparamValidateCksum`- validate (ie., recompute and check) in-cluster object's checksums
	}
)

// APPEND, Archive, Promote (object)
type (
	// Archive files and directories
	PutApndArchArgs struct {
		ArchPath string // filename _in_ archive
		Mime     string // user-specified mime type (NOTE: takes precedence if defined)
		Flags    int64  // apc.ArchAppend and apc.ArchAppendIfExist (the former requires destination shard to exist)
		PutArgs
	}

	// APPEND(object)
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

// GET(object) =========================================================================================
//
// If GetArgs.Writer is specified GetObject will use it to write the response body;
// otherwise, it'll `io.Discard` the latter.
// `io.Copy` is used internally to copy response bytes from the request to the writer.
// Returns `ObjAttrs` that can be further used to get the size and other object metadata.

func (args *GetArgs) ret() (w io.Writer, q url.Values, hdr http.Header) {
	w = io.Discard
	if args == nil {
		return
	}
	if args.Writer != nil {
		w = args.Writer
	}
	q, hdr = args.Query, args.Header
	return
}

func (oah *ObjAttrs) Size() int64 {
	if oah.n == 0 { // unlikely
		oah.n = oah.Attrs().Size
	}
	return oah.n
}

func (oah *ObjAttrs) Attrs() (out cmn.ObjAttrs) {
	out.Cksum = out.FromHeader(oah.wrespHeader)
	return
}

// e.g. usage: range read response
func (oah *ObjAttrs) RespHeader() http.Header {
	return oah.wrespHeader
}

func GetObject(bp BaseParams, bck cmn.Bck, objName string, args *GetArgs) (oah ObjAttrs, err error) {
	var (
		wresp     *wrappedResp
		w, q, hdr = args.ret()
	)
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Query = bck.NewQuery()
		reqParams.Header = hdr
	}
	// copy qparams over, if any
	for k, vs := range q {
		var v string
		if len(vs) > 0 {
			v = vs[0]
		}
		reqParams.Query.Set(k, v)
	}
	wresp, err = reqParams.doWriter(w)
	FreeRp(reqParams)
	if err == nil {
		oah.wrespHeader, oah.n = wresp.Header, wresp.n
	}
	return oah, err
}

// Same as above with checksum validation.
// Returns `cmn.ErrInvalidCksum` when the expected and actual checksum values
// are different.
func GetObjectWithValidation(bp BaseParams, bck cmn.Bck, objName string, args *GetArgs) (oah ObjAttrs, err error) {
	w, q, hdr := args.ret()
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Query = bck.AddToQuery(q)
		reqParams.Header = hdr
	}

	var (
		resp  *http.Response
		wresp *wrappedResp
	)
	resp, err = reqParams.do()
	if err != nil {
		return
	}

	wresp, err = reqParams.readValidate(resp, w)
	cos.DrainReader(resp.Body)
	resp.Body.Close()
	FreeRp(reqParams)
	if err == nil {
		oah.wrespHeader, oah.n = wresp.Header, wresp.n
	} else if err.Error() == errNilCksum {
		err = fmt.Errorf("%s is not checksummed, cannot validate", bck.Cname(objName))
	}
	return
}

// Returns reader of the requested object. It does not read body
// bytes, nor validates a checksum. Caller is responsible for closing the reader.
func GetObjectReader(bp BaseParams, bck cmn.Bck, objName string, args *GetArgs) (r io.ReadCloser, size int64, err error) {
	_, q, hdr := args.ret()
	q = bck.AddToQuery(q)
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Query = q
		reqParams.Header = hdr
	}
	r, size, err = reqParams.doReader()
	FreeRp(reqParams)
	return
}

// PUT(object) ============================================================================================
//
// Uses the specified reader (`args.Reader`) to write a new object (or a new version of the object).
// Assumes that `args.Reader` is already opened and ready for usage.
// Returns `ObjAttrs` that can be further used to get the size and other object metadata.

func (args *PutArgs) getBody() (io.ReadCloser, error) { return args.Reader.Open() }

func (args *PutArgs) put(reqArgs *cmn.HreqArgs) (*http.Request, error) {
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

func PutObject(args *PutArgs) (oah ObjAttrs, err error) {
	var (
		resp  *http.Response
		query = args.Bck.NewQuery()
	)
	if args.SkipVC {
		query.Set(apc.QparamSkipVC, "true")
	}
	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPut
		reqArgs.Base = args.BaseParams.URL
		reqArgs.Path = apc.URLPathObjects.Join(args.Bck.Name, args.ObjName)
		reqArgs.Query = query
		reqArgs.BodyR = args.Reader
	}
	resp, err = DoWithRetry(args.BaseParams.Client, args.put, reqArgs) //nolint:bodyclose // is closed inside
	cmn.FreeHra(reqArgs)
	if err == nil {
		oah.wrespHeader = resp.Header
	}
	return
}

// HEAD(object)  ==============================================================================================
//
// Returns object properties; can be conventionally used to establish in-cluster presence.
// - fltPresence:  as per QparamFltPresence enum (for values and comments, see api/apc/query.go)
// - silent==true: not to log (not-found) error

func HeadObject(bp BaseParams, bck cmn.Bck, objName string, args HeadArgs) (*cmn.ObjectProps, error) {
	bp.Method = http.MethodHead

	q := bck.NewQuery()
	q.Set(apc.QparamFltPresence, strconv.Itoa(args.FltPresence))
	if args.Silent {
		q.Set(apc.QparamSilent, "true")
	}
	if args.LatestVer {
		q.Set(apc.QparamLatestVer, "true")
	}
	if args.ValidateCksum {
		q.Set(apc.QparamValidateCksum, "true")
	}

	reqParams := AllocRp()
	defer FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Query = q
	}
	hdr, _, err := reqParams.doReqHdr()
	if err != nil {
		return nil, err
	}
	if args.FltPresence == apc.FltPresentNoProps {
		return nil, err
	}

	// first, cnm.ObjAttrs (compare with `t.objHead`)
	op := &cmn.ObjectProps{}
	op.Cksum = op.ObjAttrs.FromHeader(hdr)

	// second, all the rest
	err = cmn.IterFields(op, func(tag string, field cmn.IterField) (error, bool) {
		name := apc.PropToHeader(tag) // internal (json) obj prop => canonical http header
		v, ok := hdr[name]
		if !ok {
			return nil, false // skip missing
		}
		// single-value
		return field.SetValue(v[0], true /*force*/), false
	}, cmn.IterOpts{OnlyRead: false})

	if err != nil {
		return nil, err
	}
	return op, nil
}

// SetObjectCustomProps ================================================================================
//
// Given cos.StrKVs (map[string]string) keys and values, sets object's custom properties.
// By default, adds new or updates existing custom keys.
// Use `setNewCustomMDFlag` to _replace_ all existing keys with the specified (new) ones.
// See also: HeadObject() and apc.HdrObjCustomMD

func SetObjectCustomProps(bp BaseParams, bck cmn.Bck, objName string, custom cos.StrKVs, setNew bool) error {
	var (
		actMsg = apc.ActMsg{Value: custom}
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
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Body = cos.MustMarshal(actMsg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// DELETE(object) ======================================================================================

func DeleteObject(bp BaseParams, bck cmn.Bck, objName string) error {
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Query = bck.NewQuery()
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// Evict(object) ======================================================================================

func EvictObject(bp BaseParams, bck cmn.Bck, objName string) error {
	bp.Method = http.MethodDelete
	actMsg := apc.ActMsg{Action: apc.ActEvictObjects, Name: cos.JoinWords(bck.Name, objName)}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Body = cos.MustMarshal(actMsg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.NewQuery()
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// Prefetch(object) ======================================================================================
//
// A convenience method added for "symmetry" with the evict (above)
// (compare with api.PrefetchList and api.PrefetchRange)

func PrefetchObject(bp BaseParams, bck cmn.Bck, objName string) (string, error) {
	var msg apc.PrefetchMsg
	msg.ObjNames = []string{objName}
	return Prefetch(bp, bck, msg)
}

// Archive the content of a reader (`args.Reader` - e.g., an open file). =======================================
// Destination, depending on the options, can be an existing (.tar, .tgz or .tar.gz, .zip, .tar.lz4)
// formatted object (aka "shard") or a new one (or, a new version).
// ---
// For the updated list of supported archival formats -- aka MIME types -- see cmn/cos/archive.go.
// --
// See also:
// - api.ArchiveMultiObj(msg.AppendIfExists = true)
// - api.AppendObject

func PutApndArch(args *PutApndArchArgs) (err error) {
	q := make(url.Values, 4)
	q = args.Bck.AddToQuery(q)
	q.Set(apc.QparamArchpath, args.ArchPath)
	q.Set(apc.QparamArchmime, args.Mime)

	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPut
		reqArgs.Base = args.BaseParams.URL
		reqArgs.Path = apc.URLPathObjects.Join(args.Bck.Name, args.ObjName)
		reqArgs.Query = q
		reqArgs.BodyR = args.Reader
	}
	if args.Flags != 0 {
		flags := strconv.FormatInt(args.Flags, 10)
		reqArgs.Header = http.Header{apc.HdrPutApndArchFlags: []string{flags}}
	}
	putArgs := &args.PutArgs
	_, err = DoWithRetry(args.BaseParams.Client, putArgs.put, reqArgs) //nolint:bodyclose // is closed inside
	cmn.FreeHra(reqArgs)
	return
}

// Append(object) ===============================================================================
// Uses specified reader (`args.Reader`) to append the corresponding content to an object.
// The API can be called multiple times - each call returns a handle
// that may be used for subsequent append requests.
// Once all the "appending" is done, the caller must call `api.FlushObject`
// to finalize the object.
// NOTE:
// object becomes visible (to clients) and accessible only _after_ the call to `api.FlushObject`.

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

func AppendObject(args *AppendArgs) (string /*handle*/, error) {
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
	wresp, err := DoWithRetry(args.BaseParams.Client, args._append, reqArgs) //nolint:bodyclose // it's closed inside
	cmn.FreeHra(reqArgs)
	if err != nil {
		return "", err
	}
	return wresp.Header.Get(apc.HdrAppendHandle), err
}

// FlushObject must be called after all the appends (via `api.AppendObject`).
// To "flush", it uses the handle returned by `api.AppendObject`.
// This call will create a fully operational and accessible object.
func FlushObject(args *FlushArgs) error {
	var (
		header http.Header
		q      = make(url.Values, 4)
		method = args.BaseParams.Method
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
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	args.BaseParams.Method = method
	return err
}

// Rename(object) ==============================================================================
// renames object name from `oldName` to `newName`. Works only within a given specified bucket.

func RenameObject(bp BaseParams, bck cmn.Bck, oldName, newName string) error {
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, oldName)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActRenameObject, Name: newName})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.NewQuery()
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// Promote =========================================================================================
// promote POSIX files and/or directories to (become) in-cluster objects.

func Promote(bp BaseParams, bck cmn.Bck, args *apc.PromoteArgs) (xid string, err error) {
	actMsg := apc.ActMsg{Action: apc.ActPromote, Name: args.SrcFQN, Value: args}
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(actMsg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.NewQuery()
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return xid, err
}

//
// misc. helpers
//

// DoWithRetry executes `http-client.Do` and retries *retriable connection errors*,
// such as "broken pipe" and "connection refused".
// This function always closes the `reqArgs.BodR`, even in case of error.
// Usage: PUT and simlar requests that transfer payload from the user side.
// NOTE: always closes request body reader (reqArgs.BodyR) - explicitly or via Do()
// TODO: refactor

type newRequestCB func(args *cmn.HreqArgs) (*http.Request, error)

func DoWithRetry(client *http.Client, cb newRequestCB, reqArgs *cmn.HreqArgs) (resp *http.Response, err error) {
	var (
		req    *http.Request
		doErr  error
		sleep  = httpRetrySleep
		reader = reqArgs.BodyR.(cos.ReadOpenCloser)
	)
	// first time
	if req, err = cb(reqArgs); err != nil {
		cos.Close(reader)
		return
	}
	resp, doErr = client.Do(req)
	err = doErr
	if !_retry(doErr, resp) {
		goto exit
	}
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		sleep = httpRetryRateSleep
	}

	// retry
	for range httpMaxRetries {
		var r io.ReadCloser
		time.Sleep(sleep)
		sleep += sleep / 2
		if r, err = reader.Open(); err != nil {
			_close(resp, doErr)
			return
		}
		reqArgs.BodyR = r

		if req, err = cb(reqArgs); err != nil {
			cos.Close(r)
			_close(resp, doErr)
			return
		}
		_close(resp, doErr)
		resp, doErr = client.Do(req)
		err = doErr
		if !_retry(doErr, resp) {
			goto exit
		}
	}
exit:
	if err == nil {
		reqParams := AllocRp()
		err = reqParams.checkResp(resp)
		cos.DrainReader(resp.Body)
		FreeRp(reqParams)
	}
	_close(resp, doErr)
	return
}

func _close(resp *http.Response, doErr error) {
	if resp != nil && doErr == nil {
		cos.Close(resp.Body)
	}
}

func _retry(err error, resp *http.Response) bool {
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	return err != nil && cos.IsRetriableConnErr(err)
}
