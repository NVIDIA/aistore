// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

const (
	errNilCksum     = "nil checksum"
	errNilCksumType = "checksum is empty (checksum type %q) - cannot validate"
)

type (
	BaseParams struct {
		Client *http.Client
		URL    string
		Method string
		Token  string
		UA     string
	}

	// ReqParams is used in constructing client-side API requests to aistore.
	// Stores Query and Headers for providing arguments that are not used commonly in API requests
	//  See also: cmn.HreqArgs
	ReqParams struct {
		Query      url.Values
		Header     http.Header
		BaseParams BaseParams
		Path       string

		// Authentication
		User     string
		Password string

		// amsg, lsmsg etc.
		Body []byte

		// mem-pool (when cos.HdrContentType = cos.ContentMsgPack)
		buf []byte
	}
)

type (
	reqResp struct {
		client *http.Client
		req    *http.Request
		resp   *http.Response
	}
	wrappedResp struct {
		*http.Response
		n int64 // number bytes read from `resp.Body`
	}
)

// HTTPStatus returns HTTP status or (-1) for non-HTTP error.
func HTTPStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if herr := cmn.Err2HTTPErr(err); herr != nil {
		return herr.Status
	}
	return -1 // invalid
}

func SetAuxHeaders(r *http.Request, bp *BaseParams) {
	if bp.Token != "" {
		r.Header.Set(apc.HdrAuthorization, apc.AuthenticationTypeBearer+" "+bp.Token)
	}
	if bp.UA != "" {
		r.Header.Set(cos.HdrUserAgent, bp.UA)
	}
}

func GetWhatRawQuery(getWhat, getProps string) string {
	q := url.Values{}
	q.Add(apc.QparamWhat, getWhat)
	if getProps != "" {
		q.Add(apc.QparamProps, getProps)
	}
	return q.Encode()
}

//
// do request ------------------------------------------------------------------------------
//

// uses do() to make the request; if successful, checks, drains, and closes the response body
func (reqParams *ReqParams) DoRequest() error {
	resp, err := reqParams.do()
	if err != nil {
		return err
	}
	return reqParams.cdc(resp)
}

// same as above except that it also returns response header
func (reqParams *ReqParams) doReqHdr() (_ http.Header, status int, _ error) {
	resp, err := reqParams.do()
	if err == nil {
		return resp.Header, resp.StatusCode, reqParams.cdc(resp)
	}
	if resp != nil {
		status = resp.StatusCode
	}
	return nil, status, err
}

// Makes request via do(), decodes `resp.Body` into the `out` structure,
// closes the former, and returns the entire wrapped response
// (as well as `out`)
//
// Returns an error if the response status >= 400.
func (reqParams *ReqParams) DoReqAny(out any) (int, error) {
	debug.AssertNotPstr(out)
	resp, err := reqParams.do()
	if err != nil {
		return 0, err
	}
	err = reqParams.readAny(resp, out)
	cos.DrainReader(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, err
}

// same as above with `out` being a string
func (reqParams *ReqParams) doReqStr(out *string) (int, error) {
	resp, err := reqParams.do()
	if err != nil {
		return 0, err
	}
	err = reqParams.readStr(resp, out)
	cos.DrainReader(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, err
}

// Makes request via do() and uses provided writer to write `resp.Body`
// (which is also closes)
//
// Returns the entire wrapped response.
func (reqParams *ReqParams) doWriter(w io.Writer) (wresp *wrappedResp, err error) {
	var resp *http.Response
	resp, err = reqParams.do()
	if err != nil {
		return
	}
	wresp, err = reqParams.rwResp(resp, w)
	cos.DrainReader(resp.Body)
	resp.Body.Close()
	return
}

// same as above except that it returns response body (as io.ReadCloser) for subsequent reading
func (reqParams *ReqParams) doReader() (io.ReadCloser, int64, error) {
	resp, err := reqParams.do()
	if err != nil {
		return nil, 0, err
	}
	if err := reqParams.checkResp(resp); err != nil {
		resp.Body.Close()
		return nil, 0, err
	}
	return resp.Body, resp.ContentLength, nil
}

// makes HTTP request, retries on connection-refused and reset errors, and returns the response
func (reqParams *ReqParams) do() (resp *http.Response, err error) {
	var reqBody io.Reader
	if reqParams.Body != nil {
		reqBody = bytes.NewBuffer(reqParams.Body)
	}
	urlPath := reqParams.BaseParams.URL + reqParams.Path
	req, errR := http.NewRequest(reqParams.BaseParams.Method, urlPath, reqBody)
	if errR != nil {
		return nil, fmt.Errorf("failed to create http request: %w", errR)
	}
	reqParams.setRequestOptParams(req)
	SetAuxHeaders(req, &reqParams.BaseParams)

	rr := reqResp{client: reqParams.BaseParams.Client, req: req}
	_, err = cmn.NetworkCallWithRetry(&cmn.RetryArgs{
		Call:      rr.call,
		Verbosity: cmn.RetryLogOff,
		SoftErr:   httpMaxRetries,
		Sleep:     httpRetrySleep,
		BackOff:   true,
		IsClient:  true,
	})
	resp = rr.resp
	if err == nil {
		return resp, nil
	}
	if resp != nil {
		herr := cmn.NewErrHTTP(req, err, resp.StatusCode)
		herr.Method, herr.URLPath = reqParams.BaseParams.Method, reqParams.Path
		return nil, herr
	}
	if uerr, ok := err.(*url.Error); ok {
		err = uerr.Unwrap()
		herr := cmn.NewErrHTTP(req, err, 0)
		herr.Method, herr.URLPath = reqParams.BaseParams.Method, reqParams.Path
		return nil, herr
	}
	return nil, err
}

// Check, Drain, Close
func (reqParams *ReqParams) cdc(resp *http.Response) (err error) {
	err = reqParams.checkResp(resp)
	cos.DrainReader(resp.Body)
	resp.Body.Close() // ignore Close err, if any
	return
}

// setRequestOptParams given an existing HTTP Request and optional API parameters,
// sets the optional fields of the request if provided.
func (reqParams *ReqParams) setRequestOptParams(req *http.Request) {
	if len(reqParams.Query) != 0 {
		req.URL.RawQuery = reqParams.Query.Encode()
	}
	if reqParams.Header != nil {
		req.Header = reqParams.Header
	}
	if reqParams.User != "" && reqParams.Password != "" {
		req.SetBasicAuth(reqParams.User, reqParams.Password)
	}
}

//
// check, read, write, validate http.Response ----------------------------------------------
//

// decode response iff: err == nil AND status in (ok, partial-content)
func (reqParams *ReqParams) readAny(resp *http.Response, out any) (err error) {
	debug.Assert(out != nil)
	if err = reqParams.checkResp(resp); err != nil {
		return
	}
	if code := resp.StatusCode; code != http.StatusOK && code != http.StatusPartialContent {
		return
	}
	// json or msgpack
	if resp.Header.Get(cos.HdrContentType) == cos.ContentMsgPack {
		debug.Assert(cap(reqParams.buf) > cos.KiB) // caller must allocate
		r := msgp.NewReaderBuf(resp.Body, reqParams.buf)
		err = out.(msgp.Decodable).DecodeMsg(r)
	} else {
		err = jsoniter.NewDecoder(resp.Body).Decode(out)
	}
	if err != nil {
		err = fmt.Errorf("unexpected: failed to decode response: %v -> %T", err, out)
	}
	return
}

func (reqParams *ReqParams) readStr(resp *http.Response, out *string) error {
	if err := reqParams.checkResp(resp); err != nil {
		return err
	}
	b, err := cos.ReadAllN(resp.Body, resp.ContentLength)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}
	*out = string(b)
	return nil
}

func (reqParams *ReqParams) rwResp(resp *http.Response, w io.Writer) (*wrappedResp, error) {
	if err := reqParams.checkResp(resp); err != nil {
		return nil, err
	}
	wresp := &wrappedResp{Response: resp}
	n, err := io.Copy(w, resp.Body)
	if err != nil {
		return nil, err
	}
	// NOTE: Content-Length == -1 (unknown) for transformed objects
	debug.Assertf(n == resp.ContentLength || resp.ContentLength == -1, "%d vs %d", n, wresp.n)
	wresp.n = n
	return wresp, nil
}

// end-to-end protection (compare w/ rwResp above)
func (reqParams *ReqParams) readValidate(resp *http.Response, w io.Writer) (*wrappedResp, error) {
	var (
		wresp     = &wrappedResp{Response: resp, n: resp.ContentLength}
		cksumType = resp.Header.Get(apc.HdrObjCksumType)
	)
	if err := reqParams.checkResp(resp); err != nil {
		return nil, err
	}
	// write _and_ compute client-side checksum
	n, cksum, err := cos.CopyAndChecksum(w, resp.Body, nil, cksumType)
	if err != nil {
		return nil, err
	}
	if n != resp.ContentLength {
		return nil, fmt.Errorf("read length (%d) != (%d) content-length", n, resp.ContentLength)
	}
	if cksum == nil {
		if cksumType == "" {
			return nil, errors.New(errNilCksum) // e.g., after fast-appending to a TAR
		}
		return nil, fmt.Errorf(errNilCksumType, cksumType)
	}

	// compare client-side checksum with the one that cluster has
	hdrCksumValue := wresp.Header.Get(apc.HdrObjCksumVal)
	if hdrCksumValue != cksum.Val() {
		return nil, cmn.NewErrInvalidCksum(hdrCksumValue, cksum.Val())
	}
	return wresp, nil
}

func (reqParams *ReqParams) checkResp(resp *http.Response) error {
	if resp.StatusCode < http.StatusBadRequest {
		return nil
	}
	if reqParams.BaseParams.Method == http.MethodHead {
		// "A response to a HEAD method should not have a body."
		if msg := resp.Header.Get(apc.HdrError); msg != "" {
			return &cmn.ErrHTTP{
				TypeCode: cmn.TypeCodeHTTPErr(msg),
				Message:  msg,
				Status:   resp.StatusCode,
				Method:   reqParams.BaseParams.Method,
				URLPath:  reqParams.Path,
			}
		}
	}

	b, _ := cos.ReadAllN(resp.Body, resp.ContentLength)
	if len(b) == 0 {
		if resp.StatusCode == http.StatusServiceUnavailable {
			msg := fmt.Sprintf("[%s]: starting up, please try again later...", http.StatusText(http.StatusServiceUnavailable))
			return &cmn.ErrHTTP{Message: msg, Status: resp.StatusCode}
		}
		return &cmn.ErrHTTP{
			Message: "failed to execute " + reqParams.BaseParams.Method + " request",
			Status:  resp.StatusCode,
			Method:  reqParams.BaseParams.Method,
			URLPath: reqParams.Path,
		}
	}

	herr := &cmn.ErrHTTP{}
	if err := jsoniter.Unmarshal(b, herr); err == nil {
		return herr
	}
	// otherwise, recreate
	msg := string(b)
	return &cmn.ErrHTTP{
		TypeCode: cmn.TypeCodeHTTPErr(msg),
		Message:  msg,
		Status:   resp.StatusCode,
		Method:   reqParams.BaseParams.Method,
		URLPath:  reqParams.Path,
	}
}

// read multipart content, as per:
// * https://datatracker.ietf.org/doc/html/rfc2046#section-5.1
// given a single (GetBatch) use case, we currently
// - always expect two parts, whereby:
//   - the first part is JSON unmarshaled into `out`
//   - the second part is written into `writer`
func (reqParams *ReqParams) readMultipart(out any, writer io.Writer) (int, error) {
	debug.AssertNotPstr(out)
	resp, err := reqParams.do()
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if err := reqParams.checkResp(resp); err != nil {
		return 0, err
	}

	ctype := resp.Header.Get(cos.HdrContentType)
	mediatype, params, err := mime.ParseMediaType(ctype)
	if err != nil || !strings.HasPrefix(mediatype, MossMultipartPrefix) {
		return 0, fmt.Errorf("expected multipart response, got %q, err: %w", ctype, err)
	}

	mr := multipart.NewReader(resp.Body, params["boundary"]) // standard MIME header parameter name: "boundary"

	// Part 1: JSON metadata
	part1, err := mr.NextPart()
	if err != nil {
		return 0, fmt.Errorf("missing metadata part: %w", err)
	}

	debug.Assert(part1.FormName() == MossMetadataField, part1.FormName(), " vs ", MossMetadataField, " (see xs/moss)")

	err = jsoniter.NewDecoder(part1).Decode(out)
	if err != nil {
		part1.Close()
		return 0, fmt.Errorf("failed to decode multipart JSON: %w", err)
	}

	// Part 2: stream (e.g. TAR)
	// always closes the previous part (part1, in this case)
	part2, err := mr.NextPart()
	if err != nil {
		return 0, fmt.Errorf("missing stream part: %w", err)
	}

	debug.Assert(part2.FormName() == MossArchiveField, part2.FormName(), " vs ", MossArchiveField, " (see xs/moss)")

	n, err := io.Copy(writer, part2)
	part2.Close()
	if err != nil {
		return 0, fmt.Errorf("stream copy error: %w", err)
	}

	return int(n), nil
}

/////////////
// reqResp //
/////////////

func (rr *reqResp) call() (status int, err error) {
	rr.resp, err = rr.client.Do(rr.req) //nolint:bodyclose // closed by a caller
	if rr.resp != nil {
		status = rr.resp.StatusCode
	}
	return status, err
}

//
// mem-pools
//

var (
	reqParamPool sync.Pool
	reqParams0   ReqParams

	msgpPool sync.Pool
)

func AllocRp() *ReqParams {
	if v := reqParamPool.Get(); v != nil {
		return v.(*ReqParams)
	}
	return &ReqParams{}
}

func FreeRp(reqParams *ReqParams) {
	*reqParams = reqParams0
	reqParamPool.Put(reqParams)
}

func allocMbuf() (buf []byte) {
	if v := msgpPool.Get(); v != nil {
		buf = *(v.(*[]byte))
	} else {
		buf = make([]byte, msgpBufSize)
	}
	return
}

func freeMbuf(buf []byte) { msgpPool.Put(&buf) }
