// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

type (
	BaseParams struct {
		Client *http.Client
		URL    string
		Method string
		Token  string
	}

	// ReqParams is used in constructing client-side API requests to the AIStore.
	// Stores Query and Headers for providing arguments that are not used commonly in API requests
	ReqParams struct {
		BaseParams BaseParams
		Path       string
		Body       []byte
		Query      url.Values
		Header     http.Header

		// Authentication
		User     string
		Password string

		// Determines if the response should be validated with the checksum
		Validate bool
	}

	wrappedResp struct {
		*http.Response
		n          int64  // number bytes read from `resp.Body`
		cksumValue string // checksum value of the response
	}
)

func newErrCreateHTTPRequest(err error) error {
	return fmt.Errorf("failed to create new HTTP request, err: %v", err)
}

// HTTPStatus returns HTTP status or (-1) for non-HTTP error.
func HTTPStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if httpErr := cmn.Err2HTTPErr(err); httpErr != nil {
		return httpErr.Status
	}
	return -1 // invalid
}

func getObjectOptParams(options GetObjectInput) (w io.Writer, q url.Values, hdr http.Header) {
	w = io.Discard
	if options.Writer != nil {
		w = options.Writer
	}
	if len(options.Query) != 0 {
		q = options.Query
	}
	if len(options.Header) != 0 {
		hdr = options.Header
	}
	return
}

func SetAuthToken(r *http.Request, token string) {
	if token != "" {
		r.Header.Set(apc.HdrAuthorization, apc.AuthenticationTypeBearer+" "+token)
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

///////////////
// ReqParams //
///////////////
var (
	reqParamPool sync.Pool
	reqParams0   ReqParams
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

// uses do() to make request; if successful, checks, drains, and closes the response body
func (reqParams *ReqParams) DoHTTPRequest() error {
	resp, err := reqParams.do()
	if err != nil {
		return err
	}
	err = reqParams.checkResp(resp)
	cos.DrainReader(resp.Body)
	resp.Body.Close()
	return err
}

// uses doResp() to make request and decode response into `v`
func (reqParams *ReqParams) DoHTTPReqResp(v interface{}) error {
	_, err := reqParams.doResp(v)
	return err
}

// doResp makes http request via do(), decodes the `v` structure from the `resp.Body` (if provided),
// and returns the entire wrapped response.
//
// The function returns an error if the response status code is >= 400.
func (reqParams *ReqParams) doResp(v interface{}) (wrap *wrappedResp, err error) {
	var resp *http.Response
	resp, err = reqParams.do()
	if err != nil {
		return nil, err
	}
	wrap, err = reqParams.readResp(resp, v)
	resp.Body.Close()
	return
}

// same as above except that it returns response body (as io.ReadCloser) for subsequent reading
func (reqParams *ReqParams) doReader() (io.ReadCloser, error) {
	resp, err := reqParams.do()
	if err != nil {
		return nil, err
	}
	if err := reqParams.checkResp(resp); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
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
	SetAuthToken(req, reqParams.BaseParams.Token)

	call := func() (status int, err error) {
		resp, err = reqParams.BaseParams.Client.Do(req) // nolint:bodyclose // closed by a caller
		if resp != nil {
			status = resp.StatusCode
		}
		return
	}
	err = cmn.NetworkCallWithRetry(&cmn.RetryArgs{
		Call:      call,
		Verbosity: cmn.RetryLogOff,
		SoftErr:   httpMaxRetries,
		Sleep:     httpRetrySleep,
		BackOff:   true,
		IsClient:  true,
	})
	if err != nil && resp != nil {
		httpErr := cmn.NewErrHTTP(req, err.Error(), resp.StatusCode)
		httpErr.Method, httpErr.URLPath = reqParams.BaseParams.Method, reqParams.Path
		err = httpErr
	}
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

func (reqParams *ReqParams) readResp(resp *http.Response, v interface{}) (*wrappedResp, error) {
	defer cos.DrainReader(resp.Body)

	if err := reqParams.checkResp(resp); err != nil {
		return nil, err
	}
	wresp := &wrappedResp{Response: resp}
	if v == nil {
		return wresp, nil
	}
	if w, ok := v.(io.Writer); ok {
		if !reqParams.Validate {
			n, err := io.Copy(w, resp.Body)
			if err != nil {
				return nil, err
			}
			wresp.n = n
		} else {
			hdrCksumType := resp.Header.Get(apc.HdrObjCksumType)
			// TODO: use MMSA
			n, cksum, err := cos.CopyAndChecksum(w, resp.Body, nil, hdrCksumType)
			if err != nil {
				return nil, err
			}
			wresp.n = n
			if cksum != nil {
				wresp.cksumValue = cksum.Value()
			}
		}
	} else {
		var err error
		switch t := v.(type) {
		case *string:
			// when the response is a string (e.g., UUID)
			var b []byte
			b, err = io.ReadAll(resp.Body)
			*t = string(b)
		default:
			if resp.StatusCode == http.StatusOK {
				if resp.Header.Get(cos.HdrContentType) == cos.ContentMsgPack {
					r := msgp.NewReaderSize(resp.Body, 10*cos.KiB)
					err = v.(msgp.Decodable).DecodeMsg(r)
				} else {
					err = jsoniter.NewDecoder(resp.Body).Decode(v)
				}
			}
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read response, err: %w", err)
		}
	}
	return wresp, nil
}

func (reqParams *ReqParams) checkResp(resp *http.Response) error {
	if resp.StatusCode < http.StatusBadRequest {
		return nil
	}
	if reqParams.BaseParams.Method == http.MethodHead {
		if msg := resp.Header.Get(cos.HdrError); msg != "" {
			httpErr := cmn.NewErrHTTP(nil, msg, resp.StatusCode)
			httpErr.Method, httpErr.URLPath = reqParams.BaseParams.Method, reqParams.Path
			return httpErr
		}
	}
	var (
		httpErr *cmn.ErrHTTP
		msg, _  = io.ReadAll(resp.Body)
	)
	if reqParams.BaseParams.Method != http.MethodHead && resp.StatusCode != http.StatusServiceUnavailable {
		if jsonErr := jsoniter.Unmarshal(msg, &httpErr); jsonErr == nil {
			return httpErr
		}
	}
	strMsg := string(msg)
	if resp.StatusCode == http.StatusServiceUnavailable && strMsg == "" {
		strMsg = fmt.Sprintf("[%s]: starting up, please try again later...",
			http.StatusText(http.StatusServiceUnavailable))
	}
	// HEAD request does not return the body - create http error
	// 503 is also to be preserved
	httpErr = cmn.NewErrHTTP(nil, strMsg, resp.StatusCode)
	httpErr.Method, httpErr.URLPath = reqParams.BaseParams.Method, reqParams.Path
	return httpErr
}
