// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"

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

// uses do() to make request; if successful, checks, drains, and closes the response body
func DoHTTPRequest(reqParams ReqParams) error {
	resp, err := do(reqParams)
	if err != nil {
		return err
	}
	err = checkResp(reqParams, resp)
	cos.DrainReader(resp.Body)
	resp.Body.Close()
	return err
}

// uses doResp() to make request and decode response into `v`
func DoHTTPReqResp(reqParams ReqParams, v interface{}) error {
	_, err := doResp(reqParams, v)
	return err
}

// doResp makes http request via do(), decodes the `v` structure from the `resp.Body` (if provided),
// and returns the entire wrapped response.
//
// The function returns an error if the response status code is >= 400.
func doResp(reqParams ReqParams, v interface{}) (wrap *wrappedResp, err error) {
	var resp *http.Response
	resp, err = do(reqParams)
	if err != nil {
		return nil, err
	}
	wrap, err = readResp(reqParams, resp, v)
	resp.Body.Close()
	return
}

// same as above except that it returns response body (as io.ReadCloser) for subsequent reading
func doReader(reqParams ReqParams) (io.ReadCloser, error) {
	resp, err := do(reqParams)
	if err != nil {
		return nil, err
	}
	if err := checkResp(reqParams, resp); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}

// makes HTTP request, retries on connection-refused and reset errors, and returns the response
func do(reqParams ReqParams) (resp *http.Response, err error) {
	var (
		reqBody io.Reader
		req     *http.Request
	)
	if reqParams.Body != nil {
		reqBody = bytes.NewBuffer(reqParams.Body)
	}
	urlPath := reqParams.BaseParams.URL + reqParams.Path
	req, err = http.NewRequest(reqParams.BaseParams.Method, urlPath, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request, err: %v", err)
	}
	setRequestOptParams(req, reqParams)
	setAuthToken(req, reqParams.BaseParams)

	call := func() (int, error) {
		resp, err = reqParams.BaseParams.Client.Do(req) // nolint:bodyclose // closed by a caller
		if resp != nil {
			return resp.StatusCode, err
		}
		return 0, err
	}

	err = cmn.NetworkCallWithRetry(&cmn.CallWithRetryArgs{
		Call:      call,
		Verbosity: cmn.CallWithRetryLogOff,
		SoftErr:   httpMaxRetries,
		Sleep:     httpRetrySleep,
		BackOff:   true,
		IsClient:  true,
	})
	if err != nil {
		err = fmt.Errorf("failed to %s, err: %w", reqParams.BaseParams.Method, err)
	}
	return resp, err
}

func readResp(reqParams ReqParams, resp *http.Response, v interface{}) (*wrappedResp, error) {
	defer cos.DrainReader(resp.Body)

	if err := checkResp(reqParams, resp); err != nil {
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
			hdrCksumType := resp.Header.Get(cmn.HdrObjCksumType)
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
			// In some places like dSort, the response is just a string (id).
			var b []byte
			b, err = io.ReadAll(resp.Body)
			*t = string(b)
		default:
			if resp.StatusCode == http.StatusOK {
				if resp.Header.Get(cmn.HdrContentType) == cmn.ContentMsgPack {
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

func checkResp(reqParams ReqParams, resp *http.Response) error {
	if resp.StatusCode < http.StatusBadRequest {
		return nil
	}
	if reqParams.BaseParams.Method == http.MethodHead {
		if msg := resp.Header.Get(cmn.HdrError); msg != "" {
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

// setRequestOptParams given an existing HTTP Request and optional API parameters,
// sets the optional fields of the request if provided.
func setRequestOptParams(req *http.Request, reqParams ReqParams) {
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

func setAuthToken(r *http.Request, baseParams BaseParams) {
	if baseParams.Token != "" {
		r.Header.Set(cmn.HdrAuthorization, makeHeaderAuthnToken(baseParams.Token))
	}
}

func makeHeaderAuthnToken(token string) string {
	return cmn.AuthenticationTypeBearer + " " + token
}

func GetWhatRawQuery(getWhat, getProps string) string {
	q := url.Values{}
	q.Add(cmn.URLParamWhat, getWhat)
	if getProps != "" {
		q.Add(cmn.URLParamProps, getProps)
	}
	return q.Encode()
}
