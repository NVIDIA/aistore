// Package aisloader
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */

package aisloader

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
)

var (
	transportArgs = cmn.TransportArgs{
		Timeout:          600 * time.Second,
		IdleConnsPerHost: 100,
		UseHTTPProxyEnv:  true,
		SkipVerify:       true, // TODO: trust all servers for now
	}
	httpClient *http.Client
)

type (
	// traceableTransport is an http.RoundTripper that keeps track of a http
	// request and implements hooks to report HTTP tracing events.
	traceableTransport struct {
		transport             *http.Transport
		current               *http.Request
		tsBegin               time.Time // request initialized
		tsProxyConn           time.Time // connected with proxy
		tsRedirect            time.Time // redirected
		tsTargetConn          time.Time // connected with target
		tsHTTPEnd             time.Time // http request returned
		tsProxyWroteHeaders   time.Time
		tsProxyWroteRequest   time.Time
		tsProxyFirstResponse  time.Time
		tsTargetWroteHeaders  time.Time
		tsTargetWroteRequest  time.Time
		tsTargetFirstResponse time.Time
		connCnt               int
	}

	traceCtx struct {
		tr           *traceableTransport
		trace        *httptrace.ClientTrace
		tracedClient *http.Client
	}

	// httpLatencies stores latency of a http request
	httpLatencies struct {
		ProxyConn           time.Duration // from request is created to proxy connection is established
		Proxy               time.Duration // from proxy connection is established to redirected
		TargetConn          time.Duration // from request is redirected to target connection is established
		Target              time.Duration // from target connection is established to request is completed
		PostHTTP            time.Duration // from http ends to after read data from http response and verify hash (if specified)
		ProxyWroteHeader    time.Duration // from ProxyConn to header is written
		ProxyWroteRequest   time.Duration // from ProxyWroteHeader to response body is written
		ProxyFirstResponse  time.Duration // from ProxyWroteRequest to first byte of response
		TargetWroteHeader   time.Duration // from TargetConn to header is written
		TargetWroteRequest  time.Duration // from TargetWroteHeader to response body is written
		TargetFirstResponse time.Duration // from TargetWroteRequest to first byte of response
	}
)

// RoundTrip records the proxy redirect time and keeps track of requests.
func (t *traceableTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.connCnt == 1 {
		t.tsRedirect = time.Now()
	}

	t.current = req
	return t.transport.RoundTrip(req)
}

// GotConn records when the connection to proxy/target is made.
func (t *traceableTransport) GotConn(info httptrace.GotConnInfo) {
	switch t.connCnt {
	case 0:
		t.tsProxyConn = time.Now()
	case 1:
		t.tsTargetConn = time.Now()
	default:
		// ignore
		// this can happen during proxy stress test when the proxy dies
	}
	t.connCnt++
}

// WroteHeaders records when the header is written to
func (t *traceableTransport) WroteHeaders() {
	switch t.connCnt {
	case 1:
		t.tsProxyWroteHeaders = time.Now()
	case 2:
		t.tsTargetWroteHeaders = time.Now()
	default:
		// ignore
	}
}

// WroteRequest records when the request is completely written
func (t *traceableTransport) WroteRequest(wr httptrace.WroteRequestInfo) {
	switch t.connCnt {
	case 1:
		t.tsProxyWroteRequest = time.Now()
	case 2:
		t.tsTargetWroteRequest = time.Now()
	default:
		// ignore
	}
}

// GotFirstResponseByte records when the response starts to come back
func (t *traceableTransport) GotFirstResponseByte() {
	switch t.connCnt {
	case 1:
		t.tsProxyFirstResponse = time.Now()
	case 2:
		t.tsTargetFirstResponse = time.Now()
	default:
		// ignore
	}
}

func newTraceCtx() *traceCtx {
	tctx := &traceCtx{}

	tctx.tr = &traceableTransport{
		transport: cmn.NewTransport(transportArgs),
		tsBegin:   time.Now(),
	}
	tctx.trace = &httptrace.ClientTrace{
		GotConn:              tctx.tr.GotConn,
		WroteHeaders:         tctx.tr.WroteHeaders,
		WroteRequest:         tctx.tr.WroteRequest,
		GotFirstResponseByte: tctx.tr.GotFirstResponseByte,
	}
	tctx.tracedClient = &http.Client{
		Transport: tctx.tr,
		Timeout:   600 * time.Second,
	}

	return tctx
}

// PUT with HTTP trace
func putWithTrace(proxyURL string, bck cmn.Bck, object, hash string, reader cmn.ReadOpenCloser) (httpLatencies, error) { // nolint:interfacer // we use `reader.Open` method
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   proxyURL,
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, object),
		Query:  cmn.AddBckToQuery(nil, bck),
		BodyR:  reader,
	}

	tctx := newTraceCtx()
	newReq := func(reqArgs cmn.ReqArgs) (request *http.Request, err error) {
		req, err := reqArgs.Req()
		if err != nil {
			return nil, err
		}

		// The HTTP package doesn't automatically set this for files, so it has to be done manually
		// If it wasn't set, we would need to deal with the redirect manually.
		req.GetBody = reader.Open
		if hash != "" {
			req.Header.Set(cmn.HeaderObjCksumType, cmn.ChecksumXXHash)
			req.Header.Set(cmn.HeaderObjCksumVal, hash)
		}

		request = req.WithContext(httptrace.WithClientTrace(req.Context(), tctx.trace))
		return
	}

	_, err := api.DoReqWithRetry(tctx.tracedClient, newReq, reqArgs) // nolint:bodyclose // it's closed inside
	if err != nil {
		return httpLatencies{}, err
	}

	tctx.tr.tsHTTPEnd = time.Now()
	l := httpLatencies{
		ProxyConn:           cmn.TimeDelta(tctx.tr.tsProxyConn, tctx.tr.tsBegin),
		Proxy:               cmn.TimeDelta(tctx.tr.tsRedirect, tctx.tr.tsProxyConn),
		TargetConn:          cmn.TimeDelta(tctx.tr.tsTargetConn, tctx.tr.tsRedirect),
		Target:              cmn.TimeDelta(tctx.tr.tsHTTPEnd, tctx.tr.tsTargetConn),
		PostHTTP:            time.Since(tctx.tr.tsHTTPEnd),
		ProxyWroteHeader:    cmn.TimeDelta(tctx.tr.tsProxyWroteHeaders, tctx.tr.tsProxyConn),
		ProxyWroteRequest:   cmn.TimeDelta(tctx.tr.tsProxyWroteRequest, tctx.tr.tsProxyWroteHeaders),
		ProxyFirstResponse:  cmn.TimeDelta(tctx.tr.tsProxyFirstResponse, tctx.tr.tsProxyWroteRequest),
		TargetWroteHeader:   cmn.TimeDelta(tctx.tr.tsTargetWroteHeaders, tctx.tr.tsTargetConn),
		TargetWroteRequest:  cmn.TimeDelta(tctx.tr.tsTargetWroteRequest, tctx.tr.tsTargetWroteHeaders),
		TargetFirstResponse: cmn.TimeDelta(tctx.tr.tsTargetFirstResponse, tctx.tr.tsTargetWroteRequest),
	}
	return l, nil
}

//
// Put executes PUT
//
func put(proxyURL string, bck cmn.Bck, object, hash string, reader cmn.ReadOpenCloser) error {
	var (
		baseParams = api.BaseParams{
			Client: httpClient,
			URL:    proxyURL,
			Method: http.MethodPut,
		}
		args = api.PutObjectArgs{
			BaseParams: baseParams,
			Bck:        bck,
			Object:     object,
			Hash:       hash,
			Reader:     reader,
		}
	)
	return api.PutObject(args)
}

// same as above with HTTP trace
func getTraceDiscard(proxyURL string, bck cmn.Bck, objName string, validate bool,
	offset, length int64) (int64, httpLatencies, error) {
	var (
		hdrCksumValue, hdrCksumType string
	)
	query := url.Values{}
	query = cmn.AddBckToQuery(query, bck)
	if length > 0 {
		query.Add(cmn.URLParamOffset, strconv.FormatInt(offset, 10))
		query.Add(cmn.URLParamLength, strconv.FormatInt(length, 10))
	}
	reqArgs := cmn.ReqArgs{
		Method: http.MethodGet,
		Base:   proxyURL,
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, objName),
		Query:  query,
	}
	req, err := reqArgs.Req()
	if err != nil {
		return 0, httpLatencies{}, err
	}

	tctx := newTraceCtx()
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), tctx.trace))

	resp, err := tctx.tracedClient.Do(req)
	if err != nil {
		return 0, httpLatencies{}, err
	}
	defer resp.Body.Close()

	tctx.tr.tsHTTPEnd = time.Now()
	if validate {
		hdrCksumValue = resp.Header.Get(cmn.HeaderObjCksumVal)
		hdrCksumType = resp.Header.Get(cmn.HeaderObjCksumType)
	}

	src := fmt.Sprintf("GET (object %s from bucket %s)", objName, bck)
	n, cksumValue, err := readResponse(resp, ioutil.Discard, src, hdrCksumType)
	if err != nil {
		return 0, httpLatencies{}, err
	}
	if validate && hdrCksumValue != cksumValue {
		err = cmn.NewInvalidCksumError(hdrCksumValue, cksumValue)
	}

	latencies := httpLatencies{
		ProxyConn:           cmn.TimeDelta(tctx.tr.tsProxyConn, tctx.tr.tsBegin),
		Proxy:               cmn.TimeDelta(tctx.tr.tsRedirect, tctx.tr.tsProxyConn),
		TargetConn:          cmn.TimeDelta(tctx.tr.tsTargetConn, tctx.tr.tsRedirect),
		Target:              cmn.TimeDelta(tctx.tr.tsHTTPEnd, tctx.tr.tsTargetConn),
		PostHTTP:            time.Since(tctx.tr.tsHTTPEnd),
		ProxyWroteHeader:    cmn.TimeDelta(tctx.tr.tsProxyWroteHeaders, tctx.tr.tsProxyConn),
		ProxyWroteRequest:   cmn.TimeDelta(tctx.tr.tsProxyWroteRequest, tctx.tr.tsProxyWroteHeaders),
		ProxyFirstResponse:  cmn.TimeDelta(tctx.tr.tsProxyFirstResponse, tctx.tr.tsProxyWroteRequest),
		TargetWroteHeader:   cmn.TimeDelta(tctx.tr.tsTargetWroteHeaders, tctx.tr.tsTargetConn),
		TargetWroteRequest:  cmn.TimeDelta(tctx.tr.tsTargetWroteRequest, tctx.tr.tsTargetWroteHeaders),
		TargetFirstResponse: cmn.TimeDelta(tctx.tr.tsTargetFirstResponse, tctx.tr.tsTargetWroteRequest),
	}
	return n, latencies, err
}

// getDiscard sends a GET request and discards returned data
func getDiscard(proxyURL string, bck cmn.Bck, objName string, validate bool, offset, length int64) (int64, error) {
	var (
		hdrCksumValue, hdrCksumType string
		query                       url.Values
	)
	query = cmn.AddBckToQuery(query, bck)
	if length > 0 {
		query.Add(cmn.URLParamOffset, strconv.FormatInt(offset, 10))
		query.Add(cmn.URLParamLength, strconv.FormatInt(length, 10))
	}
	reqArgs := cmn.ReqArgs{
		Method: http.MethodGet,
		Base:   proxyURL,
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, objName),
		Query:  query,
	}
	req, err := reqArgs.Req()
	if err != nil {
		return 0, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if validate {
		hdrCksumValue = resp.Header.Get(cmn.HeaderObjCksumVal)
		hdrCksumType = resp.Header.Get(cmn.HeaderObjCksumType)
	}
	src := fmt.Sprintf("GET (object %s from bucket %s)", objName, bck)
	n, cksumValue, err := readResponse(resp, ioutil.Discard, src, hdrCksumType)
	if err != nil {
		return 0, err
	}
	if validate && hdrCksumValue != cksumValue {
		return 0, cmn.NewInvalidCksumError(hdrCksumValue, cksumValue)
	}
	return n, err
}

// getConfig sends a {what:config} request to the url and discard the message
// For testing purpose only
func getConfig(server string) (httpLatencies, error) {
	tctx := newTraceCtx()

	url := server + cmn.URLPath(cmn.Version, cmn.Daemon)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.URL.RawQuery = api.GetWhatRawQuery(cmn.GetWhatConfig, "")
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), tctx.trace))

	resp, err := tctx.tracedClient.Do(req)
	if err != nil {
		return httpLatencies{}, err
	}
	defer resp.Body.Close()

	_, err = discardResponse(resp, "GetConfig")
	l := httpLatencies{
		ProxyConn: cmn.TimeDelta(tctx.tr.tsProxyConn, tctx.tr.tsBegin),
		Proxy:     time.Since(tctx.tr.tsProxyConn),
	}
	return l, err
}

// listObjectsFast returns a slice of object names of all objects that match the prefix in a bucket
func listObjectsFast(baseParams api.BaseParams, bck cmn.Bck, prefix string) ([]string, error) {
	query := url.Values{}
	if prefix != "" {
		query.Add(cmn.URLParamPrefix, prefix)
	}

	data, err := api.ListObjectsFast(baseParams, bck, nil, query)
	if err != nil {
		return nil, err
	}

	objs := make([]string, 0, len(data.Entries))
	for _, obj := range data.Entries {
		// Skip directories
		if obj.Name[len(obj.Name)-1] != '/' {
			objs = append(objs, obj.Name)
		}
	}

	return objs, nil
}

func discardResponse(r *http.Response, src string) (int64, error) {
	n, _, err := readResponse(r, ioutil.Discard, src, "")
	return n, err
}

func readResponse(r *http.Response, w io.Writer, src, cksumType string) (int64, string, error) {
	var (
		n        int64
		cksumVal string
		err      error
	)

	if r.StatusCode >= http.StatusBadRequest {
		bytes, err := ioutil.ReadAll(r.Body)
		if err == nil {
			return 0, "", fmt.Errorf("bad status %d from %s, response: %s", r.StatusCode, src, string(bytes))
		}
		return 0, "", fmt.Errorf("bad status %d from %s, err: %v", r.StatusCode, src, err)
	}

	buf, slab := mmsa.Alloc()
	defer slab.Free(buf)

	if cksumType != "" {
		n, cksumVal, err = cmn.WriteWithHash(w, r.Body, buf, cksumType)
		if err != nil {
			return 0, "", fmt.Errorf("failed to read HTTP response, err: %v", err)
		}
	} else if n, err = io.CopyBuffer(w, r.Body, buf); err != nil {
		return 0, "", fmt.Errorf("failed to read HTTP response, err: %v", err)
	}

	return n, cksumVal, nil
}
