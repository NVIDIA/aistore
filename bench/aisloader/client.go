// Package aisloader
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */

package aisloader

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
)

const longListTime = 10 * time.Second

var (
	// AisLoader, being a stress loading tool, should have different from
	// AIS cluster timeouts. And if HTTPS is used, certificate check is
	// always disable in AisLoader client.
	transportArgs = cmn.TransportArgs{
		UseHTTPProxyEnv: true,
		SkipVerify:      true,
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
func (t *traceableTransport) GotConn(httptrace.GotConnInfo) {
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
func (t *traceableTransport) WroteRequest(httptrace.WroteRequestInfo) {
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
func putWithTrace(proxyURL string, bck cmn.Bck, object string, cksum *cos.Cksum,
	reader cos.ReadOpenCloser) (httpLatencies, error) {
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   proxyURL,
		Path:   cmn.URLPathObjects.Join(bck.Name, object),
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
		req.GetBody = func() (io.ReadCloser, error) {
			return reader.Open()
		}
		if cksum != nil {
			req.Header.Set(cmn.HdrObjCksumType, cksum.Ty())
			req.Header.Set(cmn.HdrObjCksumVal, cksum.Val())
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
		ProxyConn:           timeDelta(tctx.tr.tsProxyConn, tctx.tr.tsBegin),
		Proxy:               timeDelta(tctx.tr.tsRedirect, tctx.tr.tsProxyConn),
		TargetConn:          timeDelta(tctx.tr.tsTargetConn, tctx.tr.tsRedirect),
		Target:              timeDelta(tctx.tr.tsHTTPEnd, tctx.tr.tsTargetConn),
		PostHTTP:            time.Since(tctx.tr.tsHTTPEnd),
		ProxyWroteHeader:    timeDelta(tctx.tr.tsProxyWroteHeaders, tctx.tr.tsProxyConn),
		ProxyWroteRequest:   timeDelta(tctx.tr.tsProxyWroteRequest, tctx.tr.tsProxyWroteHeaders),
		ProxyFirstResponse:  timeDelta(tctx.tr.tsProxyFirstResponse, tctx.tr.tsProxyWroteRequest),
		TargetWroteHeader:   timeDelta(tctx.tr.tsTargetWroteHeaders, tctx.tr.tsTargetConn),
		TargetWroteRequest:  timeDelta(tctx.tr.tsTargetWroteRequest, tctx.tr.tsTargetWroteHeaders),
		TargetFirstResponse: timeDelta(tctx.tr.tsTargetFirstResponse, tctx.tr.tsTargetWroteRequest),
	}
	return l, nil
}

//
// Put executes PUT
//
func put(proxyURL string, bck cmn.Bck, object string, cksum *cos.Cksum, reader cos.ReadOpenCloser) error {
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
			Cksum:      cksum,
			Reader:     reader,
		}
	)
	return api.PutObject(args)
}

func prepareGetRequest(proxyURL string, bck cmn.Bck, objName string, offset, length int64) (*http.Request, error) {
	var (
		hdr   http.Header
		query = url.Values{}
	)

	query = cmn.AddBckToQuery(query, bck)
	if etlID != "" {
		query.Add(cmn.URLParamUUID, etlID)
	}
	if length > 0 {
		hdr = cmn.RangeHdr(offset, length)
	}
	reqArgs := cmn.ReqArgs{
		Method: http.MethodGet,
		Base:   proxyURL,
		Path:   cmn.URLPathObjects.Join(bck.Name, objName),
		Query:  query,
		Header: hdr,
	}

	return reqArgs.Req()
}

// getDiscard sends a GET request and discards returned data.
func getDiscard(proxyURL string, bck cmn.Bck, objName string, validate bool, offset, length int64) (int64, error) {
	var hdrCksumValue, hdrCksumType string

	req, err := prepareGetRequest(proxyURL, bck, objName, offset, length)
	if err != nil {
		return 0, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer cos.Close(resp.Body)

	if validate {
		hdrCksumValue = resp.Header.Get(cmn.HdrObjCksumVal)
		hdrCksumType = resp.Header.Get(cmn.HdrObjCksumType)
	}
	src := fmt.Sprintf("GET (object %s from bucket %s)", objName, bck)
	n, cksumValue, err := readResponse(resp, io.Discard, src, hdrCksumType)
	if err != nil {
		return 0, err
	}
	if validate && hdrCksumValue != cksumValue {
		return 0, cmn.NewErrInvalidCksum(hdrCksumValue, cksumValue)
	}
	return n, err
}

// Same as above, but with HTTP trace.
func getTraceDiscard(proxyURL string, bck cmn.Bck, objName string, validate bool,
	offset, length int64) (int64, httpLatencies, error) {
	var hdrCksumValue, hdrCksumType string

	req, err := prepareGetRequest(proxyURL, bck, objName, offset, length)
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
		hdrCksumValue = resp.Header.Get(cmn.HdrObjCksumVal)
		hdrCksumType = resp.Header.Get(cmn.HdrObjCksumType)
	}

	src := fmt.Sprintf("GET (object %s from bucket %s)", objName, bck)
	n, cksumValue, err := readResponse(resp, io.Discard, src, hdrCksumType)
	if err != nil {
		return 0, httpLatencies{}, err
	}
	if validate && hdrCksumValue != cksumValue {
		err = cmn.NewErrInvalidCksum(hdrCksumValue, cksumValue)
	}

	latencies := httpLatencies{
		ProxyConn:           timeDelta(tctx.tr.tsProxyConn, tctx.tr.tsBegin),
		Proxy:               timeDelta(tctx.tr.tsRedirect, tctx.tr.tsProxyConn),
		TargetConn:          timeDelta(tctx.tr.tsTargetConn, tctx.tr.tsRedirect),
		Target:              timeDelta(tctx.tr.tsHTTPEnd, tctx.tr.tsTargetConn),
		PostHTTP:            time.Since(tctx.tr.tsHTTPEnd),
		ProxyWroteHeader:    timeDelta(tctx.tr.tsProxyWroteHeaders, tctx.tr.tsProxyConn),
		ProxyWroteRequest:   timeDelta(tctx.tr.tsProxyWroteRequest, tctx.tr.tsProxyWroteHeaders),
		ProxyFirstResponse:  timeDelta(tctx.tr.tsProxyFirstResponse, tctx.tr.tsProxyWroteRequest),
		TargetWroteHeader:   timeDelta(tctx.tr.tsTargetWroteHeaders, tctx.tr.tsTargetConn),
		TargetWroteRequest:  timeDelta(tctx.tr.tsTargetWroteRequest, tctx.tr.tsTargetWroteHeaders),
		TargetFirstResponse: timeDelta(tctx.tr.tsTargetFirstResponse, tctx.tr.tsTargetWroteRequest),
	}
	return n, latencies, err
}

// getConfig sends a {what:config} request to the url and discard the message
// For testing purpose only
func getConfig(server string) (httpLatencies, error) {
	tctx := newTraceCtx()

	url := server + cmn.URLPathDaemon.S
	req, _ := http.NewRequest(http.MethodGet, url, http.NoBody)
	req.URL.RawQuery = api.GetWhatRawQuery(cmn.GetWhatConfig, "")
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), tctx.trace))

	resp, err := tctx.tracedClient.Do(req)
	if err != nil {
		return httpLatencies{}, err
	}
	defer resp.Body.Close()

	_, err = discardResponse(resp, "GetConfig")
	l := httpLatencies{
		ProxyConn: timeDelta(tctx.tr.tsProxyConn, tctx.tr.tsBegin),
		Proxy:     time.Since(tctx.tr.tsProxyConn),
	}
	return l, err
}

func listObjCallback(ctx *api.ProgressContext) {
	fmt.Printf("\rFetched %d objects", ctx.Info().Count)
	// Final message moves output to new line, to keep output tidy
	if ctx.IsFinished() {
		fmt.Println()
	}
}

// listObjectNames returns a slice of object names of all objects that match the prefix in a bucket.
func listObjectNames(baseParams api.BaseParams, bck cmn.Bck, prefix string) ([]string, error) {
	msg := &cmn.SelectMsg{Prefix: prefix, PageSize: cmn.DefaultListPageSizeAIS}
	ctx := api.NewProgressContext(listObjCallback, longListTime)
	objList, err := api.ListObjectsWithOpts(baseParams, bck, msg, 0, ctx, false)
	if err != nil {
		return nil, err
	}

	objs := make([]string, 0, len(objList.Entries))
	for _, obj := range objList.Entries {
		objs = append(objs, obj.Name)
	}
	return objs, nil
}

func discardResponse(r *http.Response, src string) (int64, error) {
	n, _, err := readResponse(r, io.Discard, src, "")
	return n, err
}

func readResponse(r *http.Response, w io.Writer, src, cksumType string) (int64, string, error) {
	var (
		n          int64
		cksum      *cos.CksumHash
		err        error
		cksumValue string
	)

	if r.StatusCode >= http.StatusBadRequest {
		bytes, err := io.ReadAll(r.Body)
		if err == nil {
			return 0, "", fmt.Errorf("bad status %d from %s, response: %s", r.StatusCode, src, string(bytes))
		}
		return 0, "", fmt.Errorf("bad status %d from %s, err: %v", r.StatusCode, src, err)
	}

	buf, slab := memsys.PageMM().Alloc()
	defer slab.Free(buf)

	n, cksum, err = cos.CopyAndChecksum(w, r.Body, buf, cksumType)
	if err != nil {
		return 0, "", fmt.Errorf("failed to read HTTP response, err: %v", err)
	}
	if cksum != nil {
		cksumValue = cksum.Value()
	}
	return n, cksumValue, nil
}

func timeDelta(time1, time2 time.Time) time.Duration {
	if time1.IsZero() || time2.IsZero() {
		return 0
	}
	return time1.Sub(time2)
}
