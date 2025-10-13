// Package aisloader
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

package aisloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/tools/readers"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

const longListTime = 10 * time.Second // list-objects progress

var (
	// see related command-line: `transportArgs.Timeout` and UseHTTPS
	cargs = cmn.TransportArgs{
		UseHTTPProxyEnv: true,
	}
	// NOTE: client X.509 certificate and other `cmn.TLSArgs` variables can be provided via (os.Getenv) environment.
	// See also:
	// - docs/aisloader.md, section "Environment variables"
	// - AIS_ENDPOINT and aisEndpoint
	sargs = cmn.TLSArgs{
		SkipVerify: true,
	}
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
	tracePutter struct {
		tctx   *traceCtx
		cksum  *cos.Cksum
		reader cos.ReadOpenCloser
	}

	// httpLatencies stores latency of a http request
	httpLatencies struct {
		ProxyConn           time.Duration // from (request is created) to (proxy connection is established)
		Proxy               time.Duration // from (proxy connection is established) to redirected
		TargetConn          time.Duration // from (request is redirected) to (target connection is established)
		Target              time.Duration // from (target connection is established) to (request is completed)
		PostHTTP            time.Duration // from http ends to after read data from http response and verify hash (if specified)
		ProxyWroteHeader    time.Duration // from ProxyConn to header is written
		ProxyWroteRequest   time.Duration // from ProxyWroteHeader to response body is written
		ProxyFirstResponse  time.Duration // from ProxyWroteRequest to first byte of response
		TargetWroteHeader   time.Duration // from TargetConn to header is written
		TargetWroteRequest  time.Duration // from TargetWroteHeader to response body is written
		TargetFirstResponse time.Duration // from TargetWroteRequest to first byte of response
	}
)

////////////////////////
// traceableTransport //
////////////////////////

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

func (t *traceableTransport) set(l *httpLatencies) {
	l.ProxyConn = timeDelta(t.tsProxyConn, t.tsBegin)
	l.Proxy = timeDelta(t.tsRedirect, t.tsProxyConn)
	l.TargetConn = timeDelta(t.tsTargetConn, t.tsRedirect)
	l.Target = timeDelta(t.tsHTTPEnd, t.tsTargetConn)
	l.PostHTTP = time.Since(t.tsHTTPEnd)
	l.ProxyWroteHeader = timeDelta(t.tsProxyWroteHeaders, t.tsProxyConn)
	l.ProxyWroteRequest = timeDelta(t.tsProxyWroteRequest, t.tsProxyWroteHeaders)
	l.ProxyFirstResponse = timeDelta(t.tsProxyFirstResponse, t.tsProxyWroteRequest)
	l.TargetWroteHeader = timeDelta(t.tsTargetWroteHeaders, t.tsTargetConn)
	l.TargetWroteRequest = timeDelta(t.tsTargetWroteRequest, t.tsTargetWroteHeaders)
	l.TargetFirstResponse = timeDelta(t.tsTargetFirstResponse, t.tsTargetWroteRequest)
}

//////////////////////////////////
// detailed http trace _putter_ //
//////////////////////////////////

// implements callback of the type `api.NewRequestCB`
func (putter *tracePutter) do(reqArgs *cmn.HreqArgs) (*http.Request, error) {
	req, err := reqArgs.ReqDeprecated()
	if err != nil {
		return nil, err
	}

	// The HTTP package doesn't automatically set this for files, so it has to be done manually
	// If it wasn't set, we would need to deal with the redirect manually.
	req.GetBody = func() (io.ReadCloser, error) {
		return putter.reader.Open()
	}
	if putter.cksum != nil {
		req.Header.Set(apc.HdrObjCksumType, putter.cksum.Ty())
		req.Header.Set(apc.HdrObjCksumVal, putter.cksum.Val())
	}
	return req.WithContext(httptrace.WithClientTrace(req.Context(), putter.tctx.trace)), nil
}

// a bare-minimum (e.g. not passing checksum or any other metadata)
func s3put(bck cmn.Bck, objName string, reader cos.ReadOpenCloser) (err error) {
	uploader := s3manager.NewUploader(s3svc)
	_, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
		Body:   reader,
	})
	if erc := reader.Close(); erc != nil && err == nil {
		err = erc
	}
	return err
}

func put(proxyURL string, bck cmn.Bck, objName string, cksum *cos.Cksum, reader cos.ReadOpenCloser) (err error) {
	var (
		baseParams = api.BaseParams{
			Client: runParams.bp.Client,
			URL:    proxyURL,
			Method: http.MethodPut,
			Token:  loggedUserToken,
			UA:     ua,
		}
		args = api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    objName,
			Cksum:      cksum,
			Reader:     reader,
			SkipVC:     true,
		}
	)
	_, err = api.PutObject(&args)
	return
}

// putMultipart performs multipart upload for the given object
func putMultipart(proxyURL string, bck cmn.Bck, objName string, size int64, numChunks int, cksumType string) error {
	baseParams := api.BaseParams{
		Client: runParams.bp.Client,
		URL:    proxyURL,
		Token:  loggedUserToken,
		UA:     ua,
	}

	// Create multipart upload
	uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
	if err != nil {
		return fmt.Errorf("failed to create multipart upload for %s: %w", objName, err)
	}

	// Upload parts in parallel
	var (
		partNumbers = make([]int, numChunks)
		mu          = &sync.Mutex{}
		group       = &errgroup.Group{}
	)

	for i := range numChunks {
		var (
			partNum    = i + 1
			offset     = uint64(partNum-1) * uint64(size) / uint64(numChunks)
			nextOffset = uint64(partNum) * uint64(size) / uint64(numChunks)
			partSize   = nextOffset - offset
		)
		group.Go(func(partNum int) func() error {
			return func() error {
				// Create reader for this part (simplified - just use random data)
				partReader, err := readers.NewRand(int64(partSize), cksumType)
				if err != nil {
					return fmt.Errorf("failed to create reader for part %d of %s: %w", partNum, objName, err)
				}

				putPartArgs := &api.PutPartArgs{
					PutArgs: api.PutArgs{
						BaseParams: baseParams,
						Bck:        bck,
						ObjName:    objName,
						Cksum:      partReader.Cksum(),
						Reader:     partReader,
						Size:       partSize,
						SkipVC:     true,
					},
					UploadID:   uploadID,
					PartNumber: partNum,
				}

				if err := api.UploadPart(putPartArgs); err != nil {
					return fmt.Errorf("failed to upload part %d of %s: %w", partNum, objName, err)
				}

				mu.Lock()
				partNumbers[partNum-1] = partNum
				mu.Unlock()

				return nil
			}
		}(partNum))
	}

	// Wait for all parts to complete
	if err := group.Wait(); err != nil {
		if abortErr := api.AbortMultipartUpload(baseParams, bck, objName, uploadID); abortErr != nil {
			return fmt.Errorf("failed to upload parts and failed to abort upload %s: upload error: %w, abort error: %v", objName, err, abortErr)
		}
		return fmt.Errorf("failed to upload parts of %s: %w", objName, err)
	}

	// Complete multipart upload
	if err := api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, partNumbers); err != nil {
		if abortErr := api.AbortMultipartUpload(baseParams, bck, objName, uploadID); abortErr != nil {
			return fmt.Errorf("failed to complete multipart upload and failed to abort %s: complete error: %w, abort error: %v", objName, err, abortErr)
		}
		return fmt.Errorf("failed to complete multipart upload for %s: %w", objName, err)
	}

	return nil
}

// PUT with HTTP trace
func putWithTrace(proxyURL string, bck cmn.Bck, objName string, latencies *httpLatencies, cksum *cos.Cksum, reader cos.ReadOpenCloser) error {
	q := make(url.Values, 1)
	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPut
		reqArgs.Base = proxyURL
		reqArgs.Path = apc.URLPathObjects.Join(bck.Name, objName)
		bck.SetQuery(q)
		reqArgs.Query = q
		reqArgs.BodyR = reader
	}
	putter := tracePutter{
		tctx:   newTraceCtx(proxyURL),
		cksum:  cksum,
		reader: reader,
	}
	_, err := api.DoWithRetry(putter.tctx.tracedClient, putter.do, reqArgs) //nolint:bodyclose // it's closed inside
	cmn.FreeHra(reqArgs)
	if err != nil {
		return err
	}
	tctx := putter.tctx
	tctx.tr.tsHTTPEnd = time.Now()

	tctx.tr.set(latencies)
	return nil
}

func newTraceCtx(proxyURL string) *traceCtx {
	var (
		tctx      = &traceCtx{}
		transport = cmn.NewTransport(cargs)
		err       error
	)
	if cos.IsHTTPS(proxyURL) {
		transport.TLSClientConfig, err = cmn.NewTLS(sargs, false /*intra-cluster*/)
		cos.AssertNoErr(err)
	}
	tctx.tr = &traceableTransport{
		transport: transport,
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

func newGetRequest(proxyURL string, bck cmn.Bck, objName string, offset, length int64, latest bool) (*http.Request, error) {
	var (
		hdr   http.Header
		query = url.Values{}
	)
	query = bck.AddToQuery(query)
	if etlName != "" {
		query.Add(apc.QparamETLName, etlName)
	}
	if latest {
		query.Add(apc.QparamLatestVer, "true")
	}
	if length > 0 {
		rng := cmn.MakeRangeHdr(offset, length)
		hdr = http.Header{cos.HdrRange: []string{rng}}
	}
	reqArgs := cmn.HreqArgs{
		Method: http.MethodGet,
		Base:   proxyURL,
		Path:   apc.URLPathObjects.Join(bck.Name, objName),
		Query:  query,
		Header: hdr,
	}
	return reqArgs.Req()
}

func s3getDiscard(bck cmn.Bck, objName string) (int64, error) {
	obj, err := s3svc.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
	})
	if err != nil {
		if obj != nil && obj.Body != nil {
			io.Copy(io.Discard, obj.Body)
			obj.Body.Close()
		}
		return 0, err // detailed enough
	}

	var size, n int64
	size = *obj.ContentLength
	n, err = io.Copy(io.Discard, obj.Body)
	obj.Body.Close()

	if err != nil {
		return n, fmt.Errorf("failed to GET %s and discard it (%d, %d): %v", bck.Cname(objName), n, size, err)
	}
	if n != size {
		err = fmt.Errorf("failed to GET %s: wrong size (%d, %d)", bck.Cname(objName), n, size)
	}
	return size, err
}

// getDiscard sends a GET request and discards returned data.
func getDiscard(proxyURL string, bck cmn.Bck, objName string, offset, length int64, validate, latest bool) (int64, error) {
	req, err := newGetRequest(proxyURL, bck, objName, offset, length, latest)
	if err != nil {
		return 0, err
	}
	api.SetAuxHeaders(req, &runParams.bp)

	resp, err := runParams.bp.Client.Do(req)

	cmn.HreqFree(req)
	if err != nil {
		return 0, err
	}

	var hdrCksumValue, hdrCksumType string
	if validate {
		hdrCksumValue = resp.Header.Get(apc.HdrObjCksumVal)
		hdrCksumType = resp.Header.Get(apc.HdrObjCksumType)
	}
	src := "GET " + bck.Cname(objName)
	n, cksumValue, err := readDiscard(resp, src, hdrCksumType)

	resp.Body.Close()
	if err != nil {
		return 0, err
	}
	if validate && hdrCksumValue != cksumValue {
		return 0, cmn.NewErrInvalidCksum(hdrCksumValue, cksumValue)
	}
	return n, err
}

// Same as above, but with HTTP trace.
func getTraceDiscard(proxyURL string, bck cmn.Bck, objName string, latencies *httpLatencies, offset, length int64, validate, latest bool) (int64, error) {
	var (
		hdrCksumValue string
		hdrCksumType  string
	)
	req, err := newGetRequest(proxyURL, bck, objName, offset, length, latest)
	if err != nil {
		return 0, err
	}

	tctx := newTraceCtx(proxyURL)
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), tctx.trace))

	resp, err := tctx.tracedClient.Do(req)

	cmn.HreqFree(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	tctx.tr.tsHTTPEnd = time.Now()
	if validate {
		hdrCksumValue = resp.Header.Get(apc.HdrObjCksumVal)
		hdrCksumType = resp.Header.Get(apc.HdrObjCksumType)
	}

	src := "GET " + bck.Cname(objName)
	n, cksumValue, err := readDiscard(resp, src, hdrCksumType)
	if err != nil {
		return 0, err
	}
	if validate && hdrCksumValue != cksumValue {
		err = cmn.NewErrInvalidCksum(hdrCksumValue, cksumValue)
	}

	tctx.tr.set(latencies)
	return n, err
}

// TODO: revisit; consume/discard TAR
func getBatchDiscard(proxyURL string, bck cmn.Bck, req *apc.MossReq) (int64, error) {
	var buf bytes.Buffer
	bp := api.BaseParams{
		URL:    proxyURL,
		Client: runParams.bp.Client,
		Token:  runParams.bp.Token,
		UA:     runParams.bp.UA,
	}
	_, err := api.GetBatch(bp, bck, req, &buf)
	return int64(buf.Len()), err
}

func listObjCallback(ctx *api.LsoCounter) {
	if ctx.Count() < 0 {
		return
	}
	fmt.Printf("\rListing %s objects", cos.FormatBigInt(ctx.Count()))
	if ctx.IsFinished() {
		fmt.Println()
	}
}

// listObjectNames returns a slice of object names of all objects that match the prefix in a bucket.
func listObjectNames(p *params) ([]string, error) {
	var (
		bp       = p.bp
		bck      = p.bck
		cached   = p.cached
		listDirs = p.listDirs
		msg      = &apc.LsoMsg{Prefix: p.subDir}
	)
	if cached {
		msg.Flags |= apc.LsCached // remote bucket: in-cluster objects only
	}
	if !listDirs {
		msg.Flags |= apc.LsNoDirs // aisloader's default (to override, use --list-dirs)
	}
	args := api.ListArgs{Callback: listObjCallback, CallAfter: longListTime}
	lst, err := api.ListObjects(bp, bck, msg, args)
	if err != nil {
		return nil, err
	}

	objs := make([]string, 0, len(lst.Entries))
	for _, obj := range lst.Entries {
		objs = append(objs, obj.Name)
	}
	return objs, nil
}

func initS3Svc() error {
	// '--s3profile' takes precedence
	if s3Profile == "" {
		if profile := os.Getenv(env.AWSProfile); profile != "" {
			s3Profile = profile
		}
	}
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithSharedConfigProfile(s3Profile),
	)
	if err != nil {
		return err
	}
	if s3Endpoint != "" {
		cfg.BaseEndpoint = aws.String(s3Endpoint)
	}
	if cfg.Region == "" {
		cfg.Region = env.AwsDefaultRegion()
	}

	s3svc = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = s3UsePathStyle
	})
	return nil
}

func s3ListObjects() ([]string, error) {
	// first page
	params := &s3.ListObjectsV2Input{Bucket: aws.String(runParams.bck.Name)}
	params.MaxKeys = aws.Int32(apc.MaxPageSizeAWS)

	prev := mono.NanoTime()
	resp, err := s3svc.ListObjectsV2(context.Background(), params)
	if err != nil {
		return nil, err
	}

	var (
		token string
		l     = len(resp.Contents)
	)
	if resp.NextContinuationToken != nil {
		token = *resp.NextContinuationToken
	}
	if token != "" {
		l = 16 * apc.MaxPageSizeAWS
	}
	names := make([]string, 0, l)
	for _, object := range resp.Contents {
		names = append(names, *object.Key)
	}
	if token == "" {
		return names, nil
	}

	// get all the rest pages in one fell swoop
	var eol bool
	for token != "" {
		params.ContinuationToken = &token
		resp, err = s3svc.ListObjectsV2(context.Background(), params)
		if err != nil {
			return nil, err
		}
		for _, object := range resp.Contents {
			names = append(names, *object.Key)
		}
		token = ""
		if resp.NextContinuationToken != nil {
			token = *resp.NextContinuationToken
		}
		now := mono.NanoTime()
		if time.Duration(now-prev) >= longListTime {
			fmt.Printf("\rListing %s objects", cos.FormatBigInt(len(names)))
			prev = now
			eol = true
		}
	}
	if eol {
		fmt.Println()
	}
	return names, nil
}

func readDiscard(r *http.Response, tag, cksumType string) (int64, string, error) {
	if r.StatusCode >= http.StatusBadRequest {
		bytes, err := cos.ReadAll(r.Body)
		if err == nil {
			return 0, "", fmt.Errorf("bad status %d from %s, response: %s", r.StatusCode, tag, string(bytes))
		}
		return 0, "", fmt.Errorf("bad status %d from %s: %v", r.StatusCode, tag, err)
	}

	n, cksum, err := cos.CopyAndChecksum(io.Discard, r.Body, nil, cksumType)
	if err != nil {
		return 0, "", fmt.Errorf("failed to read HTTP response, err: %v", err)
	}
	var cksumValue string
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
