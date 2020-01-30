// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
//
//  FIXME -- FIXME: split and transform it into the: a) client API and b) test utilities
//  FIXME -- FIXME: the client API must then move into the api package
//
package tutils

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tutils/tassert"
	jsoniter "github.com/json-iterator/go"
)

var (
	// This value is holds the input of 'proxyURLFlag' from init_tests.go.
	// It is used in DefaultBaseAPIParams to determine if the cluster is running
	// on a
	// 	1. local instance (no docker) 	- works
	//	2. local docker instance		- works
	// 	3. AWS-deployed cluster 		- not tested (but runs mainly with Ansible)
	mockDaemonID       = "MOCK"
	proxyChangeLatency = time.Minute * 2
)

const (
	httpMaxRetries = 5                     // maximum number of retries for an HTTP request
	httpRetrySleep = 30 * time.Millisecond // a sleep between HTTP request retries
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

	// HTTPLatencies stores latency of a http request
	HTTPLatencies struct {
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

type ReqError struct {
	code    int
	message string
}

func (err ReqError) Error() string {
	return err.message
}

func NewReqError(msg string, code int) ReqError {
	return ReqError{
		code:    code,
		message: msg,
	}
}

func PingURL(url string) (err error) {
	addr := strings.TrimPrefix(url, "http://")
	if addr == url {
		addr = strings.TrimPrefix(url, "https://")
	}
	conn, err := net.Dial("tcp", addr)
	if err == nil {
		conn.Close()
	}
	return
}

func readResponse(r *http.Response, w io.Writer, src string, validate bool) (int64, string, error) {
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

	buf, slab := MMSA.Alloc()
	defer slab.Free(buf)

	if validate {
		n, cksumVal, err = cmn.WriteWithHash(w, r.Body, buf)
		if err != nil {
			return 0, "", fmt.Errorf("failed to read HTTP response, err: %v", err)
		}
	} else if n, err = io.CopyBuffer(w, r.Body, buf); err != nil {
		return 0, "", fmt.Errorf("failed to read HTTP response, err: %v", err)
	}

	return n, cksumVal, nil
}

func discardResponse(r *http.Response, src string) (int64, error) {
	n, _, err := readResponse(r, ioutil.Discard, src, false /* validate */)
	return n, err
}

func emitError(r *http.Response, err error, errCh chan error) {
	if err == nil || errCh == nil {
		return
	}

	if r != nil {
		errObj := NewReqError(err.Error(), r.StatusCode)
		errCh <- errObj
	} else {
		errCh <- err
	}
}

//
// GetDiscard sends a GET request and discards returned data
//
func GetDiscard(proxyURL string, bck cmn.Bck, objName string, validate bool, offset, length int64) (int64, error) {
	var (
		hash, hdhash, hdhashtype string
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
		return 0, err
	}
	resp, err := HTTPClientGetPut.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if validate {
		hdhash = resp.Header.Get(cmn.HeaderObjCksumVal)
		hdhashtype = resp.Header.Get(cmn.HeaderObjCksumType)
	}
	v := hdhashtype == cmn.ChecksumXXHash
	n, hash, err := readResponse(resp, ioutil.Discard,
		fmt.Sprintf("GET (object %s from bucket %s)", objName, bck), v)
	if err != nil {
		return 0, err
	}
	if v {
		if hdhash != hash {
			err = cmn.NewInvalidCksumError(hdhash, hash)
		}
	}
	return n, err
}

// same as above with HTTP trace
func GetTraceDiscard(proxyURL string, bck cmn.Bck, objName string, validate bool,
	offset, length int64) (int64, HTTPLatencies, error) {
	var (
		hash, hdhash, hdhashtype string
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
		return 0, HTTPLatencies{}, err
	}

	tctx := newTraceCtx()
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), tctx.trace))

	resp, err := tctx.tracedClient.Do(req)
	if err != nil {
		return 0, HTTPLatencies{}, err
	}
	defer resp.Body.Close()

	tctx.tr.tsHTTPEnd = time.Now()
	if validate {
		hdhash = resp.Header.Get(cmn.HeaderObjCksumVal)
		hdhashtype = resp.Header.Get(cmn.HeaderObjCksumType)
	}

	v := hdhashtype == cmn.ChecksumXXHash
	n, hash, err := readResponse(resp, ioutil.Discard,
		fmt.Sprintf("GET (object %s from bucket %s)", objName, bck), v)
	if err != nil {
		return 0, HTTPLatencies{}, err
	}
	if v {
		if hdhash != hash {
			err = cmn.NewInvalidCksumError(hdhash, hash)
		}
	}

	latencies := HTTPLatencies{
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

//
// Put executes PUT
//
func Put(proxyURL string, bck cmn.Bck, object, hash string, reader cmn.ReadOpenCloser) error {
	var (
		baseParams = api.BaseParams{
			Client: HTTPClientGetPut,
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

// PUT with HTTP trace FIXME: copy-paste
func PutWithTrace(proxyURL string, bck cmn.Bck, object, hash string, reader cmn.ReadOpenCloser) (HTTPLatencies, error) {
	handle, err := reader.Open()
	if err != nil {
		return HTTPLatencies{}, fmt.Errorf("failed to open reader, err: %v", err)
	}
	defer handle.Close()

	query := url.Values{}
	query = cmn.AddBckToQuery(query, bck)
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   proxyURL,
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, object),
		Query:  query,
		BodyR:  handle,
	}
	req, err := reqArgs.Req()
	if err != nil {
		return HTTPLatencies{}, err
	}

	// The HTTP package doesn't automatically set this for files, so it has to be done manually
	// If it wasn't set, we would need to deal with the redirect manually.
	req.GetBody = reader.Open
	if hash != "" {
		req.Header.Set(cmn.HeaderObjCksumType, cmn.ChecksumXXHash)
		req.Header.Set(cmn.HeaderObjCksumVal, hash)
	}

	tctx := newTraceCtx()

	req = req.WithContext(httptrace.WithClientTrace(req.Context(), tctx.trace))
	resp, err := tctx.tracedClient.Do(req)
	if err != nil {
		sleep := httpRetrySleep
		if cmn.IsErrConnectionReset(err) || cmn.IsErrConnectionRefused(err) {
			for i := 0; i < httpMaxRetries && err != nil; i++ {
				time.Sleep(sleep)
				resp, err = tctx.tracedClient.Do(req)
				sleep += sleep / 2
			}
		}
	}

	if err != nil {
		return HTTPLatencies{}, fmt.Errorf("failed to %s, err: %v", http.MethodPut, err)
	}

	defer resp.Body.Close()
	tctx.tr.tsHTTPEnd = time.Now()

	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return HTTPLatencies{}, fmt.Errorf("failed to read response, err: %v", err)
		}
		return HTTPLatencies{}, fmt.Errorf("HTTP error = %d, message = %s", resp.StatusCode, string(b))
	}

	l := HTTPLatencies{
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

func Del(proxyURL string, bck cmn.Bck, object string, wg *sync.WaitGroup, errCh chan error, silent bool) error {
	if wg != nil {
		defer wg.Done()
	}
	if !silent {
		fmt.Printf("DEL: %s\n", object)
	}
	baseParams := BaseAPIParams(proxyURL)
	err := api.DeleteObject(baseParams, bck, object)
	emitError(nil, err, errCh)
	return err
}

func CheckExists(proxyURL string, bck cmn.Bck, objName string) (bool, error) {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, objName) + "?" + cmn.URLParamCheckExists + "=true"
	r, err := HTTPClient.Head(url)
	if err != nil {
		return false, err
	}
	defer func() {
		r.Body.Close()
	}()
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		if r.StatusCode == http.StatusNotFound {
			return false, nil
		}
		b, ioErr := ioutil.ReadAll(r.Body)
		if ioErr != nil {
			err = fmt.Errorf("failed to read response body, err: %v", ioErr)
			return false, err
		}
		err = fmt.Errorf("CheckExists failed: bucket/object: %s/%s, HTTP status: %d, HTTP response: %s",
			bck, objName, r.StatusCode, string(b))
		return false, err
	}
	return true, nil
}

// PutAsync sends a PUT request to the given URL
func PutAsync(wg *sync.WaitGroup, proxyURL string, bck cmn.Bck, object string, reader Reader, errCh chan error) {
	defer wg.Done()
	baseParams := BaseAPIParams(proxyURL)
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     object,
		Hash:       reader.XXHash(),
		Reader:     reader,
	}
	err := api.PutObject(putArgs)
	if err != nil {
		if errCh == nil {
			fmt.Println("Error channel is not given, do not know how to report error", err)
		} else {
			errCh <- err
		}
	}
}

// ReplicateMultipleObjects replicates all the objects in the map bucketToObjects.
// bucketsToObjects is a key value pairing where the keys are bucket names and the
// corresponding value is a slice of objects.
// ReplicateMultipleObjects returns a map of errors where the key is bucket+"/"+object and the
// corresponding value is the error that caused replication to fail.
func ReplicateMultipleObjects(proxyURL string, bucketToObjects map[string][]string) map[string]error {
	objectsWithErrors := make(map[string]error)
	baseParams := BaseAPIParams(proxyURL)
	for bucket, objectList := range bucketToObjects {
		for _, object := range objectList {
			if err := api.ReplicateObject(baseParams, cmn.Bck{Name: bucket}, object); err != nil {
				objectsWithErrors[filepath.Join(bucket, object)] = err
			}
		}
	}
	return objectsWithErrors
}

// ListObjects returns a slice of object names of all objects that match the prefix in a bucket
func ListObjects(proxyURL string, bck cmn.Bck, prefix string, objectCountLimit int) ([]string, error) {
	msg := &cmn.SelectMsg{Prefix: prefix}
	baseParams := BaseAPIParams(proxyURL)

	data, err := api.ListBucket(baseParams, bck, msg, objectCountLimit)
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

// ListObjects returns a slice of object names of all objects that match the prefix in a bucket
func ListObjectsFast(proxyURL string, bck cmn.Bck, prefix string) ([]string, error) {
	baseParams := BaseAPIParams(proxyURL)
	query := url.Values{}
	if prefix != "" {
		query.Add(cmn.URLParamPrefix, prefix)
	}

	data, err := api.ListBucketFast(baseParams, bck, nil, query)
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

// GetConfig sends a {what:config} request to the url and discard the message
// For testing purpose only
func GetConfig(server string) (HTTPLatencies, error) {
	tctx := newTraceCtx()

	url := server + cmn.URLPath(cmn.Version, cmn.Daemon)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.URL.RawQuery = GetWhatRawQuery(cmn.GetWhatConfig, "")
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), tctx.trace))

	resp, err := tctx.tracedClient.Do(req)
	if err != nil {
		return HTTPLatencies{}, err
	}
	defer resp.Body.Close()

	_, err = discardResponse(resp, fmt.Sprintf("Get config"))
	emitError(resp, err, nil)
	l := HTTPLatencies{
		ProxyConn: cmn.TimeDelta(tctx.tr.tsProxyConn, tctx.tr.tsBegin),
		Proxy:     time.Since(tctx.tr.tsProxyConn),
	}
	return l, err
}

func GetPrimaryURL() string {
	primary, err := GetPrimaryProxy(proxyURLReadOnly)
	if err != nil {
		return proxyURLReadOnly
	}
	return primary.URL(cmn.NetworkPublic)
}

// GetPrimaryProxy returns the primary proxy's url of a cluster
func GetPrimaryProxy(proxyURL string) (*cluster.Snode, error) {
	baseParams := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return nil, err
	}
	return smap.ProxySI, err
}

func CreateFreshBucket(t *testing.T, proxyURL string, bck cmn.Bck) {
	DestroyBucket(t, proxyURL, bck)
	baseParams := BaseAPIParams(proxyURL)
	err := api.CreateBucket(baseParams, bck)
	tassert.CheckFatal(t, err)
}

func DestroyBucket(t *testing.T, proxyURL string, bck cmn.Bck) {
	baseParams := BaseAPIParams(proxyURL)
	exists, err := api.DoesBucketExist(baseParams, bck)
	tassert.CheckFatal(t, err)
	if exists {
		err = api.DestroyBucket(baseParams, bck)
		tassert.CheckFatal(t, err)
	}
}

func CleanCloudBucket(t *testing.T, proxyURL string, bck cmn.Bck, prefix string) {
	bck.Provider = cmn.Cloud
	toDelete, err := ListObjects(proxyURL, bck, prefix, 0)
	tassert.CheckFatal(t, err)
	baseParams := BaseAPIParams(proxyURL)
	err = api.DeleteList(baseParams, bck, toDelete, true, 0)
	tassert.CheckFatal(t, err)
}

func GetWhatRawQuery(getWhat string, getProps string) string {
	q := url.Values{}
	q.Add(cmn.URLParamWhat, getWhat)
	if getProps != "" {
		q.Add(cmn.URLParamProps, getProps)
	}
	return q.Encode()
}

func UnregisterNode(proxyURL, sid string) error {
	baseParams := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return fmt.Errorf("api.GetClusterMap failed, err: %v", err)
	}

	target, ok := smap.Tmap[sid]
	var idsToIgnore []string
	if ok {
		idsToIgnore = []string{target.ID()}
	}
	if err := api.UnregisterNode(baseParams, sid); err != nil {
		return err
	}

	// If target does not exists in cluster we should not wait for map version
	// sync because update will not be scheduled
	if ok {
		return WaitMapVersionSync(time.Now().Add(registerTimeout), smap, smap.Version, idsToIgnore)
	}
	return nil
}

func determineReaderType(sgl *memsys.SGL, readerPath, readerType, objName string, size uint64) (reader Reader, err error) {
	if sgl != nil {
		sgl.Reset()
		reader, err = NewSGReader(sgl, int64(size), true /* with Hash */)
	} else {
		if readerType == ReaderTypeFile && readerPath == "" {
			err = fmt.Errorf("path to reader cannot be empty when reader type is %s", ReaderTypeFile)
			return
		}
		// need to ensure that readerPath exists before trying to create a file there
		if err = cmn.CreateDir(readerPath); err != nil {
			return
		}
		reader, err = NewReader(ParamReader{
			Type: readerType,
			SGL:  nil,
			Path: readerPath,
			Name: objName,
			Size: int64(size),
		})
	}
	return
}

func WaitForObjectToBeDowloaded(baseParams api.BaseParams, bck cmn.Bck, objName string, timeout time.Duration) error {
	maxTime := time.Now().Add(timeout)

	for {
		if time.Now().After(maxTime) {
			return fmt.Errorf("timed out when downloading %s/%s", bck, objName)
		}

		reslist, err := api.ListBucket(baseParams, bck, &cmn.SelectMsg{Fast: true}, 0)
		if err != nil {
			return err
		}

		for _, obj := range reslist.Entries {
			if obj.Name == objName {
				return nil
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func EnsureObjectsExist(t *testing.T, params api.BaseParams, bck cmn.Bck, objectsNames ...string) {
	for _, objName := range objectsNames {
		_, err := api.GetObject(params, bck, objName)
		if err != nil {
			t.Errorf("Unexpected GetObject(%s) error: %v.", objName, err)
		}
	}
}

func putObjs(proxyURL string, bck cmn.Bck, readerPath, readerType, objPath string, objSize uint64, sgl *memsys.SGL, errCh chan error, objCh, objsPutCh chan string) {
	var (
		size   = objSize
		reader Reader
		err    error
	)
	for {
		objName, ok := <-objCh
		if !ok {
			return
		}
		if size == 0 {
			size = uint64(cmn.NowRand().Intn(1024)+1) * 1024
		}
		reader, err = determineReaderType(sgl, readerPath, readerType, objName, size)
		if err != nil {
			Logf("Failed to generate random file %s, err: %v\n", path.Join(readerPath, objName), err)
			if errCh != nil {
				errCh <- err
			}
			return
		}

		fullObjName := path.Join(objPath, objName)
		// We could PUT while creating files, but that makes it
		// begin all the puts immediately (because creating random files is fast
		// compared to the listbucket call that getRandomFiles does)
		baseParams := BaseAPIParams(proxyURL)
		putArgs := api.PutObjectArgs{
			BaseParams: baseParams,
			Bck:        bck,
			Object:     fullObjName,
			Hash:       reader.XXHash(),
			Reader:     reader,
			Size:       size,
		}
		err = api.PutObject(putArgs)
		if err != nil {
			if errCh == nil {
				Logf("Error performing PUT of object with random data, provided error channel is nil\n")
			} else {
				errCh <- err
			}
		}
		if objsPutCh != nil {
			objsPutCh <- objName
		}
	}
}

func PutObjsFromList(proxyURL string, bck cmn.Bck, readerPath, readerType, objPath string, objSize uint64, objList []string,
	errCh chan error, objsPutCh chan string, sgl *memsys.SGL, fixedSize ...bool) {
	var (
		wg         = &sync.WaitGroup{}
		objCh      = make(chan string, len(objList))
		numworkers = 10
	)
	// if len(objList) < numworkers, only need as many workers as there are objects to be PUT
	numworkers = cmn.Min(numworkers, len(objList))
	sgls := make([]*memsys.SGL, numworkers)

	// need an SGL for each worker with its size being that of the original SGL
	if sgl != nil {
		slabSize := sgl.Slab().Size()
		for i := 0; i < numworkers; i++ {
			sgls[i] = MMSA.NewSGL(slabSize)
		}
		defer func() {
			for _, sgl := range sgls {
				sgl.Free()
			}
		}()
	}

	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		var sgli *memsys.SGL
		if sgl != nil {
			sgli = sgls[i]
		}
		go func(sgli *memsys.SGL) {
			size := objSize
			// randomize sizes
			if len(fixedSize) == 0 || !fixedSize[0] {
				x := uintptr(unsafe.Pointer(sgli)) & 0xfff
				size = objSize + uint64(x)
			}
			putObjs(proxyURL, bck, readerPath, readerType, objPath, size, sgli, errCh, objCh, objsPutCh)
			wg.Done()
		}(sgli)
	}

	for _, objName := range objList {
		objCh <- objName
	}
	close(objCh)
	wg.Wait()
}

func PutRandObjs(proxyURL string, bck cmn.Bck, readerPath, readerType, objPath string, objSize uint64, numPuts int, errCh chan error, objsPutCh chan string, sgl *memsys.SGL, fixedSize ...bool) {
	var (
		fNameLen = 16
		objList  = make([]string, 0, numPuts)
	)
	for i := 0; i < numPuts; i++ {
		fname := GenRandomString(fNameLen)
		objList = append(objList, fname)
	}
	PutObjsFromList(proxyURL, bck, readerPath, readerType, objPath, objSize, objList, errCh, objsPutCh, sgl, fixedSize...)
}

// Put an object into a cloud bucket and evict it afterwards - can be used to test cold GET
func PutObjectInCloudBucketWithoutCachingLocally(t *testing.T, proxyURL string, bck cmn.Bck,
	object string, objContent cmn.ReadOpenCloser) {
	baseParams := DefaultBaseAPIParams(t)

	err := api.PutObject(api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     object,
		Reader:     objContent,
	})
	tassert.CheckFatal(t, err)

	EvictObjects(t, proxyURL, bck, []string{object})
}

func GetObjectAtime(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, object string, timeFormat string) time.Time {
	msg := &cmn.SelectMsg{Props: cmn.GetPropsAtime, TimeFormat: timeFormat, Prefix: object}
	bucketList, err := api.ListBucket(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)

	for _, entry := range bucketList.Entries {
		if entry.Name == object {
			atime, err := time.Parse(timeFormat, entry.Atime)
			tassert.CheckFatal(t, err)
			return atime
		}
	}

	tassert.Fatalf(t, false, "Object with name %s not present in bucket %s.", object, bck)
	return time.Time{}
}

// WaitForDSortToFinish waits until all dSorts jobs finishe without failure or
// all jobs abort.
func WaitForDSortToFinish(proxyURL, managerUUID string) (allAborted bool, err error) {
	baseParams := BaseAPIParams(proxyURL)
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		allMetrics, err := api.MetricsDSort(baseParams, managerUUID)
		if err != nil {
			return false, err
		}

		allAborted := true
		allFinished := true
		for _, metrics := range allMetrics {
			allAborted = allAborted && metrics.Aborted
			allFinished = allFinished && !metrics.Aborted && metrics.Extraction.Finished && metrics.Sorting.Finished && metrics.Creation.Finished
		}

		if allAborted {
			return true, nil
		}

		if allFinished {
			return false, nil
		}

		time.Sleep(500 * time.Millisecond)
	}
	return false, fmt.Errorf("deadline exceeded")
}

func DefaultBaseAPIParams(t *testing.T) api.BaseParams {
	primary, err := GetPrimaryProxy(proxyURLReadOnly)
	tassert.CheckFatal(t, err)
	return BaseAPIParams(primary.URL(cmn.NetworkPublic))
}

func BaseAPIParams(url string) api.BaseParams {
	return api.BaseParams{
		Client: HTTPClient, // TODO -- FIXME: make use of HTTPClientGetPut as well
		URL:    url,
	}
}

// ParseEnvVariables takes in a .env file and parses its contents
func ParseEnvVariables(fpath string, delimiter ...string) map[string]string {
	m := map[string]string{}
	dlim := "="
	data, err := ioutil.ReadFile(fpath)
	if err != nil {
		return nil
	}

	if len(delimiter) > 0 {
		dlim = delimiter[0]
	}

	paramList := strings.Split(string(data), "\n")
	for _, dat := range paramList {
		datum := strings.Split(dat, dlim)
		// key=val
		if len(datum) == 2 {
			m[datum[0]] = datum[1]
		}
	}
	return m
}

// waitForBucket waits until all targets ack having ais bucket created or deleted
func WaitForBucket(proxyURL string, bck cmn.Bck, exists bool) error {
	baseParams := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return err
	}
	to := time.Now().Add(bucketTimeout)
	for _, s := range smap.Tmap {
		for {
			baseParams := BaseAPIParams(s.URL(cmn.NetworkPublic))
			bucketExists, err := api.DoesBucketExist(baseParams, bck)
			if err != nil {
				return err
			}
			if bucketExists == exists {
				break
			}
			if time.Now().After(to) {
				return fmt.Errorf("wait for ais bucket timed out, target = %s", baseParams.URL)
			}
			time.Sleep(time.Second)
		}
	}
	return nil
}

func EvictObjects(t *testing.T, proxyURL string, bck cmn.Bck, fileslist []string) {
	err := api.EvictList(BaseAPIParams(proxyURL), bck, fileslist, true, 0)
	if err != nil {
		t.Errorf("Evict bucket %s failed, err = %v", bck, err)
	}
}

func GetXactionStats(baseParams api.BaseParams, bck cmn.Bck, kind string) (map[string][]*stats.BaseXactStatsExt, error) {
	return api.MakeXactGetRequest(baseParams, bck, kind, cmn.ActXactStats, true)
}

func allCompleted(targetsStats map[string][]*stats.BaseXactStatsExt) bool {
	for target, targetStats := range targetsStats {
		for _, xaction := range targetStats {
			if xaction.Running() {
				Logf("%s(%d) in progress for %s\n", xaction.Kind(), xaction.ShortID(), target)
				return false
			}
		}
	}
	return true
}

func CheckXactAPIErr(t *testing.T, err error) {
	if err != nil {
		if httpErr, ok := err.(*cmn.HTTPError); !ok {
			t.Fatalf("Unrecognized error from xactions request: [%v]", err)
		} else if httpErr.Status != http.StatusNotFound {
			t.Fatalf("Unable to get global rebalance stats. Error: [%v]", err)
		}
	}
}

// nolint:unparam // for now timeout is always the same but it is better to keep it generalized
func WaitForBucketXactionToComplete(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, kind string, timeout time.Duration) {
	var (
		wg    = &sync.WaitGroup{}
		ch    = make(chan error, 1)
		sleep = 3 * time.Second
		i     time.Duration
	)
	wg.Add(1)
	go func() {
		for {
			time.Sleep(sleep)
			i++
			stats, err := GetXactionStats(baseParams, bck, kind)
			CheckXactAPIErr(t, err)
			if allCompleted(stats) {
				break
			}
			if i == 1 {
				Logf("waiting for %s\n", kind)
			}
			if i*sleep > timeout {
				ch <- errors.New(kind + ": timeout")
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()
	close(ch)
	for err := range ch {
		t.Fatal(err)
	}
}

func WaitForBucketXactionToStart(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, kind string, timeouts ...time.Duration) {
	var (
		start   = time.Now()
		timeout = time.Duration(0)
		logged  = false
	)

	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}

	for {
		stats, err := GetXactionStats(baseParams, bck, kind)
		CheckXactAPIErr(t, err)
		for _, targetStats := range stats {
			for _, xaction := range targetStats {
				if xaction.Running() {
					return // xaction started
				}
			}
		}
		if len(stats) > 0 {
			return // all xaction finished
		}

		if !logged {
			Logf("waiting for %s to start\n", kind)
			logged = true
		}

		if timeout != 0 && time.Since(start) > timeout {
			t.Fatalf("%s has not started before %s", kind, timeout)
			return
		}

		time.Sleep(1 * time.Second)
	}
}

// Waits for both local and global rebalances to complete
// If they were not started, this function treats them as completed
// and returns. If timeout set, if any of rebalances doesn't complete before timeout
// the function ends with fatal
func WaitForRebalanceToComplete(t *testing.T, baseParams api.BaseParams, timeouts ...time.Duration) {
	start := time.Now()
	time.Sleep(time.Second * 10)
	wg := &sync.WaitGroup{}
	wg.Add(2)
	ch := make(chan error, 2)

	timeout := time.Duration(0)
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	sleep := time.Second * 10
	go func() {
		var logged bool
		defer wg.Done()
		for {
			time.Sleep(sleep)
			globalRebalanceStats, err := GetXactionStats(baseParams, cmn.Bck{}, cmn.ActGlobalReb)
			CheckXactAPIErr(t, err)

			if allCompleted(globalRebalanceStats) {
				return
			}
			if !logged {
				Logf("waiting for global rebalance\n")
				logged = true
			}

			if timeout.Nanoseconds() != 0 && time.Since(start) > timeout {
				ch <- errors.New("global rebalance has not completed before " + timeout.String())
				return
			}
		}
	}()

	go func() {
		var logged bool
		defer wg.Done()
		for {
			time.Sleep(sleep)
			localRebalanceStats, err := GetXactionStats(baseParams, cmn.Bck{}, cmn.ActLocalReb)
			CheckXactAPIErr(t, err)

			if allCompleted(localRebalanceStats) {
				return
			}
			if !logged {
				Logf("waiting for local rebalance\n")
				logged = true
			}

			if timeout.Nanoseconds() != 0 && time.Since(start) > timeout {
				ch <- errors.New("global rebalance has not completed before " + timeout.String())
				return
			}
		}
	}()

	wg.Wait()
	close(ch)

	for err := range ch {
		t.Fatal(err)
	}
}

func GetClusterStats(t *testing.T, proxyURL string) (stats stats.ClusterStats) {
	baseParams := BaseAPIParams(proxyURL)
	clusterStats, err := api.GetClusterStats(baseParams)
	tassert.CheckFatal(t, err)
	return clusterStats
}

func GetNamedTargetStats(trunner *stats.Trunner, name string) int64 {
	v, ok := trunner.Core.Tracker[name]
	if !ok {
		return 0
	}
	return v.Value
}

func GetDaemonStats(t *testing.T, url string) (stats map[string]interface{}) {
	q := GetWhatRawQuery(cmn.GetWhatStats, "")
	url = fmt.Sprintf("%s?%s", url+cmn.URLPath(cmn.Version, cmn.Daemon), q)
	resp, err := DefaultHTTPClient.Get(url)
	if err != nil {
		t.Fatalf("Failed to perform get, err = %v", err)
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body, err = %v", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		t.Fatalf("HTTP error = %d, message = %s", err, string(b))
	}

	dec := jsoniter.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	// If this isn't used, json.Unmarshal converts uint32s to floats, losing precision
	err = dec.Decode(&stats)
	if err != nil {
		t.Fatalf("Failed to unmarshal config: %v", err)
	}

	return
}

func GetClusterMap(t *testing.T, url string) *cluster.Smap {
	baseParams := BaseAPIParams(url)
	time.Sleep(time.Second * 2)
	smap, err := api.GetClusterMap(baseParams)
	tassert.CheckFatal(t, err)
	return smap
}

func GetClusterConfig(t *testing.T) (config *cmn.Config) {
	proxyURL := GetPrimaryURL()
	primary, err := GetPrimaryProxy(proxyURL)
	tassert.CheckFatal(t, err)
	return GetDaemonConfig(t, primary.ID())
}

func GetDaemonConfig(t *testing.T, nodeID string) (config *cmn.Config) {
	var (
		err        error
		proxyURL   = GetPrimaryURL()
		baseParams = BaseAPIParams(proxyURL)
	)
	config, err = api.GetDaemonConfig(baseParams, nodeID)
	tassert.CheckFatal(t, err)
	return
}

func SetDaemonConfig(t *testing.T, proxyURL string, nodeID string, nvs cmn.SimpleKVs) {
	baseParams := BaseAPIParams(proxyURL)
	err := api.SetDaemonConfig(baseParams, nodeID, nvs)
	tassert.CheckFatal(t, err)
}

func SetClusterConfig(t *testing.T, nvs cmn.SimpleKVs) {
	proxyURL := GetPrimaryURL()
	baseParams := BaseAPIParams(proxyURL)
	err := api.SetClusterConfig(baseParams, nvs)
	tassert.CheckFatal(t, err)
}
