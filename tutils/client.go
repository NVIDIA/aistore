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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

var (
	// This value is holds the input of 'proxyURLFlag' from init_tests.go.
	// It is used in DefaultBaseAPIParams to determine if the cluster is running
	// on a
	// 	1. local instance (no docker) 	- works
	//	2. local docker instance		- works
	// 	3. AWS-deployed cluster 		- not tested (but runs mainly with Ansible)
	readProxyURL = "http://localhost:8080" // Just setting a default, will get actual value later
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

func Tcping(url string) (err error) {
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

func readResponse(r *http.Response, w io.Writer, err error, src string, validate bool) (int64, string, error) {
	var (
		length   int64
		cksumVal string
	)

	// Note: This code can use some cleanup.
	if err == nil {
		if r.StatusCode >= http.StatusBadRequest {
			bytes, err := ioutil.ReadAll(r.Body)
			if err == nil {
				return 0, "", fmt.Errorf("bad status %d from %s, response: %s", r.StatusCode, src, string(bytes))
			}
			return 0, "", fmt.Errorf("bad status %d from %s, err: %v", r.StatusCode, src, err)
		}

		buf, slab := Mem2.AllocFromSlab2(cmn.DefaultBufSize)
		defer slab.Free(buf)
		if validate {
			length, cksumVal, err = cmn.WriteWithHash(w, r.Body, buf)
			if err != nil {
				return 0, "", fmt.Errorf("failed to read HTTP response, err: %v", err)
			}
		} else {
			if length, err = io.CopyBuffer(w, r.Body, buf); err != nil {
				return 0, "", fmt.Errorf("failed to read HTTP response, err: %v", err)
			}
		}
	} else {
		return 0, "", fmt.Errorf("%s failed, err: %v", src, err)
	}

	return length, cksumVal, nil
}

func discardResponse(r *http.Response, err error, src string) (int64, error) {
	len, _, err := readResponse(r, ioutil.Discard, err, src, false /* validate */)
	return len, err
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

// Get sends a GET request to url and discards returned data
func GetWithMetrics(url, bucket string, keyname string, validate bool, offset, length int64) (int64, HTTPLatencies, error) {
	var (
		hash, hdhash, hdhashtype string
		w                        = ioutil.Discard
	)

	tctx := newTraceCtx()

	url += cmn.URLPath(cmn.Version, cmn.Objects, bucket, keyname)
	if length > 0 {
		url += fmt.Sprintf("?%s=%d&%s=%d",
			cmn.URLParamOffset, offset,
			cmn.URLParamLength, length)
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, HTTPLatencies{}, err
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), tctx.trace))

	resp, err := tctx.tracedClient.Do(req)
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	tctx.tr.tsHTTPEnd = time.Now()

	if validate && resp != nil {
		hdhash = resp.Header.Get(cmn.HeaderObjCksumVal)
		hdhashtype = resp.Header.Get(cmn.HeaderObjCksumType)
	}

	v := hdhashtype == cmn.ChecksumXXHash
	len, hash, err := readResponse(resp, w, err, fmt.Sprintf("GET (object %s from bucket %s)", keyname, bucket), v)
	if err != nil {
		return 0, HTTPLatencies{}, err
	}
	if v {
		if hdhash != hash {
			err = cmn.NewInvalidCksumError(hdhash, hash)
		}
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
	return len, l, err
}

// Put sends a PUT request to url
func PutWithMetrics(url, bucket, object, hash string, reader cmn.ReadOpenCloser) (HTTPLatencies, error) {
	handle, err := reader.Open()
	if err != nil {
		return HTTPLatencies{}, fmt.Errorf("failed to open reader, err: %v", err)
	}
	defer handle.Close()

	url += cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	req, err := http.NewRequest(http.MethodPut, url, handle)
	if err != nil {
		return HTTPLatencies{}, err
	}

	// The HTTP package doesn't automatically set this for files, so it has to be done manually
	// If it wasn't set, we would need to deal with the redirect manually.
	req.GetBody = func() (io.ReadCloser, error) {
		return reader.Open()
	}
	if hash != "" {
		req.Header.Set(cmn.HeaderObjCksumType, cmn.ChecksumXXHash)
		req.Header.Set(cmn.HeaderObjCksumVal, hash)
	}

	tctx := newTraceCtx()

	req = req.WithContext(httptrace.WithClientTrace(req.Context(), tctx.trace))
	resp, err := tctx.tracedClient.Do(req)
	if err != nil {
		sleep := httpRetrySleep
		if cmn.IsErrBrokenPipe(err) || cmn.IsErrConnectionRefused(err) {
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

	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()
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

func Del(proxyURL, bucket, object, bckProvider string, wg *sync.WaitGroup, errCh chan error, silent bool) error {
	if wg != nil {
		defer wg.Done()
	}
	if !silent {
		fmt.Printf("DEL: %s\n", object)
	}
	baseParams := BaseAPIParams(proxyURL)
	err := api.DeleteObject(baseParams, bucket, object, bckProvider)
	emitError(nil, err, errCh)
	return err
}

func IsCached(proxyURL, bucket, objname string) (bool, error) {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, objname) + "?" + cmn.URLParamCheckCached + "=true"
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
		err = fmt.Errorf("IsCached failed: bucket/object: %s/%s, HTTP status: %d, HTTP response: %s",
			bucket, objname, r.StatusCode, string(b))
		return false, err
	}
	return true, nil
}

// PutAsync sends a PUT request to the given URL
func PutAsync(wg *sync.WaitGroup, proxyURL, bucket, object string, reader Reader, errCh chan error) {
	defer wg.Done()
	baseParams := BaseAPIParams(proxyURL)
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bucket:     bucket,
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
			if err := api.ReplicateObject(baseParams, bucket, object); err != nil {
				objectsWithErrors[filepath.Join(bucket, object)] = err
			}
		}
	}
	return objectsWithErrors
}

// ListObjects returns a slice of object names of all objects that match the prefix in a bucket
func ListObjects(proxyURL, bucket, bckProvider, prefix string, objectCountLimit int) ([]string, error) {
	msg := &cmn.GetMsg{GetPrefix: prefix}
	baseParams := BaseAPIParams(proxyURL)
	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, bckProvider)

	data, err := api.ListBucket(baseParams, bucket, msg, objectCountLimit, query)
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
func ListObjectsFast(proxyURL, bucket, bckProvider, prefix string) ([]string, error) {
	baseParams := BaseAPIParams(proxyURL)
	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, bckProvider)
	if prefix != "" {
		query.Add(cmn.URLParamPrefix, prefix)
	}

	data, err := api.ListBucketFast(baseParams, bucket, nil, query)
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
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	_, err = discardResponse(resp, err, fmt.Sprintf("Get config"))
	emitError(resp, err, nil)
	l := HTTPLatencies{
		ProxyConn: cmn.TimeDelta(tctx.tr.tsProxyConn, tctx.tr.tsBegin),
		Proxy:     time.Since(tctx.tr.tsProxyConn),
	}
	return l, err
}

// GetPrimaryProxy returns the primary proxy's url of a cluster
func GetPrimaryProxy(proxyURL string) (string, error) {
	readProxyURL = proxyURL //Sets the appropriate proxy url based on local, docker or AISURL environment var
	baseParams := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return "", err
	}

	return smap.ProxySI.PublicNet.DirectURL, nil
}

// DoesLocalBucketExist queries a proxy or target to get a list of all local buckets, returns true if
// the bucket exists.
func DoesLocalBucketExist(serverURL string, bucket string) (bool, error) {
	baseParams := BaseAPIParams(serverURL)
	buckets, err := api.GetBucketNames(baseParams, cmn.LocalBs)
	if err != nil {
		return false, err
	}

	for _, b := range buckets.Local {
		if b == bucket {
			return true, nil
		}
	}

	return false, nil
}

func CreateFreshLocalBucket(t *testing.T, proxyURL, bucketFQN string) {
	DestroyLocalBucket(t, proxyURL, bucketFQN)
	baseParams := BaseAPIParams(proxyURL)
	err := api.CreateLocalBucket(baseParams, bucketFQN)
	CheckFatal(err, t)
}

func DestroyLocalBucket(t *testing.T, proxyURL, bucket string) {
	exists, err := DoesLocalBucketExist(proxyURL, bucket)
	CheckFatal(err, t)
	if exists {
		baseParams := BaseAPIParams(proxyURL)
		err = api.DestroyLocalBucket(baseParams, bucket)
		CheckFatal(err, t)
	}
}

func GetWhatRawQuery(getWhat string, getProps string) string {
	q := url.Values{}
	q.Add(cmn.URLParamWhat, getWhat)
	if getProps != "" {
		q.Add(cmn.URLParamProps, getProps)
	}
	return q.Encode()
}

func UnregisterTarget(proxyURL, sid string) error {
	baseParams := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return fmt.Errorf("api.GetClusterMap failed, err: %v", err)
	}

	target, ok := smap.Tmap[sid]
	var idsToIgnore []string
	if ok {
		idsToIgnore = []string{target.DaemonID}
	}
	if err = api.UnregisterTarget(baseParams, sid); err != nil {
		return err
	}

	// If target does not exists in cluster we should not wait for map version
	// sync because update will not be scheduled
	if ok {
		return WaitMapVersionSync(time.Now().Add(registerTimeout), smap, smap.Version, idsToIgnore)
	}
	return nil
}

func RegisterTarget(proxyURL string, targetNode *cluster.Snode, smap cluster.Smap) error {
	_, ok := smap.Tmap[targetNode.DaemonID]
	baseParams := BaseAPIParams(proxyURL)
	if err := api.RegisterTarget(baseParams, targetNode); err != nil {
		return err
	}

	// If target is already in cluster we should not wait for map version
	// sync because update will not be scheduled
	if !ok {
		return WaitMapVersionSync(time.Now().Add(registerTimeout), smap, smap.Version, []string{})
	}
	return nil
}

func WaitMapVersionSync(timeout time.Time, smap cluster.Smap, prevVersion int64, idsToIgnore []string) error {
	inList := func(s string, values []string) bool {
		for _, v := range values {
			if s == v {
				return true
			}
		}

		return false
	}

	checkAwaitingDaemon := func(smap cluster.Smap, idsToIgnore []string) (string, string, bool) {
		for _, d := range smap.Pmap {
			if !inList(d.DaemonID, idsToIgnore) {
				return d.DaemonID, d.PublicNet.DirectURL, true
			}
		}
		for _, d := range smap.Tmap {
			if !inList(d.DaemonID, idsToIgnore) {
				return d.DaemonID, d.PublicNet.DirectURL, true
			}
		}

		return "", "", false
	}

	for {
		sid, url, exists := checkAwaitingDaemon(smap, idsToIgnore)
		if !exists {
			break
		}
		baseParams := BaseAPIParams(url)
		daemonSmap, err := api.GetClusterMap(baseParams)
		if err != nil && !cmn.IsErrConnectionRefused(err) {
			return err
		}

		if err == nil && daemonSmap.Version > prevVersion {
			idsToIgnore = append(idsToIgnore, sid)
			smap = daemonSmap // update smap for newer version
			continue
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("timed out waiting for sync-ed Smap version > %d from %s (v%d)", prevVersion, url, smap.Version)
		}

		fmt.Printf("wait for Smap > v%d: %s\n", prevVersion, url)
		time.Sleep(time.Second)
	}
	return nil
}

// TODO: rename, and move to the api package
func GetXactionResponse(proxyURL string, kind string) ([]byte, error) {
	q := GetWhatRawQuery(cmn.GetWhatXaction, kind)
	url := fmt.Sprintf("%s?%s", proxyURL+cmn.URLPath(cmn.Version, cmn.Cluster), q)
	r, err := HTTPClient.Get(url)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()

	if err != nil {
		return []byte{}, err
	}

	if r != nil && r.StatusCode >= http.StatusBadRequest {
		return []byte{},
			fmt.Errorf("GET xaction, HTTP Status %d", r.StatusCode)
	}

	var response []byte
	response, err = ioutil.ReadAll(r.Body)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to read response, err: %v", err)
	}

	return response, nil
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

func putObjs(proxyURL, bucket, readerPath, readerType, objPath string, objSize uint64, sgl *memsys.SGL, errCh chan error, objCh, objsPutCh chan string) {
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
			random := rand.New(rand.NewSource(time.Now().UnixNano()))
			size = uint64(random.Intn(1024)+1) * 1024
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
			Bucket:     bucket,
			Object:     fullObjName,
			Hash:       reader.XXHash(),
			Reader:     reader,
		}
		err = api.PutObject(putArgs)
		if err != nil {
			if errCh == nil {
				Logf("Error performing PUT of object with random data, provided error channel is nil\n")
			} else {
				errCh <- err
			}
		}
		objsPutCh <- objName
	}
}

func PutObjsFromList(proxyURL, bucket, readerPath, readerType, objPath string, objSize uint64, objList []string,
	errCh chan error, objsPutCh chan string, sgl *memsys.SGL) {
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
			sgls[i] = Mem2.NewSGL(slabSize)
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
			putObjs(proxyURL, bucket, readerPath, readerType, objPath, objSize, sgli, errCh, objCh, objsPutCh)
			wg.Done()
		}(sgli)
	}

	for _, objName := range objList {
		objCh <- objName
	}
	close(objCh)
	wg.Wait()
}

func PutRandObjs(proxyURL, bucket, readerPath, readerType, objPath string, objSize uint64, numPuts int,
	errCh chan error, objsPutCh chan string, sgl *memsys.SGL) {

	var fnlen = 10
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	objList := make([]string, 0, numPuts)
	for i := 0; i < numPuts; i++ {
		fname := FastRandomFilename(random, fnlen)
		objList = append(objList, fname)
	}
	PutObjsFromList(proxyURL, bucket, readerPath, readerType, objPath, objSize, objList, errCh, objsPutCh, sgl)
}

func StartDSort(proxyURL string, rs dsort.RequestSpec) (string, error) {
	msg, err := json.Marshal(rs)
	if err != nil {
		return "", err
	}

	baseParams := BaseAPIParams(proxyURL)
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Start)
	body, err := api.DoHTTPRequest(baseParams, path, msg)
	if err != nil {
		return "", err
	}

	return string(body), err
}

func AbortDSort(proxyURL, managerUUID string) error {
	baseParams := BaseAPIParams(proxyURL)
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Abort, managerUUID)
	_, err := api.DoHTTPRequest(baseParams, path, nil)
	return err
}

func WaitForDSortToFinish(proxyURL, managerUUID string) (bool, error) {
	for {
		allMetrics, err := MetricsDSort(proxyURL, managerUUID)
		if err != nil {
			return false, err
		}

		allFinished := true
		for _, metrics := range allMetrics {
			if metrics.Aborted {
				return true, nil
			}

			allFinished = allFinished && metrics.Extraction.Finished && metrics.Sorting.Finished && metrics.Creation.Finished
		}

		if allFinished {
			break
		}

		time.Sleep(time.Millisecond * 500)
	}

	return false, nil
}

func MetricsDSort(proxyURL, managerUUID string) (map[string]*dsort.Metrics, error) {
	baseParams := BaseAPIParams(proxyURL)
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Metrics, managerUUID)
	body, err := api.DoHTTPRequest(baseParams, path, nil)
	if err != nil {
		return nil, err
	}

	var metrics map[string]*dsort.Metrics
	err = jsoniter.Unmarshal(body, &metrics)
	return metrics, err
}

func DefaultBaseAPIParams(t *testing.T) *api.BaseParams {
	primaryURL, err := GetPrimaryProxy(readProxyURL)
	CheckFatal(err, t)
	return BaseAPIParams(primaryURL)
}

func BaseAPIParams(url string) *api.BaseParams {
	return &api.BaseParams{
		Client: HTTPClient,
		URL:    url,
	}
}

// ParseEnvVariables takes in a .env file and parses its contents
func ParseEnvVariables(fpath string, delimiter ...string) map[string]string {
	m := map[string]string{}
	dlim := "="
	data, err := ioutil.ReadFile(fpath)
	if err != nil {
		Logf("Could not read file: %v\n", err)
		return nil
	}

	if len(delimiter) > 0 {
		dlim = delimiter[0]
	}

	paramList := strings.Split(string(data), "\n")
	for _, dat := range paramList {
		datum := strings.Split(dat, dlim)
		//key=val
		if len(datum) == 2 {
			m[datum[0]] = datum[1]
		}
	}
	return m
}

// waitForLocalBucket wait until all targets have local bucket created or deleted
func WaitForLocalBucket(proxyURL, name string, exists bool) error {
	baseParams := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return err
	}
	to := time.Now().Add(bucketTimeout)
	for _, s := range smap.Tmap {
	loop_bucket:
		for {
			pubURL := s.URL(cmn.NetworkPublic)
			bucketExists, err := DoesLocalBucketExist(pubURL, name)
			if err != nil {
				return err
			}
			if bucketExists == exists {
				break loop_bucket
			}
			if time.Now().After(to) {
				return fmt.Errorf("wait for local bucket timed out, target = %s", pubURL)
			}
			time.Sleep(time.Second)
		}
	}
	return nil
}
