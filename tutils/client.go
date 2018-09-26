/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package tutils provides common low-level utilities for all dfcpub unit and integration tests
//
//  FIXME -- FIXME: split and transform it into the: a) client API and b) test utilities
//  FIXME -- FIXME: the client API must then move into the api package
//
package tutils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
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
		length int64
		hash   string
	)

	// Note: This code can use some cleanup.
	if err == nil {
		if r.StatusCode >= http.StatusBadRequest {
			bytes, err := ioutil.ReadAll(r.Body)
			if err == nil {
				return 0, "", fmt.Errorf("Bad status %d from %s, response: %s", r.StatusCode, src, string(bytes))
			} else {
				return 0, "", fmt.Errorf("Bad status %d from %s, err: %v", r.StatusCode, src, err)
			}
		}

		buf, slab := Mem2.AllocFromSlab2(cmn.DefaultBufSize)
		defer slab.Free(buf)
		if validate {
			length, hash, err = cmn.ReadWriteWithHash(r.Body, w, buf)
			if err != nil {
				return 0, "", fmt.Errorf("Failed to read HTTP response, err: %v", err)
			}
		} else {
			if length, err = io.CopyBuffer(w, r.Body, buf); err != nil {
				return 0, "", fmt.Errorf("Failed to read HTTP response, err: %v", err)
			}
		}
	} else {
		return 0, "", fmt.Errorf("%s failed, err: %v", src, err)
	}

	return length, hash, nil
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
func GetWithMetrics(url, bucket string, keyname string, silent bool, validate bool) (int64, HTTPLatencies, error) {
	var (
		hash, hdhash, hdhashtype string
		w                        = ioutil.Discard
	)

	url += cmn.URLPath(cmn.Version, cmn.Objects, bucket, keyname)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, HTTPLatencies{}, err
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := tracedClient.Do(req)
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	tr.tsHTTPEnd = time.Now()

	if validate && resp != nil {
		hdhash = resp.Header.Get(cmn.HeaderDFCChecksumVal)
		hdhashtype = resp.Header.Get(cmn.HeaderDFCChecksumType)
	}

	v := hdhashtype == cmn.ChecksumXXHash
	len, hash, err := readResponse(resp, w, err, fmt.Sprintf("GET (object %s from bucket %s)", keyname, bucket), v)
	if err != nil {
		return 0, HTTPLatencies{}, err
	}
	if v {
		if hdhash != hash {
			err = cmn.NewInvalidCksumError(hdhash, hash)
		} else {
			if !silent {
				fmt.Printf("Header's hash %s matches the file's %s \n", hdhash, hash)
			}
		}
	}

	l := HTTPLatencies{
		ProxyConn:           tr.tsProxyConn.Sub(tr.tsBegin),
		Proxy:               tr.tsRedirect.Sub(tr.tsProxyConn),
		TargetConn:          tr.tsTargetConn.Sub(tr.tsRedirect),
		Target:              tr.tsHTTPEnd.Sub(tr.tsTargetConn),
		PostHTTP:            time.Now().Sub(tr.tsHTTPEnd),
		ProxyWroteHeader:    tr.tsProxyWroteHeaders.Sub(tr.tsProxyConn),
		ProxyWroteRequest:   tr.tsProxyWroteRequest.Sub(tr.tsProxyWroteHeaders),
		ProxyFirstResponse:  tr.tsProxyFirstResponse.Sub(tr.tsProxyWroteRequest),
		TargetWroteHeader:   tr.tsTargetWroteHeaders.Sub(tr.tsTargetConn),
		TargetWroteRequest:  tr.tsTargetWroteRequest.Sub(tr.tsTargetWroteHeaders),
		TargetFirstResponse: tr.tsTargetFirstResponse.Sub(tr.tsTargetWroteRequest),
	}
	return len, l, err
}

func Del(proxyURL, bucket string, object string, wg *sync.WaitGroup, errCh chan error, silent bool) error {
	if wg != nil {
		defer wg.Done()
	}
	if !silent {
		fmt.Printf("DEL: %s\n", object)
	}
	err := api.DeleteObject(HTTPClient, proxyURL, bucket, object)
	emitError(nil, err, errCh)
	return err
}

// ListBucket returns list of objects in a bucket. objectCountLimit is the
// maximum number of objects returned by ListBucket (0 - return all objects in a bucket)
func ListBucket(proxyURL, bucket string, msg *cmn.GetMsg, objectCountLimit int) (*cmn.BucketList, error) {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	reslist := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, 1000)}

	// An optimization to read as few objects from bucket as possible.
	// toRead is the current number of objects ListBucket must read before
	// returning the list. Every cycle the loop reads objects by pages and
	// decreases toRead by the number of received objects. When toRead gets less
	// than pageSize, the loop does the final request with reduced pageSize
	toRead := objectCountLimit
	for {
		var resp *http.Response

		if toRead != 0 {
			if (msg.GetPageSize == 0 && toRead < cmn.DefaultPageSize) ||
				(msg.GetPageSize != 0 && msg.GetPageSize > toRead) {
				msg.GetPageSize = toRead
			}
		}

		injson, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}
		if len(injson) == 0 {
			resp, err = HTTPClient.Get(url)
		} else {
			injson, err = json.Marshal(cmn.ActionMsg{Action: cmn.ActListObjects, Value: msg})
			if err != nil {
				return nil, err
			}
			resp, err = HTTPClient.Post(url, "application/json", bytes.NewBuffer(injson))
		}

		if err != nil {
			return nil, err
		}

		defer func() {
			if resp != nil {
				resp.Body.Close()
			}
		}()

		page := &cmn.BucketList{}
		page.Entries = make([]*cmn.BucketEntry, 0, 1000)
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("Failed to read http response body, err: %v", err)
		}

		if resp.StatusCode >= http.StatusBadRequest {
			return nil, fmt.Errorf("HTTP error %d, message = %v", resp.StatusCode, string(b))
		}

		err = json.Unmarshal(b, page)
		if err != nil {
			return nil, fmt.Errorf("Failed to json-unmarshal, err: %v [%s]", err, string(b))
		}

		reslist.Entries = append(reslist.Entries, page.Entries...)
		if page.PageMarker == "" {
			break
		}

		if objectCountLimit != 0 {
			if len(reslist.Entries) >= objectCountLimit {
				break
			}
			toRead -= len(page.Entries)
		}

		msg.GetPageMarker = page.PageMarker
	}

	return reslist, nil
}

func Evict(proxyURL, bucket string, fname string) error {
	var (
		injson []byte
		err    error
	)
	EvictMsg := cmn.ActionMsg{Action: cmn.ActEvict}
	EvictMsg.Name = bucket + "/" + fname
	injson, err = json.Marshal(EvictMsg)
	if err != nil {
		return fmt.Errorf("Failed to marshal EvictMsg, err: %v", err)
	}

	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, fname)
	return HTTPRequest(http.MethodDelete, url, NewBytesReader(injson))
}

func doListRangeCall(proxyURL, bucket, action, method string, listrangemsg interface{}) error {
	var (
		injson []byte
		err    error
	)
	actionMsg := cmn.ActionMsg{Action: action, Value: listrangemsg}
	injson, err = json.Marshal(actionMsg)
	if err != nil {
		return fmt.Errorf("Failed to marhsal cmn.ActionMsg, err: %v", err)
	}
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	headers := map[string]string{
		"Content-Type": "application/json",
	}

	return HTTPRequest(method, url, NewBytesReader(injson), headers)
}

func PrefetchList(proxyURL, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := cmn.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeCall(proxyURL, bucket, cmn.ActPrefetch, http.MethodPost, prefetchMsg)
}

func PrefetchRange(proxyURL, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	prefetchMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := cmn.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: prefetchMsgBase}
	return doListRangeCall(proxyURL, bucket, cmn.ActPrefetch, http.MethodPost, prefetchMsg)
}

func DeleteList(proxyURL, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := cmn.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeCall(proxyURL, bucket, cmn.ActDelete, http.MethodDelete, deleteMsg)
}

func DeleteRange(proxyURL, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := cmn.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeCall(proxyURL, bucket, cmn.ActDelete, http.MethodDelete, deleteMsg)
}

func EvictList(proxyURL, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := cmn.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeCall(proxyURL, bucket, cmn.ActEvict, http.MethodDelete, evictMsg)
}

func EvictRange(proxyURL, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := cmn.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeCall(proxyURL, bucket, cmn.ActEvict, http.MethodDelete, evictMsg)
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
			err = fmt.Errorf("Failed to read response body, err: %v", ioErr)
			return false, err
		}
		err = fmt.Errorf("IsCached failed: bucket/object: %s/%s, HTTP status: %d, HTTP response: %s",
			bucket, objname, r.StatusCode, string(b))
		return false, err
	}
	return true, nil
}

// Put sends a PUT request to the given URL
func Put(proxyURL string, reader Reader, bucket string, key string, silent bool) error {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, key)
	if !silent {
		fmt.Printf("PUT: %s/%s\n", bucket, key)
	}

	handle, err := reader.Open() // FIXME: wrong semantics for in-mem readers
	if err != nil {
		return fmt.Errorf("Failed to open reader, err: %v", err)
	}
	defer handle.Close()

	req, err := http.NewRequest(http.MethodPut, url, handle)
	if err != nil {
		return fmt.Errorf("Failed to create new http request, err: %v", err)
	}

	// The HTTP package doesn't automatically set this for files, so it has to be done manually
	// If it wasn't set, we would need to deal with the redirect manually.
	req.GetBody = func() (io.ReadCloser, error) {
		return reader.Open()
	}

	if reader.XXHash() != "" {
		req.Header.Set(cmn.HeaderDFCChecksumType, cmn.ChecksumXXHash)
		req.Header.Set(cmn.HeaderDFCChecksumVal, reader.XXHash())
	}

	resp, err := HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("Failed to PUT, err: %v", err)
	}
	defer func() {
		resp.Body.Close()
	}()

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Failed to read HTTP response, err: %v", err)
	}
	return nil
}

// PutAsync sends a PUT request to the given URL
func PutAsync(wg *sync.WaitGroup, proxyURL string, reader Reader, bucket string, key string,
	errCh chan error, silent bool) {
	defer wg.Done()
	err := Put(proxyURL, reader, bucket, key, silent)
	if err != nil {
		if errCh == nil {
			fmt.Println("Error channel is not given, do not know how to report error", err)
		} else {
			errCh <- err
		}
	}
}

// ReplicateObject replicates given objectName in bucketName using
// targetrunner's replicate endpoint.
func ReplicateObject(proxyURL, bucketName, objectName string) error {
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActReplicate})
	if err != nil {
		return err
	}

	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucketName, objectName)
	return HTTPRequest(http.MethodPost, url, NewBytesReader(msg))
}

// ReplicateMultipleObjects replicates all the objects in the map bucketToObjects.
// bucketsToObjects is a key value pairing where the keys are bucket names and the
// corresponding value is a slice of objects.
// ReplicateMultipleObjects returns a map of errors where the key is bucket+"/"+object and the
// corresponding value is the error that caused replication to fail.
func ReplicateMultipleObjects(proxyURL string, bucketToObjects map[string][]string) map[string]error {
	objectsWithErrors := make(map[string]error)
	for bucket, objectList := range bucketToObjects {
		for _, object := range objectList {
			if err := ReplicateObject(proxyURL, bucket, object); err != nil {
				objectsWithErrors[filepath.Join(bucket, object)] = err
			}
		}
	}
	return objectsWithErrors
}

// ListObjects returns a slice of object names of all objects that match the prefix in a bucket
func ListObjects(proxyURL, bucket, prefix string, objectCountLimit int) ([]string, error) {
	msg := &cmn.GetMsg{GetPrefix: prefix}
	data, err := ListBucket(proxyURL, bucket, msg, objectCountLimit)
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
	url := server + cmn.URLPath(cmn.Version, cmn.Daemon)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.URL.RawQuery = GetWhatRawQuery(cmn.GetWhatConfig, "")
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := tracedClient.Do(req)
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	_, err = discardResponse(resp, err, fmt.Sprintf("Get config"))
	emitError(resp, err, nil)
	l := HTTPLatencies{
		ProxyConn: tr.tsProxyConn.Sub(tr.tsBegin),
		Proxy:     time.Now().Sub(tr.tsProxyConn),
	}
	return l, err
}

// GetClusterMap retrives a DFC's server map
// Note: this may not be a good idea to expose the map to clients, but this how it is for now.
func GetClusterMap(url string) (cluster.Smap, error) {
	q := GetWhatRawQuery(cmn.GetWhatSmap, "")
	requestURL := fmt.Sprintf("%s?%s", url+cmn.URLPath(cmn.Version, cmn.Daemon), q)
	r, err := HTTPClient.Get(requestURL)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()

	if err != nil {
		// Note: might return connection refused if the servet is not ready
		//       caller can retry in that case
		return cluster.Smap{}, err
	}

	if r != nil && r.StatusCode >= http.StatusBadRequest {
		return cluster.Smap{}, fmt.Errorf("get Smap, HTTP status %d", r.StatusCode)
	}

	var (
		b    []byte
		smap cluster.Smap
	)
	b, err = ioutil.ReadAll(r.Body)
	if err != nil {
		return cluster.Smap{}, fmt.Errorf("Failed to read response, err: %v", err)
	}

	err = json.Unmarshal(b, &smap)
	if err != nil {
		return cluster.Smap{}, fmt.Errorf("Failed to unmarshal Smap, err: %v", err)
	}

	return smap, nil
}

// GetPrimaryProxy returns the primary proxy's url of a cluster
func GetPrimaryProxy(url string) (string, error) {
	smap, err := GetClusterMap(url)
	if err != nil {
		return "", err
	}

	return smap.ProxySI.PublicNet.DirectURL, nil
}

// HTTPRequest sends one HTTP request and checks result
func HTTPRequest(method string, url string, msg Reader, headers ...map[string]string) error {
	_, err := HTTPRequestWithResp(method, url, msg, headers...)
	return err
}

// HTTPRequestWithResp sends one HTTP request, checks result and returns body of
// response.
func HTTPRequestWithResp(method string, url string, msg Reader, headers ...map[string]string) ([]byte, error) {
	var (
		req *http.Request
		err error
	)
	if msg == nil {
		req, err = http.NewRequest(method, url, msg)
	} else {
		handle, err := msg.Open()
		if err != nil {
			return nil, fmt.Errorf("Failed to open msg, err: %v", err)
		}
		defer handle.Close()

		req, err = http.NewRequest(method, url, handle)
	}

	if err != nil {
		return nil, fmt.Errorf("Failed to create request, err: %v", err)
	}

	if msg != nil {
		req.GetBody = func() (io.ReadCloser, error) {
			return msg.Open()
		}
	}

	if len(headers) != 0 {
		for key, value := range headers[0] {
			req.Header.Set(key, value)
		}
	}
	resp, err := HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Failed to %s, err: %v", method, err)
	}

	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("Failed to read response, err: %v", err)
		}

		return nil, fmt.Errorf("HTTP error = %d, message = %s", resp.StatusCode, string(b))
	}
	return ioutil.ReadAll(resp.Body)
}

// DoesLocalBucketExist queries a proxy or target to get a list of all local buckets, returns true if
// the bucket exists.
func DoesLocalBucketExist(serverURL string, bucket string) (bool, error) {
	buckets, err := api.GetBucketNames(HTTPClient, serverURL, true)
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

func GetWhatRawQuery(getWhat string, getProps string) string {
	q := url.Values{}
	q.Add(cmn.URLParamWhat, getWhat)
	if getProps != "" {
		q.Add(cmn.URLParamProps, getProps)
	}
	return q.Encode()
}

func TargetMountpaths(targetUrl string) (*cmn.MountpathList, error) {
	q := GetWhatRawQuery(cmn.GetWhatMountpaths, "")
	url := fmt.Sprintf("%s?%s", targetUrl+cmn.URLPath(cmn.Version, cmn.Daemon), q)

	resp, err := HTTPClient.Get(url)
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("Target mountpath list, HTTP status = %d", resp.StatusCode)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read response, err: %v", err)
	}

	mp := &cmn.MountpathList{}
	err = json.Unmarshal(b, mp)
	return mp, err
}

func EnableTargetMountpath(daemonUrl, mpath string) error {
	url := daemonUrl + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathEnable, Value: mpath})
	if err != nil {
		return err
	}
	return HTTPRequest(http.MethodPost, url, NewBytesReader(msg))
}

func DisableTargetMountpath(daemonUrl, mpath string) error {
	url := daemonUrl + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathDisable, Value: mpath})
	if err != nil {
		return err
	}
	return HTTPRequest(http.MethodPost, url, NewBytesReader(msg))
}

func AddTargetMountpath(daemonUrl, mpath string) error {
	url := daemonUrl + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathAdd, Value: mpath})
	if err != nil {
		return err
	}
	return HTTPRequest(http.MethodPut, url, NewBytesReader(msg))
}

func RemoveTargetMountpath(daemonUrl, mpath string) error {
	url := daemonUrl + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathRemove, Value: mpath})
	if err != nil {
		return err
	}
	return HTTPRequest(http.MethodDelete, url, NewBytesReader(msg))
}

func UnregisterTarget(proxyURL, sid string) error {
	smap, err := GetClusterMap(proxyURL)
	if err != nil {
		return fmt.Errorf("GetClusterMap() failed, err: %v", err)
	}

	target, ok := smap.Tmap[sid]
	var idsToIgnore []string
	if ok {
		idsToIgnore = []string{target.DaemonID}
	}

	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, sid)
	if err = HTTPRequest(http.MethodDelete, url, nil); err != nil {
		return err
	}

	// If target does not exists in cluster we should not wait for map version
	// sync because update will not be scheduled
	if ok {
		return WaitMapVersionSync(time.Now().Add(registerTimeout), smap, smap.Version, idsToIgnore)
	}

	return nil
}

func RegisterTarget(sid, targetDirectURL string, smap cluster.Smap) error {
	_, ok := smap.Tmap[sid]
	url := targetDirectURL + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Register)
	if err := HTTPRequest(http.MethodPost, url, nil); err != nil {
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

		daemonSmap, err := GetClusterMap(url)
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
			fmt.Errorf("Get xaction, HTTP Status %d", r.StatusCode)
	}

	var response []byte
	response, err = ioutil.ReadAll(r.Body)
	if err != nil {
		return []byte{}, fmt.Errorf("Failed to read response, err: %v", err)
	}

	return response, nil
}
