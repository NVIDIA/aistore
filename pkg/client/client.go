// Package client provides common operations for files in cloud storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"strconv"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/OneOfOne/xxhash"
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
		ProxyConn           time.Duration // from request is created to proxy connection is estabilished
		Proxy               time.Duration // from proxy connection is estabilished to redirected
		TargetConn          time.Duration // from request is redirected to target connection is estabilished
		Target              time.Duration // from target connection is estabilished to request is completed
		PostHTTP            time.Duration // from http ends to after read data from http response and verify hash (if specified)
		ProxyWroteHeader    time.Duration // from ProxyConn to header is written
		ProxyWroteRequest   time.Duration // from ProxyWroteHeader to response body is written
		ProxyFirstResponse  time.Duration // from ProxyWroteRequest to first byte of response
		TargetWroteHeader   time.Duration // from TargetConn to header is written
		TargetWroteRequest  time.Duration // from TargetWroteHeader to response body is written
		TargetFirstResponse time.Duration // from TargetWroteRequest to first byte of response
	}
)

var (
	transport = &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 60 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 600 * time.Second,
		MaxIdleConnsPerHost: 100, // arbitrary number, to avoid connect: cannot assign requested address
	}
	client = &http.Client{
		Timeout:   600 * time.Second,
		Transport: transport,
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

type BucketProps struct {
	CloudProvider string
	Versioning    string
}

type ObjectProps struct {
	Size    int
	Version string
}

// Reader is the interface a client works with to read in data and send to a HTTP server
type Reader interface {
	io.ReadCloser
	io.Seeker
	Open() (io.ReadCloser, error)
	XXHash() string
	Description() string
}

type bytesReaderCloser struct {
	bytes.Reader
}

func (q *bytesReaderCloser) Close() error {
	return nil
}

func (err ReqError) Error() string {
	return err.message
}

func newReqError(msg string, code int) ReqError {
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
		len  int64
		hash string
	)

	// Note: This code can use some cleanup.
	if err == nil {
		if r.StatusCode >= http.StatusBadRequest {
			return 0, "", fmt.Errorf("Bad status code from %s: http status %d", src, r.StatusCode)
		}

		bufreader := bufio.NewReader(r.Body)
		if validate {
			len, hash, err = ReadWriteWithHash(bufreader, w)
			if err != nil {
				return 0, "", fmt.Errorf("Failed to read http response, err: %v", err)
			}
		} else {
			if len, err = io.Copy(w, bufreader); err != nil {
				return 0, "", fmt.Errorf("Failed to read http response, err: %v", err)
			}
		}
	} else {
		return 0, "", fmt.Errorf("%s failed, err: %v", src, err)
	}

	return len, hash, nil
}

func discardResponse(r *http.Response, err error, src string) (int64, error) {
	len, _, err := readResponse(r, ioutil.Discard, err, src, false /* validate */)
	return len, err
}

func emitError(r *http.Response, err error, errch chan error) {
	if err == nil || errch == nil {
		return
	}

	if r != nil {
		errObj := newReqError(err.Error(), r.StatusCode)
		errch <- errObj
	} else {
		errch <- err
	}
}

func get(proxyurl, bucket string, keyname string, wg *sync.WaitGroup, errch chan error,
	silent bool, validate bool, w io.Writer) (int64, HTTPLatencies, error) {
	var (
		hash, hdhash, hdhashtype string
	)

	if wg != nil {
		defer wg.Done()
	}

	url := proxyurl + "/" + dfc.Rversion + "/" + dfc.Robjects + "/" + bucket + "/" + keyname
	req, _ := http.NewRequest("GET", url, nil)

	tr := &traceableTransport{
		transport: transport,
		tsBegin:   time.Now(),
	}
	trace := &httptrace.ClientTrace{
		GotConn:              tr.GotConn,
		WroteHeaders:         tr.WroteHeaders,
		WroteRequest:         tr.WroteRequest,
		GotFirstResponseByte: tr.GotFirstResponseByte,
	}

	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	tr.tsHTTPEnd = time.Now()

	if validate {
		hdhash = resp.Header.Get(dfc.HeaderDfcChecksumVal)
		hdhashtype = resp.Header.Get(dfc.HeaderDfcChecksumType)
	}

	v := hdhashtype == dfc.ChecksumXXHash
	len, hash, err := readResponse(resp, w, err, fmt.Sprintf("GET (object %s from bucket %s)", keyname, bucket), v)
	if err != nil && v {
		if hdhash != hash {
			s := fmt.Sprintf("Header's hash %s doesn't match the file's %s \n", hdhash, hash)
			if errch != nil {
				errch <- errors.New(s)
			}
		} else {
			if !silent {
				fmt.Printf("Header's hash %s matches the file's %s \n", hdhash, hash)
			}
		}
	}

	emitError(resp, err, errch)
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

// Get sends a get request to proxy and discard the data returned
func Get(proxyurl, bucket string, keyname string, wg *sync.WaitGroup, errch chan error,
	silent bool, validate bool) (int64, HTTPLatencies, error) {
	return get(proxyurl, bucket, keyname, wg, errch, silent, validate, ioutil.Discard)
}

// GetFile sends a get request to proxy and save the data returned to an io.Writer
func GetFile(proxyurl, bucket string, keyname string, wg *sync.WaitGroup, errch chan error,
	silent bool, validate bool, w io.Writer) (int64, HTTPLatencies, error) {
	return get(proxyurl, bucket, keyname, wg, errch, silent, validate, w)
}

func Del(proxyurl, bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool) (err error) {
	if wg != nil {
		defer wg.Done()
	}

	delurl := proxyurl + "/" + dfc.Rversion + "/" + dfc.Robjects + "/" + bucket + "/" + keyname
	if !silent {
		fmt.Printf("DEL: %s\n", keyname)
	}
	req, httperr := http.NewRequest(http.MethodDelete, delurl, nil)
	if httperr != nil {
		err = fmt.Errorf("Failed to create new http request, err: %v", httperr)
		emitError(nil, err, errch)
		return err
	}

	r, httperr := client.Do(req)
	if httperr != nil {
		err = fmt.Errorf("Failed to delete file, err: %v", httperr)
		emitError(nil, err, errch)
		return err
	}

	defer func() {
		r.Body.Close()
	}()

	_, err = discardResponse(r, err, "DELETE")
	emitError(r, err, errch)
	return err
}

// ListBucket returns list of objects in a bucket. objectCountLimit is the
// maximum number of objects returned by ListBucket (0 - return all objects in a bucket)
func ListBucket(proxyurl, bucket string, msg *dfc.GetMsg, objectCountLimit int) (*dfc.BucketList, error) {
	var (
		url     = proxyurl + "/" + dfc.Rversion + "/" + dfc.Rbuckets + "/" + bucket
		request *http.Request
	)

	reslist := &dfc.BucketList{Entries: make([]*dfc.BucketEntry, 0, 1000)}
	// An optimization to read as few objects from bucket as possible.
	// toRead is the current number of objects ListBucket must read before
	// returning the list. Every cycle the loop reads objects by pages and
	// decreases toRead by the number of received objects. When toRead gets less
	// than pageSize, the loop does the final request with reduced pageSize
	toRead := objectCountLimit
	for {
		var resp *http.Response

		if toRead != 0 {
			if (msg.GetPageSize == 0 && toRead < dfc.DefaultPageSize) ||
				(msg.GetPageSize != 0 && msg.GetPageSize > toRead) {
				msg.GetPageSize = toRead
			}
		}

		injson, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}
		if len(injson) == 0 {
			resp, err = client.Get(url)
		} else {
			request, err = http.NewRequest("GET", url, bytes.NewBuffer(injson))
			if err == nil {
				request.Header.Set("Content-Type", "application/json")
				resp, err = client.Do(request)
			}
		}

		if err != nil {
			return nil, err
		}

		defer func() {
			if resp != nil {
				resp.Body.Close()
			}
		}()

		page := &dfc.BucketList{}
		page.Entries = make([]*dfc.BucketEntry, 0, 1000)
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("Failed to read http response body, err = %v", err)
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

func Evict(proxyurl, bucket string, fname string) error {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	EvictMsg := dfc.ActionMsg{Action: dfc.ActEvict}
	EvictMsg.Name = bucket + "/" + fname
	injson, err = json.Marshal(EvictMsg)
	if err != nil {
		return fmt.Errorf("Failed to marshal EvictMsg: %v", err)
	}

	req, err = http.NewRequest("DELETE", proxyurl+"/"+dfc.Rversion+"/"+dfc.Robjects+"/"+bucket+"/"+fname, bytes.NewBuffer(injson))
	if err != nil {
		return fmt.Errorf("Failed to create request: %v", err)
	}

	r, err = client.Do(req)
	if r != nil {
		r.Body.Close()
	}

	if err != nil {
		return err
	}

	return nil
}

func doListRangeCall(proxyurl, bucket, action, method string, listrangemsg interface{}, wait bool) error {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	actionMsg := dfc.ActionMsg{Action: action, Value: listrangemsg}
	injson, err = json.Marshal(actionMsg)
	if err != nil {
		return fmt.Errorf("Failed to marhsal ActionMsg: %v", err)
	}
	req, err = http.NewRequest(method, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rbuckets+"/"+bucket+"/", bytes.NewBuffer(injson))
	if err != nil {
		return fmt.Errorf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if wait {
		r, err = client.Do(req)
	} else {
		r, err = client.Do(req)
	}
	if r != nil {
		r.Body.Close()
	}
	return err
}

func PrefetchList(proxyurl, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	rangeListMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := dfc.ListMsg{Objnames: fileslist, RangeListMsgBase: rangeListMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActPrefetch, http.MethodPost, prefetchMsg, wait)
}

func PrefetchRange(proxyurl, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	prefetchMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := dfc.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, RangeListMsgBase: prefetchMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActPrefetch, http.MethodPost, prefetchMsg, wait)
}

func DeleteList(proxyurl, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	rangeListMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := dfc.ListMsg{Objnames: fileslist, RangeListMsgBase: rangeListMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActDelete, http.MethodDelete, deleteMsg, wait)
}

func DeleteRange(proxyurl, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	rangeListMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := dfc.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, RangeListMsgBase: rangeListMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActDelete, http.MethodDelete, deleteMsg, wait)
}

func EvictList(proxyurl, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	rangeListMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := dfc.ListMsg{Objnames: fileslist, RangeListMsgBase: rangeListMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActEvict, http.MethodDelete, evictMsg, wait)
}

func EvictRange(proxyurl, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	rangeListMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := dfc.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, RangeListMsgBase: rangeListMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActEvict, http.MethodDelete, evictMsg, wait)
}

// fastRandomFilename is taken from https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func FastRandomFilename(src *rand.Rand, fnlen int) string {
	b := make([]byte, fnlen)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := fnlen-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

func HeadBucket(proxyurl, bucket string) (bucketprops *BucketProps, err error) {
	var (
		url = proxyurl + "/" + dfc.Rversion + "/" + dfc.Rbuckets + "/" + bucket
		r   *http.Response
	)
	bucketprops = &BucketProps{}
	r, err = client.Head(url)
	if err != nil {
		return
	}
	defer func() {
		r.Body.Close()
	}()
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		b, ioErr := ioutil.ReadAll(r.Body)
		if ioErr != nil {
			err = fmt.Errorf("failed to read response body, err = %s", ioErr)
			return
		}
		err = fmt.Errorf("head bucket: %s failed, HTTP status code: %d, HTTP response body: %s",
			bucket, r.StatusCode, string(b))
		return
	}
	bucketprops.CloudProvider = r.Header.Get(dfc.CloudProvider)
	bucketprops.Versioning = r.Header.Get(dfc.Versioning)
	return
}

func HeadObject(proxyurl, bucket, objname string) (objProps *ObjectProps, err error) {
	var (
		url = proxyurl + "/" + dfc.Rversion + "/" + dfc.Robjects + "/" + bucket + "/" + objname
		r   *http.Response
	)
	objProps = &ObjectProps{}
	r, err = client.Head(url)
	if err != nil {
		return
	}
	defer func() {
		r.Body.Close()
	}()
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		b, ioErr := ioutil.ReadAll(r.Body)
		if ioErr != nil {
			err = fmt.Errorf("failed to read response body, err = %s", ioErr)
			return
		}
		err = fmt.Errorf("head bucket/object: %s/%s failed, HTTP status code: %d, HTTP response body: %s",
			bucket, objname, r.StatusCode, string(b))
		return
	}
	size, err := strconv.Atoi(r.Header.Get(dfc.Size))
	if err != nil {
		return
	}

	objProps.Size = size
	objProps.Version = r.Header.Get(dfc.Version)
	return
}

func checkHTTPStatus(resp *http.Response, op string) error {
	if resp.StatusCode >= http.StatusBadRequest {
		return ReqError{
			code:    resp.StatusCode,
			message: fmt.Sprintf("Bad status code from %s", op),
		}
	}

	return nil
}

func discardHTTPResp(resp *http.Response) {
	bufreader := bufio.NewReader(resp.Body)
	dfc.ReadToNull(bufreader)
}

// Put sends a PUT request to the given URL
func Put(proxyURL string, reader Reader, bucket string, key string, silent bool) error {
	url := proxyURL + "/" + dfc.Rversion + "/" + dfc.Robjects + "/" + bucket + "/" + key

	if !silent {
		fmt.Printf("PUT: %s/%s\n", bucket, key)
	}

	handle, err := reader.Open()
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
		req.Header.Set(dfc.HeaderDfcChecksumType, dfc.ChecksumXXHash)
		req.Header.Set(dfc.HeaderDfcChecksumVal, reader.XXHash())
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Failed to send put request, err = %v", err)
	}

	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("Failed to read response body, err = %v", err)
		}

		return fmt.Errorf("HTTP error = %d, message = %s", resp.StatusCode, string(b))
	}

	return nil
}

// PutAsync sends a PUT request to the given URL
func PutAsync(wg *sync.WaitGroup, proxyURL string, reader Reader, bucket string, key string,
	errch chan error, silent bool) {
	defer wg.Done()
	err := Put(proxyURL, reader, bucket, key, silent)
	if err != nil {
		if errch == nil {
			fmt.Println("Error channel is not given, do know how to report error", err)
		} else {
			errch <- err
		}
	}
}

// CreateLocalBucket sends a HTTP request to a proxy and asks it to create a local bucket
func CreateLocalBucket(proxyURL, bucket string) error {
	msg, err := json.Marshal(dfc.ActionMsg{Action: dfc.ActCreateLB})
	if err != nil {
		return err
	}

	err = HTTPRequest("POST", proxyURL+"/"+dfc.Rversion+"/"+dfc.Rbuckets+"/"+bucket, bytes.NewBuffer(msg))
	if err != nil {
		return err
	}

	return waitForLocalBucket(proxyURL, bucket)
}

// waitForLocalBucket wait until all targets have local bucket created or timeout
func waitForLocalBucket(url, name string) error {
	smap, err := GetClusterMap(url)
	if err != nil {
		return err
	}

	to := time.Now().Add(time.Minute)
	for _, s := range smap.Tmap {
	loop_bucket:
		for {
			exists, err := DoesLocalBucketExist(s.DirectURL, name)
			if err != nil {
				return err
			}

			if exists {
				break loop_bucket
			}

			if time.Now().After(to) {
				return fmt.Errorf("wait for local bucket timed out, target = %s", s.DirectURL)
			}

			time.Sleep(time.Second)
		}
	}

	return nil
}

// waitForNoLocalBucket wait until all targets do not have local bucket anymore or timeout
func waitForNoLocalBucket(url, name string) error {
	smap, err := GetClusterMap(url)
	if err != nil {
		return err
	}

	to := time.Now().Add(time.Minute)
	for _, s := range smap.Tmap {
	loop_bucket:
		for {
			exists, err := DoesLocalBucketExist(s.DirectURL, name)
			if err != nil {
				return err
			}

			if !exists {
				break loop_bucket
			}

			if time.Now().After(to) {
				return fmt.Errorf("wait for no local bucket timed out, target = %s", s.DirectURL)
			}

			time.Sleep(time.Second)
		}
	}

	return nil
}

// RenameLocalBucket changes the name of a bucket to newBucketName
func RenameLocalBucket(proxyURL, bucket, newBucketName string) error {
	msg, err := json.Marshal(dfc.ActionMsg{Action: dfc.ActRenameLB, Name: newBucketName})
	if err != nil {
		return err
	}

	err = HTTPRequest("POST", proxyURL+"/"+dfc.Rversion+"/"+dfc.Rbuckets+"/"+bucket, bytes.NewBuffer(msg))
	if err != nil {
		return err
	}

	err = waitForNoLocalBucket(proxyURL, bucket)
	if err != nil {
		return err
	}

	return waitForLocalBucket(proxyURL, newBucketName)
}

// DestroyLocalBucket deletes a local bucket
func DestroyLocalBucket(proxyURL, bucket string) error {
	msg, err := json.Marshal(dfc.ActionMsg{Action: dfc.ActDestroyLB})
	if err != nil {
		return err
	}

	err = HTTPRequest("DELETE", proxyURL+"/"+dfc.Rversion+"/"+dfc.Rbuckets+"/"+bucket, bytes.NewBuffer(msg))
	if err != nil {
		return err
	}

	return waitForNoLocalBucket(proxyURL, bucket)
}

// ListObjects returns a slice of object names of all objects that match the prefix in a bucket
func ListObjects(proxyURL, bucket, prefix string, objectCountLimit int) ([]string, error) {
	msg := &dfc.GetMsg{GetPrefix: prefix}
	data, err := ListBucket(proxyURL, bucket, msg, objectCountLimit)
	if err != nil {
		return nil, err
	}

	var objs []string
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
	url := server + "/" + dfc.Rversion + "/" + dfc.Rdaemon
	msg, err := json.Marshal(&dfc.GetMsg{GetWhat: dfc.GetWhatConfig})
	if err != nil {
		return HTTPLatencies{}, err
	}

	req, _ := http.NewRequest("GET", url, bytes.NewBuffer(msg))
	tr := &traceableTransport{
		transport: transport,
		tsBegin:   time.Now(),
	}
	trace := &httptrace.ClientTrace{
		GotConn: tr.GotConn,
	}

	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	client := &http.Client{
		Transport: tr,
	}
	resp, err := client.Do(req)
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

// ReadWriteWithHash reads data from an io.Reader, writes data to an io.Writer and calculate
// xxHash on the data.
func ReadWriteWithHash(r io.Reader, w io.Writer) (int64, string, error) {
	var (
		total   int64
		bufSize = 32768
	)

	buf := make([]byte, bufSize)
	h := xxhash.New64()
	mw := io.MultiWriter(h, w)
	for {
		n, err := r.Read(buf)
		total += int64(n)
		if err != nil && err != io.EOF {
			return 0, "", err
		}

		if n == 0 {
			break
		}

		mw.Write(buf[:n])
	}

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(h.Sum64()))
	return total, hex.EncodeToString(b), nil
}

// ListBuckets returns all buckets in DFC (cloud and local)
func ListBuckets(proxyURL string, local bool) (*dfc.BucketNames, error) {
	url := proxyURL + "/" + dfc.Rversion + "/" + dfc.Rbuckets + "/*"
	if local {
		url += "?local=true"
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp != nil && resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("HTTP failed, status = %d", resp.StatusCode)
	}

	var b []byte
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	buckets := &dfc.BucketNames{}
	err = json.Unmarshal(b, buckets)
	return buckets, err
}

// GetClusterMap retrives a DFC's server map
// Note: this may not be a good idea to expose the map to clients, but this how it is for now.
func GetClusterMap(url string) (dfc.Smap, error) {
	injson, err := json.Marshal(dfc.GetMsg{GetWhat: dfc.GetWhatSmap})
	if err != nil {
		return dfc.Smap{}, fmt.Errorf("Failed to marshal GetStatsMsg: %v", err)
	}

	req, err := http.NewRequest("GET", url+"/"+dfc.Rversion+"/"+dfc.Rdaemon, bytes.NewBuffer(injson))
	if err != nil {
		return dfc.Smap{}, fmt.Errorf("Failed to create request: %v", err)
	}

	r, err := client.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()

	if err != nil {
		// Note: might return connection refused if the servet is not ready
		//       caller can retry in that case
		return dfc.Smap{}, err
	}

	if r != nil && r.StatusCode >= http.StatusBadRequest {
		return dfc.Smap{}, fmt.Errorf("get cluster map, http status %d", r.StatusCode)
	}

	var (
		b    []byte
		smap dfc.Smap
	)
	b, err = ioutil.ReadAll(r.Body)
	if err != nil {
		return dfc.Smap{}, fmt.Errorf("Failed to read response body")
	}

	err = json.Unmarshal(b, &smap)
	if err != nil {
		return dfc.Smap{}, fmt.Errorf("Failed to unmarshal smap: %v", err)
	}

	return smap, nil
}

// GetPrimaryProxy returns the primary proxy's url of a cluster
func GetPrimaryProxy(url string) (string, error) {
	smap, err := GetClusterMap(url)
	if err != nil {
		return "", err
	}

	return smap.ProxySI.DirectURL, nil
}

// HTTPRequest sends one HTTP request and checks result
func HTTPRequest(method string, url string, msg io.Reader) error {
	req, err := http.NewRequest(method, url, msg)
	if err != nil {
		return fmt.Errorf("Failed to create request, err = %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Failed to do request, err = %v", err)
	}

	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("Failed to read response body, err = %v", err)
		}

		return fmt.Errorf("HTTP error = %d, message = %s", resp.StatusCode, string(b))
	}

	return nil
}

// GetLocalBucketNames returns list of all local buckets.
func GetLocalBucketNames(proxyurl string) (*dfc.BucketNames, error) {
	url := proxyurl + "/" + dfc.Rversion + "/" + dfc.Rbuckets + "/*?local=true"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to create request, err = %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Failed to do request, err = %v", err)
	}

	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read response body, err = %v", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("HTTP error = %d, message = %s", resp.StatusCode, string(b))
	}

	var buckets dfc.BucketNames
	err = json.Unmarshal(b, &buckets)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal bucket names, err: %v", err)
	}

	return &buckets, nil
}

// DoesLocalBucketExist queries a proxy or target to get a list of all local buckets, returns true if
// the bucket exists.
func DoesLocalBucketExist(serverURL string, bucket string) (bool, error) {
	buckets, err := GetLocalBucketNames(serverURL)
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
