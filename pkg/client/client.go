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

	"net/url"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/OneOfOne/xxhash"
)

const (
	registerTimeout    = time.Minute * 2
	maxBodyErrorLength = 256
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

var (
	transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 60 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 600 * time.Second,
		MaxIdleConnsPerHost: 100, // arbitrary number, to avoid connect: cannot assign requested address
	}
	client = &http.Client{
		Timeout:   600 * time.Second,
		Transport: transport,
	}
	Mem2 *memsys.Mem2
)

func init() {
	Mem2 = &memsys.Mem2{Period: time.Minute * 2, Name: "ClientMem2"}
	Mem2.Init()
	go Mem2.Run()
}

// populateData reads data from random source and writes to a writer,
// calculates and returns xxhash (if needed)
func populateData(w io.Writer, size int64, withHash bool, rnd *rand.Rand) (string, error) {
	var (
		left = size
		hash string
		h    *xxhash.XXHash64
	)
	blk, s := Mem2.AllocFromSlab2(common.MiB)
	blkSize := int64(len(blk))
	defer s.Free(blk)

	if withHash {
		h = xxhash.New64()
	}
	for i := int64(0); i <= size/blkSize; i++ {
		n := common.MinI64(blkSize, left)
		rnd.Read(blk[:n])
		m, err := w.Write(blk[:n])
		if err != nil {
			return "", err
		}

		if withHash {
			h.Write(blk[:m])
		}

		left -= int64(m)
	}
	if withHash {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(h.Sum64()))
		hash = hex.EncodeToString(b)
	}
	return hash, nil
}

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

type InvalidCksumError struct {
	ExpectedHash string
	ActualHash   string
}

type BucketProps struct {
	CloudProvider   string
	Versioning      string
	NextTierURL     string
	ReadPolicy      string
	WritePolicy     string
	ChecksumType    string
	ValidateColdGet string
	ValidateWarmGet string
	ValidateRange   string
	LowWM           string
	HighWM          string
	AtimeCacheMax   string
	DontEvictTime   string
	CapUpdTime      string
	LRUEnabled      string
}

type ObjectProps struct {
	Size    int
	Version string
}

func (err ReqError) Error() string {
	return err.message
}

func (e InvalidCksumError) Error() string {
	return fmt.Sprintf("Expected Hash: [%s] Actual Hash: [%s]", e.ExpectedHash, e.ActualHash)
}

func newReqError(msg string, code int) ReqError {
	return ReqError{
		code:    code,
		message: msg,
	}
}

func newInvalidCksumError(eHash string, aHash string) InvalidCksumError {
	return InvalidCksumError{
		ActualHash:   aHash,
		ExpectedHash: eHash,
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

		bufreader := bufio.NewReader(r.Body)
		if validate {
			length, hash, err = ReadWriteWithHash(bufreader, w)
			if err != nil {
				return 0, "", fmt.Errorf("Failed to read HTTP response, err: %v", err)
			}
		} else {
			if length, err = io.Copy(w, bufreader); err != nil {
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

func get(url, bucket string, keyname string, wg *sync.WaitGroup, errch chan error,
	silent bool, validate bool, w io.Writer, query url.Values) (int64, HTTPLatencies, error) {
	var (
		hash, hdhash, hdhashtype string
	)

	if wg != nil {
		defer wg.Done()
	}

	url += api.URLPath(api.Version, api.Objects, bucket, keyname)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, HTTPLatencies{}, err
	}
	req.URL.RawQuery = query.Encode() // golang handles query == nil

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

	if validate && resp != nil {
		hdhash = resp.Header.Get(api.HeaderDFCChecksumVal)
		hdhashtype = resp.Header.Get(api.HeaderDFCChecksumType)
	}

	v := hdhashtype == dfc.ChecksumXXHash
	len, hash, err := readResponse(resp, w, err, fmt.Sprintf("GET (object %s from bucket %s)", keyname, bucket), v)
	if v {
		if hdhash != hash {
			err = newInvalidCksumError(hdhash, hash)
			if errch != nil {
				errch <- err
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

// Get sends a GET request to url and discards returned data
func Get(url, bucket string, keyname string, wg *sync.WaitGroup, errch chan error,
	silent bool, validate bool) (int64, HTTPLatencies, error) {
	return get(url, bucket, keyname, wg, errch, silent, validate, ioutil.Discard, nil)
}

// Get sends a GET request to url and discards the return
func GetWithQuery(url, bucket string, keyname string, wg *sync.WaitGroup, errch chan error,
	silent bool, validate bool, q url.Values) (int64, HTTPLatencies, error) {
	return get(url, bucket, keyname, wg, errch, silent, validate, ioutil.Discard, q)
}

// GetFile sends a GET request to url and writes the data to io.Writer
func GetFile(url, bucket string, keyname string, wg *sync.WaitGroup, errch chan error,
	silent bool, validate bool, w io.Writer) (int64, HTTPLatencies, error) {
	return get(url, bucket, keyname, wg, errch, silent, validate, w, nil)
}

// GetFile sends a GET request to url and writes the data to io.Writer
func GetFileWithQuery(url, bucket string, keyname string, wg *sync.WaitGroup, errch chan error,
	silent bool, validate bool, w io.Writer, query url.Values) (int64, HTTPLatencies, error) {
	return get(url, bucket, keyname, wg, errch, silent, validate, w, query)
}

func Del(proxyURL, bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool) (err error) {
	if wg != nil {
		defer wg.Done()
	}
	url := proxyURL + api.URLPath(api.Version, api.Objects, bucket, keyname)
	if !silent {
		fmt.Printf("DEL: %s\n", keyname)
	}
	req, httperr := http.NewRequest(http.MethodDelete, url, nil)
	if httperr != nil {
		err = fmt.Errorf("Failed to create new HTTP request, err: %v", httperr)
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
func ListBucket(proxyURL, bucket string, msg *api.GetMsg, objectCountLimit int) (*api.BucketList, error) {
	url := proxyURL + api.URLPath(api.Version, api.Buckets, bucket)
	reslist := &api.BucketList{Entries: make([]*api.BucketEntry, 0, 1000)}

	// An optimization to read as few objects from bucket as possible.
	// toRead is the current number of objects ListBucket must read before
	// returning the list. Every cycle the loop reads objects by pages and
	// decreases toRead by the number of received objects. When toRead gets less
	// than pageSize, the loop does the final request with reduced pageSize
	toRead := objectCountLimit
	for {
		var resp *http.Response

		if toRead != 0 {
			if (msg.GetPageSize == 0 && toRead < api.DefaultPageSize) ||
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
			injson, err = json.Marshal(api.ActionMsg{Action: api.ActListObjects, Value: msg})
			if err != nil {
				return nil, err
			}
			resp, err = client.Post(url, "application/json", bytes.NewBuffer(injson))
		}

		if err != nil {
			return nil, err
		}

		defer func() {
			if resp != nil {
				resp.Body.Close()
			}
		}()

		page := &api.BucketList{}
		page.Entries = make([]*api.BucketEntry, 0, 1000)
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
	EvictMsg := api.ActionMsg{Action: api.ActEvict}
	EvictMsg.Name = bucket + "/" + fname
	injson, err = json.Marshal(EvictMsg)
	if err != nil {
		return fmt.Errorf("Failed to marshal EvictMsg, err: %v", err)
	}

	url := proxyURL + api.URLPath(api.Version, api.Objects, bucket, fname)
	return HTTPRequest(http.MethodDelete, url, NewBytesReader(injson))
}

func doListRangeCall(proxyURL, bucket, action, method string, listrangemsg interface{}, wait bool) error {
	var (
		injson []byte
		err    error
	)
	actionMsg := api.ActionMsg{Action: action, Value: listrangemsg}
	injson, err = json.Marshal(actionMsg)
	if err != nil {
		return fmt.Errorf("Failed to marhsal api.ActionMsg, err: %v", err)
	}
	url := proxyURL + api.URLPath(api.Version, api.Buckets, bucket)
	headers := map[string]string{
		"Content-Type": "application/json",
	}

	return HTTPRequest(method, url, NewBytesReader(injson), headers)
}

func PrefetchList(proxyURL, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := api.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := api.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeCall(proxyURL, bucket, api.ActPrefetch, http.MethodPost, prefetchMsg, wait)
}

func PrefetchRange(proxyURL, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	prefetchMsgBase := api.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := api.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: prefetchMsgBase}
	return doListRangeCall(proxyURL, bucket, api.ActPrefetch, http.MethodPost, prefetchMsg, wait)
}

func DeleteList(proxyURL, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := api.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := api.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeCall(proxyURL, bucket, api.ActDelete, http.MethodDelete, deleteMsg, wait)
}

func DeleteRange(proxyURL, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := api.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := api.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeCall(proxyURL, bucket, api.ActDelete, http.MethodDelete, deleteMsg, wait)
}

func EvictList(proxyURL, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := api.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := api.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeCall(proxyURL, bucket, api.ActEvict, http.MethodDelete, evictMsg, wait)
}

func EvictRange(proxyURL, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := api.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := api.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeCall(proxyURL, bucket, api.ActEvict, http.MethodDelete, evictMsg, wait)
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

func HeadBucket(proxyURL, bucket string) (*BucketProps, error) {
	r, err := client.Head(proxyURL + api.URLPath(api.Version, api.Buckets, bucket))
	if err != nil {
		return nil, err
	}
	defer func() {
		r.Body.Close()
	}()
	if r.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf(
				"ioutil.ReadAll falled on response body, err: %v, HTTP status code: %d", err, r.StatusCode)
		}
		return nil, fmt.Errorf("head bucket: %s failed, HTTP status code: %d, HTTP response body: %s",
			bucket, r.StatusCode, string(b))
	}
	return &BucketProps{
		CloudProvider:   r.Header.Get(api.HeaderCloudProvider),
		Versioning:      r.Header.Get(api.HeaderVersioning),
		NextTierURL:     r.Header.Get(api.HeaderNextTierURL),
		ReadPolicy:      r.Header.Get(api.HeaderReadPolicy),
		WritePolicy:     r.Header.Get(api.HeaderWritePolicy),
		ChecksumType:    r.Header.Get(api.HeaderBucketChecksumType),
		ValidateColdGet: r.Header.Get(api.HeaderBucketValidateColdGet),
		ValidateWarmGet: r.Header.Get(api.HeaderBucketValidateWarmGet),
		ValidateRange:   r.Header.Get(api.HeaderBucketValidateRange),
		LowWM:           r.Header.Get(api.HeaderBucketLRULowWM),
		HighWM:          r.Header.Get(api.HeaderBucketLRUHighWM),
		AtimeCacheMax:   r.Header.Get(api.HeaderBucketAtimeCacheMax),
		DontEvictTime:   r.Header.Get(api.HeaderBucketDontEvictTime),
		CapUpdTime:      r.Header.Get(api.HeaderBucketCapUpdTime),
		LRUEnabled:      r.Header.Get(api.HeaderBucketLRUEnabled),
	}, nil
}

func HeadObject(proxyURL, bucket, objname string) (objProps *ObjectProps, err error) {
	objProps = &ObjectProps{}
	r, err := client.Head(proxyURL + api.URLPath(api.Version, api.Objects, bucket, objname))
	if err != nil {
		return
	}
	defer func() {
		r.Body.Close()
	}()
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		b, ioErr := ioutil.ReadAll(r.Body)
		if ioErr != nil {
			err = fmt.Errorf("Failed to read response, err: %v", ioErr)
			return
		}
		err = fmt.Errorf("HEAD bucket/object: %s/%s failed, HTTP status: %d, HTTP response: %s",
			bucket, objname, r.StatusCode, string(b))
		return
	}
	size, err := strconv.Atoi(r.Header.Get(api.HeaderSize))
	if err != nil {
		return
	}

	objProps.Size = size
	objProps.Version = r.Header.Get(api.HeaderVersion)
	return
}

func SetBucketProps(proxyURL, bucket string, props dfc.BucketProps) error {
	var url = proxyURL + api.URLPath(api.Version, api.Buckets, bucket)

	if props.CksumConf.Checksum == "" {
		props.CksumConf.Checksum = dfc.ChecksumInherit
	}

	b, err := json.Marshal(api.ActionMsg{Action: api.ActSetProps, Value: props})
	if err != nil {
		return err
	}
	return HTTPRequest(http.MethodPut, url, NewBytesReader(b))
}

func ResetBucketProps(proxyURL, bucket string) error {
	url := proxyURL + api.URLPath(api.Version, api.Buckets, bucket)

	b, err := json.Marshal(api.ActionMsg{Action: api.ActResetProps})
	if err != nil {
		return err
	}
	return HTTPRequest(http.MethodPut, url, NewBytesReader(b))
}

func IsCached(proxyURL, bucket, objname string) (bool, error) {
	url := proxyURL + api.URLPath(api.Version, api.Objects, bucket, objname) + "?" + api.URLParamCheckCached + "=true"
	r, err := client.Head(url)
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
	url := proxyURL + api.URLPath(api.Version, api.Objects, bucket, key)
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
		req.Header.Set(api.HeaderDFCChecksumType, dfc.ChecksumXXHash)
		req.Header.Set(api.HeaderDFCChecksumVal, reader.XXHash())
	}

	resp, err := client.Do(req)
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
	msg, err := json.Marshal(api.ActionMsg{Action: api.ActCreateLB})
	if err != nil {
		return err
	}

	url := proxyURL + api.URLPath(api.Version, api.Buckets, bucket)
	err = HTTPRequest(http.MethodPost, url, NewBytesReader(msg))
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
			exists, err := DoesLocalBucketExist(s.PublicNet.DirectURL, name)
			if err != nil {
				return err
			}

			if exists {
				break loop_bucket
			}

			if time.Now().After(to) {
				return fmt.Errorf("wait for local bucket timed out, target = %s", s.PublicNet.DirectURL)
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
			exists, err := DoesLocalBucketExist(s.PublicNet.DirectURL, name)
			if err != nil {
				return err
			}

			if !exists {
				break loop_bucket
			}

			if time.Now().After(to) {
				return fmt.Errorf("timed out waiting for local bucket %s being removed, target %s", name, s.PublicNet.DirectURL)
			}

			time.Sleep(time.Second)
		}
	}

	return nil
}

// RenameLocalBucket changes the name of a bucket to newBucketName
func RenameLocalBucket(proxyURL, bucket, newBucketName string) error {
	msg, err := json.Marshal(api.ActionMsg{Action: api.ActRenameLB, Name: newBucketName})
	if err != nil {
		return err
	}

	url := proxyURL + api.URLPath(api.Version, api.Buckets, bucket)
	err = HTTPRequest(http.MethodPost, url, NewBytesReader(msg))
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
	msg, err := json.Marshal(api.ActionMsg{Action: api.ActDestroyLB})
	if err != nil {
		return err
	}

	url := proxyURL + api.URLPath(api.Version, api.Buckets, bucket)
	err = HTTPRequest(http.MethodDelete, url, NewBytesReader(msg))
	if err != nil {
		return err
	}

	return waitForNoLocalBucket(proxyURL, bucket)
}

// ListObjects returns a slice of object names of all objects that match the prefix in a bucket
func ListObjects(proxyURL, bucket, prefix string, objectCountLimit int) ([]string, error) {
	msg := &api.GetMsg{GetPrefix: prefix}
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
	url := server + api.URLPath(api.Version, api.Daemon)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.URL.RawQuery = getWhatRawQuery(api.GetWhatConfig, "")
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

// ListBuckets returns all buckets in DFC (cloud and local)
func ListBuckets(proxyURL string, local bool) (*api.BucketNames, error) {
	url := proxyURL + api.URLPath(api.Version, api.Buckets, "*")
	if local {
		url += fmt.Sprintf("?%s=true", api.URLParamLocal)
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

	buckets := &api.BucketNames{}
	if len(b) != 0 {
		err = json.Unmarshal(b, buckets)
	}
	return buckets, err
}

// GetClusterMap retrives a DFC's server map
// Note: this may not be a good idea to expose the map to clients, but this how it is for now.
func GetClusterMap(url string) (dfc.Smap, error) {
	q := getWhatRawQuery(api.GetWhatSmap, "")
	requestURL := fmt.Sprintf("%s?%s", url+api.URLPath(api.Version, api.Daemon), q)
	r, err := client.Get(requestURL)
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
		return dfc.Smap{}, fmt.Errorf("get Smap, HTTP status %d", r.StatusCode)
	}

	var (
		b    []byte
		smap dfc.Smap
	)
	b, err = ioutil.ReadAll(r.Body)
	if err != nil {
		return dfc.Smap{}, fmt.Errorf("Failed to read response, err: %v", err)
	}

	err = json.Unmarshal(b, &smap)
	if err != nil {
		return dfc.Smap{}, fmt.Errorf("Failed to unmarshal Smap, err: %v", err)
	}

	return smap, nil
}

func GetXactionRebalance(proxyURL string) (dfc.RebalanceStats, error) {
	var rebalanceStats dfc.RebalanceStats
	responseBytes, err := getXactionResponse(proxyURL, api.XactionRebalance)
	if err != nil {
		return rebalanceStats, err
	}

	err = json.Unmarshal(responseBytes, &rebalanceStats)
	if err != nil {
		return rebalanceStats,
			fmt.Errorf("Failed to unmarshal rebalance stats: %v", err)
	}

	return rebalanceStats, nil
}

func getXactionResponse(proxyURL string, kind string) ([]byte, error) {
	q := getWhatRawQuery(api.GetWhatXaction, kind)
	url := fmt.Sprintf("%s?%s", proxyURL+api.URLPath(api.Version, api.Cluster), q)
	r, err := client.Get(url)
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
	resp, err := client.Do(req)
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

// GetLocalBucketNames returns list of all local buckets.
func GetLocalBucketNames(proxyURL string) (*api.BucketNames, error) {
	url := proxyURL + api.URLPath(api.Version, api.Buckets, "*") + fmt.Sprintf("?%s=true", api.URLParamLocal)
	b, err := HTTPRequestWithResp(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	var buckets api.BucketNames
	if len(b) != 0 {
		err = json.Unmarshal(b, &buckets)
		if err != nil {
			return nil, fmt.Errorf("Failed to unmarshal bucket names, err: %v - [%s]", err, string(b))
		}
	} else {
		return nil, fmt.Errorf("Empty response instead of empty bucket list from %s\n", proxyURL)
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

func getWhatRawQuery(getWhat string, getProps string) string {
	q := url.Values{}
	q.Add(api.URLParamWhat, getWhat)
	if getProps != "" {
		q.Add(api.URLParamProps, getProps)
	}
	return q.Encode()
}

func TargetMountpaths(targetUrl string) (*api.MountpathList, error) {
	q := getWhatRawQuery(api.GetWhatMountpaths, "")
	url := fmt.Sprintf("%s?%s", targetUrl+api.URLPath(api.Version, api.Daemon), q)

	resp, err := client.Get(url)
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

	mp := &api.MountpathList{}
	err = json.Unmarshal(b, mp)
	return mp, err
}

func EnableTargetMountpath(daemonUrl, mpath string) error {
	url := daemonUrl + api.URLPath(api.Version, api.Daemon, api.Mountpaths)
	msg, err := json.Marshal(api.ActionMsg{Action: api.ActMountpathEnable, Value: mpath})
	if err != nil {
		return err
	}
	return HTTPRequest(http.MethodPost, url, NewBytesReader(msg))
}

func DisableTargetMountpath(daemonUrl, mpath string) error {
	url := daemonUrl + api.URLPath(api.Version, api.Daemon, api.Mountpaths)
	msg, err := json.Marshal(api.ActionMsg{Action: api.ActMountpathDisable, Value: mpath})
	if err != nil {
		return err
	}
	return HTTPRequest(http.MethodPost, url, NewBytesReader(msg))
}

func AddTargetMountpath(daemonUrl, mpath string) error {
	url := daemonUrl + api.URLPath(api.Version, api.Daemon, api.Mountpaths)
	msg, err := json.Marshal(api.ActionMsg{Action: api.ActMountpathAdd, Value: mpath})
	if err != nil {
		return err
	}
	return HTTPRequest(http.MethodPut, url, NewBytesReader(msg))
}

func RemoveTargetMountpath(daemonUrl, mpath string) error {
	url := daemonUrl + api.URLPath(api.Version, api.Daemon, api.Mountpaths)
	msg, err := json.Marshal(api.ActionMsg{Action: api.ActMountpathRemove, Value: mpath})
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

	url := proxyURL + api.URLPath(api.Version, api.Cluster, api.Daemon, sid)
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

func RegisterTarget(sid, targetDirectURL string, smap dfc.Smap) error {
	_, ok := smap.Tmap[sid]
	url := targetDirectURL + api.URLPath(api.Version, api.Daemon, api.Register)
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

func WaitMapVersionSync(timeout time.Time, smap dfc.Smap, prevVersion int64, idsToIgnore []string) error {
	inList := func(s string, values []string) bool {
		for _, v := range values {
			if s == v {
				return true
			}
		}

		return false
	}

	checkAwaitingDaemon := func(smap dfc.Smap, idsToIgnore []string) (string, string, bool) {
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
		if err != nil && !dfc.IsErrConnectionRefused(err) {
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
