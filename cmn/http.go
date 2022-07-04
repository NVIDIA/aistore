// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bytes"
	"context"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

type (
	// HTTPRange specifies the byte range to be sent to the client.
	HTTPRange struct {
		Start, Length int64
	}

	// HreqArgs specifies HTTP request that we want to send.
	HreqArgs struct {
		Method string      // GET, POST, ...
		Header http.Header // request headers
		Base   string      // base URL: http://xyz.abc
		Path   string      // path URL: /x/y/z
		Query  url.Values  // query: ?a=x&b=y
		Body   []byte      // body for [POST, PUT, ...]
		// BodyR is an alternative to `Body` to avoid unnecessary allocations
		// when body for [POST, PUT, ...] is in stored `io.Reader`.
		// If non-nil and implements `io.Closer`, it will be closed by `client.Do`,
		// even on errors.
		BodyR io.Reader
	}

	RetryArgs struct {
		Call    func() (int, error)
		IsFatal func(error) bool

		Action string
		Caller string

		SoftErr uint // How many retires on ConnectionRefused or ConnectionReset error.
		HardErr uint // How many retries on any other error.
		Sleep   time.Duration

		Verbosity int  // Determine the verbosity level.
		BackOff   bool // If requests should be retried less and less often.
		IsClient  bool // true: client (e.g. tutils, etc.)
	}

	ErrRangeNoOverlap struct {
		ranges []string // RFC 7233
		size   int64    // [0, size)
	}
)

const (
	RetryLogVerbose = iota
	RetryLogQuiet
	RetryLogOff
)

// ErrNoOverlap is returned by serveContent's parseRange if first-byte-pos of
// all of the byte-range-spec values is greater than the content size.
func (e *ErrRangeNoOverlap) Error() string {
	msg := fmt.Sprintf("overlap with the content [0, %d)", e.size)
	if len(e.ranges) == 1 {
		return fmt.Sprintf("range %q does not %s", e.ranges[0], msg)
	}
	return fmt.Sprintf("none of the ranges %v %s", e.ranges, msg)
}

// PrependProtocol prepends protocol in URL in case it is missing.
// By default it adds `http://` as prefix to the URL.
func PrependProtocol(url string, protocol ...string) string {
	if url == "" || strings.Contains(url, "://") {
		return url
	}
	proto := httpProto
	if len(protocol) == 1 {
		proto = protocol[0]
	}
	return proto + "://" + url
}

// MatchRESTItems splits url path and matches the parts against specified `items`.
// If `splitAfter` is true all items will be split, otherwise the
// rest of the path will be split only to `itemsAfter` items.
// Returns all items that follow the specified `items`.
func MatchRESTItems(unescapedPath string, itemsAfter int, splitAfter bool, items []string) ([]string, error) {
	var split []string
	escaped := html.EscapeString(unescapedPath)
	if len(escaped) > 0 && escaped[0] == '/' {
		escaped = escaped[1:] // remove leading slash
	}
	if splitAfter {
		split = strings.Split(escaped, "/")
	} else {
		split = strings.SplitN(escaped, "/", len(items)+cos.Max(1, itemsAfter))
	}
	apiItems := split[:0] // filtering without allocation
	for _, item := range split {
		if item != "" { // omit empty
			apiItems = append(apiItems, item)
		}
	}
	if len(apiItems) < len(items) {
		return nil, fmt.Errorf("expected %d items, but got: %d", len(items), len(apiItems))
	}

	for idx, item := range items {
		if item != apiItems[idx] {
			return nil, fmt.Errorf("expected %s in path, but got: %s", item, apiItems[idx])
		}
	}

	apiItems = apiItems[len(items):]
	if len(apiItems) < itemsAfter {
		return nil, fmt.Errorf("path is too short: got %d items, but expected %d",
			len(apiItems)+len(items), itemsAfter+len(items))
	} else if len(apiItems) > itemsAfter && !splitAfter {
		return nil, fmt.Errorf("path is too long: got %d items, but expected %d",
			len(apiItems)+len(items), itemsAfter+len(items))
	}

	return apiItems, nil
}

func ReadBytes(r *http.Request) (b []byte, err error) {
	var e error

	b, e = io.ReadAll(r.Body)
	if e != nil {
		err = fmt.Errorf("failed to read %s request, err: %v", r.Method, e)
		if e == io.EOF {
			trailer := r.Trailer.Get("Error")
			if trailer != "" {
				err = fmt.Errorf("failed to read %s request, err: %v, trailer: %s", r.Method, e, trailer)
			}
		}
	}
	cos.Close(r.Body)

	return b, err
}

func ReadJSON(w http.ResponseWriter, r *http.Request, out interface{}) (err error) {
	err = jsoniter.NewDecoder(r.Body).Decode(out)
	cos.Close(r.Body)
	if err == nil {
		return
	}
	return WriteErrJSON(w, r, out, err)
}

func WriteErrJSON(w http.ResponseWriter, r *http.Request, out interface{}, err error) error {
	at := thisNodeName
	if thisNodeName == "" {
		at = r.URL.Path
	}
	err = fmt.Errorf(FmtErrUnmarshal, at, fmt.Sprintf("[%T]", out), r.Method, err)
	if _, file, line, ok := runtime.Caller(2); ok {
		f := filepath.Base(file)
		err = fmt.Errorf("%v (%s, #%d)", err, f, line)
	}
	WriteErr(w, r, err)
	return err
}

// WriteJSON writes a struct or byte slice to an HTTP response.
// If `v` is a byte slice, it is passed as-is(already JSON-encoded data).
// In other cases, `v` is encoded to JSON and then passed.
// The function returns an error if writing to the response fails.
func WriteJSON(w http.ResponseWriter, v interface{}) error {
	w.Header().Set(cos.HdrContentType, cos.ContentJSON)
	if b, ok := v.([]byte); ok {
		_, err := w.Write(b)
		return err
	}
	return jsoniter.NewEncoder(w).Encode(v)
}

// Copies headers from original request(from client) to
// a new one(inter-cluster call)
func copyHeaders(src http.Header, dst *http.Header) {
	for k, values := range src {
		for _, v := range values {
			dst.Set(k, v)
		}
	}
}

func (r HTTPRange) ContentRange(size int64) string {
	return fmt.Sprintf("%s%d-%d/%d", cos.HdrContentRangeValPrefix, r.Start, r.Start+r.Length-1, size)
}

// ParseMultiRange parses a Range Header string as per RFC 7233.
// ErrNoOverlap is returned if none of the ranges overlap with the [0, size) content.
func ParseMultiRange(s string, size int64) (ranges []HTTPRange, err error) {
	var noOverlap bool
	if !strings.HasPrefix(s, cos.HdrRangeValPrefix) {
		return nil, fmt.Errorf("read range %q is invalid (prefix)", s)
	}
	allRanges := strings.Split(s[len(cos.HdrRangeValPrefix):], ",")
	for _, ra := range allRanges {
		ra = strings.TrimSpace(ra)
		if ra == "" {
			continue
		}
		i := strings.Index(ra, "-")
		if i < 0 {
			return nil, fmt.Errorf("read range %q is invalid (-)", s)
		}
		var (
			r          HTTPRange
			start, end = strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
		)
		if start == "" {
			// If no start is specified, end specifies the range start relative
			// to the end of the file, and we are dealing with <suffix-length>
			// which has to be a non-negative integer as per RFC 7233 Section 2.1 "Byte-Ranges".
			if end == "" || end[0] == '-' {
				return nil, fmt.Errorf("read range %q is invalid as per RFC 7233 Section 2.1", ra)
			}
			i, err := strconv.ParseInt(end, 10, 64)
			if i < 0 || err != nil {
				return nil, fmt.Errorf("read range %q is invalid (see RFC 7233 Section 2.1)", ra)
			}
			if i > size {
				i = size
			}
			r.Start = size - i
			r.Length = size - r.Start
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i < 0 {
				return nil, fmt.Errorf("read range %q is invalid (start)", ra)
			}
			if i >= size {
				// If the range begins after the size of the content it does not overlap.
				noOverlap = true
				continue
			}
			r.Start = i
			if end == "" {
				// If no end is specified, range extends to the end of the file.
				r.Length = size - r.Start
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.Start > i {
					return nil, fmt.Errorf("read range %q is invalid (end)", ra)
				}
				if i >= size {
					i = size - 1
				}
				r.Length = i - r.Start + 1
			}
		}
		ranges = append(ranges, r)
	}

	if noOverlap && len(ranges) == 0 {
		return nil, &ErrRangeNoOverlap{allRanges, size}
	}
	return ranges, nil
}

func RangeHdr(start, length int64) (hdr http.Header) {
	if start == 0 && length == 0 {
		return hdr
	}
	hdr = make(http.Header, 1)
	hdr.Add(cos.HdrRange, fmt.Sprintf("%s%d-%d", cos.HdrRangeValPrefix, start, start+length-1))
	return
}

func NetworkCallWithRetry(args *RetryArgs) (err error) {
	var (
		hardErrCnt, softErrCnt, iter uint
		status                       int
		nonEmptyErr                  error
		callerStr                    string
		sleep                        = args.Sleep
	)
	if args.Sleep == 0 {
		if args.IsClient {
			args.Sleep = time.Second / 2
		} else {
			args.Sleep = Timeout.CplaneOperation() / 4
		}
	}
	if args.Caller != "" {
		callerStr = args.Caller + ": "
	}
	if args.Action == "" {
		args.Action = "call"
	}
	for hardErrCnt, softErrCnt, iter = uint(0), uint(0), uint(1); ; iter++ {
		if status, err = args.Call(); err == nil {
			if args.Verbosity == RetryLogVerbose && (hardErrCnt > 0 || softErrCnt > 0) {
				glog.Warningf("%s Successful %s after (soft/hard errors: %d/%d, last: %v)",
					callerStr, args.Action, softErrCnt, hardErrCnt, nonEmptyErr)
			}
			return
		}
		// handle
		nonEmptyErr = err
		if args.IsFatal != nil && args.IsFatal(err) {
			return
		}
		if args.Verbosity == RetryLogVerbose {
			glog.Errorf("%s Failed to %s, iter %d, err: %v(%d)", callerStr, args.Action, iter, err, status)
		}
		if cos.IsRetriableConnErr(err) {
			softErrCnt++
		} else {
			hardErrCnt++
		}
		if args.BackOff && iter > 1 {
			if args.IsClient {
				sleep = cos.MinDuration(sleep+(args.Sleep/2), 4*time.Second)
			} else {
				sleep = cos.MinDuration(sleep+(args.Sleep/2), Timeout.MaxKeepalive())
			}
		}
		if hardErrCnt > args.HardErr || softErrCnt > args.SoftErr {
			break
		}
		time.Sleep(sleep)
	}
	// Quiet: print once the summary (Verbose: no need)
	if args.Verbosity == RetryLogQuiet {
		glog.Errorf("%sFailed to %s (soft/hard errors: %d/%d, last: %v)",
			callerStr, args.Action, softErrCnt, hardErrCnt, err)
	}
	return
}

//////////////
// HreqArgs //
//////////////

var (
	hraPool sync.Pool
	hra0    HreqArgs
)

func AllocHra() (a *HreqArgs) {
	if v := hraPool.Get(); v != nil {
		a = v.(*HreqArgs)
		return
	}
	return &HreqArgs{}
}

func FreeHra(a *HreqArgs) {
	*a = hra0
	hraPool.Put(a)
}

func (u *HreqArgs) URL() string {
	url := cos.JoinPath(u.Base, u.Path)
	query := u.Query.Encode()
	if query != "" {
		url += "?" + query
	}
	return url
}

func (u *HreqArgs) Req() (*http.Request, error) {
	r := u.BodyR
	if r == nil && u.Body != nil {
		r = bytes.NewBuffer(u.Body)
	}
	req, err := http.NewRequest(u.Method, u.URL(), r)
	if err != nil {
		return nil, err
	}
	if u.Header != nil {
		copyHeaders(u.Header, &req.Header)
	}
	return req, nil
}

// ReqWithCancel creates request with ability to cancel it.
func (u *HreqArgs) ReqWithCancel() (*http.Request, context.Context, context.CancelFunc, error) {
	req, err := u.Req()
	if err != nil {
		return nil, nil, nil, err
	}
	if u.Method == http.MethodPost || u.Method == http.MethodPut {
		req.Header.Set(cos.HdrContentType, cos.ContentJSON)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	return req, ctx, cancel, nil
}

func (u *HreqArgs) ReqWithTimeout(timeout time.Duration) (*http.Request, context.Context, context.CancelFunc, error) {
	req, err := u.Req()
	if err != nil {
		return nil, nil, nil, err
	}
	if u.Method == http.MethodPost || u.Method == http.MethodPut {
		req.Header.Set(cos.HdrContentType, cos.ContentJSON)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req = req.WithContext(ctx)
	return req, ctx, cancel, nil
}
