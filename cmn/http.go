// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/sys"
	jsoniter "github.com/json-iterator/go"
)

const (
	RetryLogVerbose = iota
	RetryLogQuiet
	RetryLogOff
)

type (
	// usage 1: initialize and fill out HTTP request.
	// usage 2: intra-cluster control-plane (except streams)
	// usage 3: PUT and APPEND API
	// BodyR optimizes-out allocations - if non-nil and implements `io.Closer`, will always be closed by `client.Do`
	HreqArgs struct {
		BodyR    io.Reader
		Header   http.Header // request headers
		Query    url.Values  // query, e.g. ?a=x&b=y&c=z
		RawQuery string      // raw query
		Method   string
		Base     string // base URL, e.g. http://xyz.abc
		Path     string // path URL, e.g. /x/y/z
		Body     []byte
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
		IsClient  bool // true: client (e.g. dev tools, etc.)
	}
)

// PrependProtocol prepends protocol in URL in case it is missing.
// By default it adds `http://` to the URL.
func PrependProtocol(url string, protocol ...string) string {
	if url == "" || strings.Contains(url, "://") {
		return url
	}
	proto := "http"
	if len(protocol) == 1 {
		proto = protocol[0]
	}
	return proto + "://" + url // rfc2396.txt
}

// (compare w/ htrange.contentRange)
func MakeRangeHdr(start, length int64) (hdr http.Header) {
	if start == 0 && length == 0 {
		return hdr
	}
	hdr = make(http.Header, 1)
	hdr.Set(cos.HdrRange, fmt.Sprintf("%s%d-%d", cos.HdrRangeValPrefix, start, start+length-1))
	return
}

// ParseURL splits URL path at "/" and matches resulting items against the specified, if any.
// - splitAfter == true:  strings.Split() the entire path;
// - splitAfter == false: strings.SplitN(len(items)+itemsAfter)
// Returns all items that follow the specified `items`.
func ParseURL(unescapedPath string, itemsAfter int, splitAfter bool, items []string) ([]string, error) {
	var (
		split   []string
		escaped = html.EscapeString(unescapedPath)
	)
	if len(escaped) > 0 && escaped[0] == '/' {
		escaped = escaped[1:] // remove leading slash
	}
	if splitAfter {
		split = strings.Split(escaped, "/")
	} else {
		split = strings.SplitN(escaped, "/", len(items)+max(1, itemsAfter))
	}
	apiItems := split[:0] // filtering without allocation
	for _, item := range split {
		if item != "" { // omit empty
			apiItems = append(apiItems, item)
		}
	}
	if len(apiItems) < len(items) {
		return nil, fmt.Errorf("invalid URL path '%s': expected %d items, got %d",
			unescapedPath, len(items), len(apiItems))
	}
	for idx, item := range items {
		if item != apiItems[idx] {
			return nil, fmt.Errorf("invalid URL '%s': expected '%s', got '%s'",
				unescapedPath, item, apiItems[idx])
		}
	}
	apiItems = apiItems[len(items):]
	if len(apiItems) < itemsAfter {
		return nil, fmt.Errorf("URL path '%s' is too short: expected %d items, got %d",
			unescapedPath, itemsAfter+len(items), len(apiItems)+len(items))
	}
	if len(apiItems) > itemsAfter && !splitAfter {
		return nil, fmt.Errorf("URL path '%s' is too long: expected %d items, got %d",
			unescapedPath, itemsAfter+len(items), len(apiItems)+len(items))
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

func ReadJSON(w http.ResponseWriter, r *http.Request, out any) (err error) {
	err = jsoniter.NewDecoder(r.Body).Decode(out)
	cos.Close(r.Body)
	if err == nil {
		return
	}
	return WriteErrJSON(w, r, out, err)
}

func WriteErrJSON(w http.ResponseWriter, r *http.Request, out any, err error) error {
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

// Copies headers from original request(from client) to
// a new one(inter-cluster call)
func copyHeaders(src http.Header, dst *http.Header) {
	for k, values := range src {
		for _, v := range values {
			dst.Set(k, v)
		}
	}
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
				nlog.Warningf("%s Successful %s after (soft/hard errors: %d/%d, last: %v)",
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
			nlog.Errorf("%s Failed to %s, iter %d, err: %v(%d)", callerStr, args.Action, iter, err, status)
		}
		if cos.IsRetriableConnErr(err) {
			softErrCnt++
		} else {
			hardErrCnt++
		}
		if args.BackOff && iter > 1 {
			if args.IsClient {
				sleep = min(sleep+(args.Sleep/2), 4*time.Second)
			} else {
				sleep = min(sleep+(args.Sleep/2), Timeout.MaxKeepalive())
			}
		}
		if hardErrCnt > args.HardErr || softErrCnt > args.SoftErr {
			break
		}
		time.Sleep(sleep)
	}
	// Quiet: print once the summary (Verbose: no need)
	if args.Verbosity == RetryLogQuiet {
		nlog.Errorf("%sFailed to %s (soft/hard errors: %d/%d, last: %v)",
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
	if u.RawQuery != "" {
		return url + "?" + u.RawQuery
	}
	if rawq := u.Query.Encode(); rawq != "" {
		return url + "?" + rawq
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

//
// number of intra-cluster broadcasting goroutines
//

func MaxBcastParallel() int { return max(sys.NumCPU(), 4) }
