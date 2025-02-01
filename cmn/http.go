// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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

// Ref: https://www.rfc-editor.org/rfc/rfc7233#section-2.1
// (compare w/ htrange.contentRange)
func MakeRangeHdr(start, length int64) string {
	debug.Assert(start != 0 || length != 0)
	return fmt.Sprintf("%s%d-%d", cos.HdrRangeValPrefix, start, start+length-1)
}

// Ref: https://www.rfc-editor.org/rfc/rfc7233#section-4.2
func ParseRangeHdr(contentRange string) (start, length, objectSize int64, err error) {
	var (
		end int64
		n   int
	)
	n, err = fmt.Sscanf(contentRange, "bytes %d-%d/%d", &start, &end, &objectSize)
	if n != 3 {
		err = fmt.Errorf("contentRange (\"%s\") malformed", contentRange)
		return
	}
	length = end + 1 - start
	return
}

// ParseURL splits URL path at "/" and matches resulting items against the specified, if any.
// - splitAfter == true:  strings.Split() the entire path;
// - splitAfter == false: strings.SplitN(len(itemsPresent)+itemsAfter)
// Returns all items that follow the specified `items`.

const maxItems = 1000

func ParseURL(path string, itemsPresent []string, itemsAfter int, splitAfter bool) ([]string, error) {
	// path.Clean(string) reduced to this:
	for path != "" && path[0] == '/' {
		path = path[1:]
	}

	var (
		split []string
		l     = len(itemsPresent)
	)
	if splitAfter {
		split = strings.SplitN(path, "/", maxItems)
	} else {
		split = strings.SplitN(path, "/", l+max(1, itemsAfter))
	}

	apiItems := split[:0] // filtering without allocation
	for _, item := range split {
		if item != "" { // omit empty
			apiItems = append(apiItems, item)
		}
	}
	if len(apiItems) < l {
		return nil, fmt.Errorf("invalid URL '%s': expected %d items, got %d", path, l, len(apiItems))
	}
	for idx, item := range itemsPresent {
		if item != apiItems[idx] {
			return nil, fmt.Errorf("invalid URL '%s': expected '%s', got '%s'", path, item, apiItems[idx])
		}
	}

	apiItems = apiItems[l:]
	if len(apiItems) < itemsAfter {
		return nil, fmt.Errorf("URL '%s' is too short: expected %d items, got %d", path, itemsAfter+l, len(apiItems)+l)
	}
	if len(apiItems) > itemsAfter && !splitAfter {
		return nil, fmt.Errorf("URL '%s' is too long: expected %d items, got %d", path, itemsAfter+l, len(apiItems)+l)
	}
	return apiItems, nil
}

func ReadBytes(r *http.Request) (b []byte, err error) {
	var e error

	b, e = cos.ReadAllN(r.Body, r.ContentLength)
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
			args.Sleep = Rom.CplaneOperation() / 4
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
				nlog.Warningf("%s successful %s after (soft/hard errors: %d/%d, last: %v)",
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
			nlog.Errorf("%s failed to %s, iter %d, err: %v(%d)", callerStr, args.Action, iter, err, status)
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
				sleep = min(sleep+(args.Sleep/2), Rom.MaxKeepalive())
			}
		}
		if hardErrCnt > args.HardErr || softErrCnt > args.SoftErr {
			break
		}
		time.Sleep(sleep)
	}
	// Quiet: print once the summary (Verbose: no need)
	if args.Verbosity == RetryLogQuiet {
		nlog.Errorf("%sfailed to %s (soft/hard errors: %d/%d, last: %v)",
			callerStr, args.Action, softErrCnt, hardErrCnt, err)
	}
	return
}

func ParseReadHeaderTimeout() (_ time.Duration, isSet bool) {
	val := os.Getenv(apc.EnvReadHeaderTimeout)
	if val == "" {
		return 0, false
	}
	timeout, err := time.ParseDuration(val)
	if err != nil {
		nlog.Errorf("invalid env '%s = %s': %v - ignoring, proceeding with default = %v",
			apc.EnvReadHeaderTimeout, val, err, apc.ReadHeaderTimeout)
		return 0, false
	}
	return timeout, true
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

func MaxParallelism() int { return max(sys.NumCPU(), 4) }
