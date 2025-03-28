// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"

	jsoniter "github.com/json-iterator/go"
)

const (
	RetryLogVerbose = iota
	RetryLogQuiet
	RetryLogOff
)

type (
	RetryArgs struct {
		Call    func() (int, error)
		IsFatal func(error) bool

		Action string
		Caller string

		SoftErr int // How many retires on ConnectionRefused or ConnectionReset error.
		HardErr int // How many retries on any other error.
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

func NetworkCallWithRetry(args *RetryArgs) (ecode int, err error) {
	var (
		hardErrCnt, softErrCnt, iter int
		lastErr                      error
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
	for iter = 1; ; iter++ {
		if ecode, err = args.Call(); err == nil {
			if args.Verbosity == RetryLogVerbose && (hardErrCnt > 0 || softErrCnt > 0) {
				nlog.Warningf("%s successful %s after (soft/hard errors: %d/%d, last: %v)",
					callerStr, args.Action, softErrCnt, hardErrCnt, lastErr)
			}
			return 0, nil
		}
		lastErr = err

		// handle
		if args.IsFatal != nil && args.IsFatal(err) {
			return ecode, err
		}
		if args.Verbosity == RetryLogVerbose {
			nlog.Errorf("%s failed to %s, iter %d, err: %v(%d)", callerStr, args.Action, iter, err, ecode)
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
	return ecode, err
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
