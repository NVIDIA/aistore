// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	jsoniter "github.com/json-iterator/go"
)

// Error structure for HTTP errors
type HTTPError struct {
	Status     int    `json:"status"`
	Message    string `json:"message"`
	Method     string `json:"method"`
	URLPath    string `json:"url_path"`
	RemoteAddr string `json:"remote_addr"`
	Trace      string `json:"trace"`
}

// Eg: Bad Request: Bucket abc does not appear to be local or does not exist:
//   DELETE /v1/buckets/abc from 127.0.0.1:54064| ([httpcommon.go, #840] <- [proxy.go, #484] <- [proxy.go, #264])
func (e *HTTPError) String() string {
	return http.StatusText(e.Status) + ": " + e.Message + ": " + e.Method + " " + e.URLPath + " from " + e.RemoteAddr + "| (" + e.Trace + ")"
}

// Implements error interface
func (e *HTTPError) Error() string {
	// Stop from escaping <, > ,and &
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(e); err != nil {
		return err.Error()
	}
	return buf.String()
}

// newHTTPError returns a HTTPError struct.
// There are cases where the message is already formatted as a HTTPError (from target)
// in which case, returns true otherwise false
// NOTE: The format of the error message is being used in the CLI
// If there are any changes, please make sure to update `errorHandler` in the CLI
func NewHTTPError(r *http.Request, msg string, status int) (*HTTPError, bool) {
	var httpErr HTTPError
	if err := jsoniter.UnmarshalFromString(msg, &httpErr); err == nil {
		return &httpErr, true
	}
	return &HTTPError{Status: status, Message: msg, Method: r.Method, URLPath: r.URL.Path, RemoteAddr: r.RemoteAddr}, false
}

// URLPath returns a HTTP URL path by joining all segments with "/"
func URLPath(segments ...string) string {
	return path.Join("/", path.Join(segments...))
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

// RESTItems splits whole path into the items.
func RESTItems(unescapedPath string) []string {
	escaped := html.EscapeString(unescapedPath)
	split := strings.Split(escaped, "/")
	apiItems := split[:0] // filtering without allocation
	for _, item := range split {
		if item != "" { // omit empty
			apiItems = append(apiItems, item)
		}
	}
	return apiItems
}

// MatchRESTItems splits url path into api items and match them with provided
// items. If splitAfter is set to true all items will be split, otherwise the
// rest of the path will be splited only to itemsAfter items. Returns all items
// which come after all of the provided items
func MatchRESTItems(unescapedPath string, itemsAfter int, splitAfter bool, items ...string) ([]string, error) {
	var split []string
	escaped := html.EscapeString(unescapedPath)
	if len(escaped) > 0 && escaped[0] == '/' {
		escaped = escaped[1:] // remove leading slash
	}
	if splitAfter {
		split = strings.Split(escaped, "/")
	} else {
		split = strings.SplitN(escaped, "/", itemsAfter+len(items))
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
		return nil, fmt.Errorf("path is too short: got %d items, but expected %d", len(apiItems)+len(items), itemsAfter+len(items))
	}

	return apiItems, nil
}

func InvalidHandler(w http.ResponseWriter, r *http.Request) {
	InvalidHandlerWithMsg(w, r, "invalid request")
}

// InvalidHandlerWithMsg writes error to response writer.
func InvalidHandlerWithMsg(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	status := http.StatusBadRequest
	if len(errCode) != 0 {
		status = errCode[0]
	}

	err, _ := NewHTTPError(r, msg, status)
	http.Error(w, err.Error(), status)
}

// InvalidHandlerDetailed writes detailed error (includes line and file) to response writer.
func InvalidHandlerDetailed(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	status := http.StatusBadRequest
	if len(errCode) > 0 && errCode[0] >= http.StatusBadRequest {
		status = errCode[0]
	}

	err, isHTTPError := NewHTTPError(r, msg, status)

	if isHTTPError {
		glog.Errorln(err.String())
		http.Error(w, err.Error(), status)
		return
	}

	var errMsg bytes.Buffer
	if !strings.Contains(msg, ".go, #") {
		for i := 1; i < 4; i++ {
			if _, file, line, ok := runtime.Caller(i); ok {
				f := filepath.Base(file)
				if i > 1 {
					errMsg.WriteString(" <- ")
				}
				fmt.Fprintf(&errMsg, "[%s, #%d]", f, line)
			}
		}
	}
	err.Trace = errMsg.String()
	glog.Errorln(err.String())
	http.Error(w, err.Error(), status)
}

func ReadBytes(r *http.Request) (b []byte, errDetails string, err error) {
	b, err = ioutil.ReadAll(r.Body)
	if err != nil {
		errDetails = fmt.Sprintf("Failed to read %s request, err: %v", r.Method, err)
		if err == io.EOF {
			trailer := r.Trailer.Get("Error")
			if trailer != "" {
				errDetails = fmt.Sprintf("Failed to read %s request, err: %v, trailer: %s", r.Method, err, trailer)
			}
		}
	}
	r.Body.Close()

	return b, errDetails, err
}

func ReadJSON(w http.ResponseWriter, r *http.Request, out interface{}) error {
	getErrorLine := func() string {
		if _, file, line, ok := runtime.Caller(2); ok {
			f := filepath.Base(file)
			return fmt.Sprintf("(%s, #%d)", f, line)
		}
		return ""
	}

	b, errstr, err := ReadBytes(r)
	if err != nil {
		InvalidHandlerDetailed(w, r, errstr)
		return err
	}

	err = jsoniter.Unmarshal(b, out)
	if err != nil {
		s := fmt.Sprintf("Failed to json-unmarshal %s request, err: %v [%v]", r.Method, err, string(b))
		s += getErrorLine()

		InvalidHandlerDetailed(w, r, s)
		return err
	}
	return nil
}

// ReqWithContext executes request with ability to cancel it.
func ReqWithContext(method, url string, body []byte) (*http.Request, context.Context, context.CancelFunc, error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, nil, nil, err
	}
	if method == http.MethodPost || method == http.MethodPut {
		req.Header.Set("Content-Type", "application/json")
	}
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	return req, ctx, cancel, nil
}
