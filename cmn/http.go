/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package cmn provides common low-level types and utilities for all dfcpub projects
package cmn

import (
	"fmt"
	"html"
	"net/http"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

// URLPath returns a HTTP URL path by joining all segments with "/"
func URLPath(segments ...string) string {
	return path.Join("/", path.Join(segments...))
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
		return nil, fmt.Errorf("path is too short %d, expected more items %d", len(apiItems)+len(items), itemsAfter+len(items))
	}

	return apiItems, nil
}

// ErrHTTP returns a formatted error string for an HTTP request.
func ErrHTTP(r *http.Request, msg string, status int) string {
	return http.StatusText(status) + ": " + msg + ": " + r.Method + " " + r.URL.Path + " from " + r.RemoteAddr
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

	s := ErrHTTP(r, msg, status)
	http.Error(w, s, status)
}

// InvalidHandlerDetailed writes detailed error (includes line and file) to response writer.
func InvalidHandlerDetailed(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	status := http.StatusBadRequest
	if len(errCode) > 0 {
		status = errCode[0]
	}
	errMsg := ErrHTTP(r, msg, status)
	stack := "("
	for i := 1; i < 4; i++ {
		if _, file, line, ok := runtime.Caller(i); ok {
			if !strings.Contains(msg, ".go, #") {
				f := filepath.Base(file)
				if stack != "(" {
					stack += " -> "
				}
				stack += fmt.Sprintf("[%s, #%d]", f, line)
			}
		}
	}
	stack += ")"
	errMsg += "| " + stack

	glog.Errorln(errMsg)
	http.Error(w, errMsg, status)
}
