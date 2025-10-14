// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// usage 1: initialize and fill out HTTP request.
// usage 2: intra-cluster control-plane (except streams)
// usage 3: PUT and APPEND API
// BodyR optimizes-out allocations - if non-nil and implements `io.Closer`, will always be closed by `client.Do`
type HreqArgs struct {
	BodyR    io.ReadCloser
	Header   http.Header // request headers
	Query    url.Values  // query, e.g. ?a=x&b=y&c=z
	RawQuery string      // raw query
	Method   string
	Base     string // base URL, e.g. http://xyz.abc
	Path     string // path URL, e.g. /x/y/z
	Body     []byte
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

// TODO: remaining usage - `api` package (remove when time permits)
func (u *HreqArgs) ReqDeprecated() (*http.Request, error) {
	r := u.BodyR
	if r == nil && u.Body != nil {
		r = cos.NewByteReader(u.Body)
	}
	req, err := http.NewRequestWithContext(context.Background(), u.Method, u.URL(), r)
	if err != nil {
		return nil, err
	}
	if u.Header != nil {
		CopyHeaders(req.Header, u.Header)
	}
	return req, nil
}

// NOTE: unlike standard http.NewRequest (above)
// - this method returns context-less request
// - it also does not assign req.GetBody - it is the caller's responsibility to assign one when and if required
func (u *HreqArgs) Req() (*http.Request, error) {
	var (
		l  = len(u.Body)
		rc io.ReadCloser
	)
	switch {
	case u.BodyR != nil:
		rc = u.BodyR
	case l > 0:
		rc = cos.NewByteReader(u.Body)
	default:
		rc = http.NoBody
	}

	req, err := newRequest(u.Method, u.URL())
	if err != nil {
		return nil, err
	}

	req.Body = rc
	req.ContentLength = int64(l) // todo: preferably, with BodyR case as well
	if u.Header != nil {
		CopyHeaders(req.Header, u.Header)
	}
	return req, nil
}

// TODO: HreqFree(original req) vs "shallow copy" produced by WithContext
func (u *HreqArgs) ReqWith(timeout time.Duration) (*http.Request, context.Context, context.CancelFunc, error) {
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

func splitUpath(surl string) (prefix, path string) {
	const (
		minSchemeHostLen = 10
	)
	i := strings.Index(surl, apc.URLPathObjects.S)
	if i <= minSchemeHostLen {
		return "", ""
	}
	prefix, path = surl[:i], surl[i:]
	for _, c := range path { // reject control characters and space
		if c <= 0x20 || c == 0x7f {
			return "", ""
		}
	}
	return prefix, path
}

func newRequest(method, surl string) (*http.Request, error) {
	var (
		err error
		u   *url.URL
	)
	// 1. fast path: split and reuse "/v1/objects/" url
	prefix, path := splitUpath(surl)
	if prefix == "" {
		u, err = url.Parse(surl)
		if err != nil {
			return nil, err
		}
	} else if parsed, ok := hmap.Load(prefix); ok {
		u = parsed.(*url.URL)
		tmp := *u // shallow copy (not to mutate the cached one)

		p, q, ok := strings.Cut(path, "?")
		if ok {
			tmp.RawQuery = q
		} else {
			tmp.RawQuery = ""
		}
		decoded, _ := url.PathUnescape(p)
		tmp.Path = decoded
		tmp.RawPath = p

		u = &tmp
	} else {
		u, err = url.Parse(surl)
		if err != nil {
			return nil, err
		}
		hmap.Store(prefix, u)
	}

	// 2. reuse and initialize request
	req := hreqAlloc()
	req.Method = method
	req.URL = u
	req.Proto = "HTTP/1.1"
	req.ProtoMajor = 1
	req.ProtoMinor = 1
	if req.Header == nil {
		req.Header = make(http.Header, 4)
	}
	req.Host = u.Host

	return req, nil
}
