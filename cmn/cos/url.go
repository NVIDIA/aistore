// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	gsStorageURL    = "storage.googleapis.com"
	gsAPIURL        = "www.googleapis.com"
	gsAPIPathPrefix = "/storage/v1"

	s3UrlRegex = `(s3-|s3\.)?(.*)\.amazonaws\.com`

	azBlobURL = ".blob.core.windows.net"
)

func IsHTTPS(url string) bool { return strings.HasPrefix(url, "https://") }
func IsHT(url string) bool    { return strings.HasPrefix(url, "http://") }

func ParseURL(s string) (u *url.URL, valid bool) {
	if s == "" {
		return
	}
	var err error
	u, err = url.Parse(s)
	valid = err == nil && u.Scheme != "" && u.Host != ""
	return
}

func IsGoogleStorageURL(u *url.URL) bool {
	return u.Host == gsStorageURL
}

func IsGoogleAPIURL(u *url.URL) bool {
	return u.Host == gsAPIURL && strings.Contains(u.Path, gsAPIPathPrefix)
}

func IsS3URL(link string) bool {
	re := regexp.MustCompile(s3UrlRegex)
	return re.MatchString(link)
}

func IsAzureURL(u *url.URL) bool {
	return strings.Contains(u.Host, azBlobURL)
}

// [TODO]
// func IsOCIURL(u *url.URL) bool {}

// WARNING: `ReparseQuery` might affect non-tensorflow clients using S3-compatible API
// with AIStore. To be used with caution.
func ReparseQuery(r *http.Request) {
	if !strings.ContainsRune(r.URL.Path, '?') {
		return
	}
	q := r.URL.Query()
	tmpURL, err := url.Parse(r.URL.Path)
	debug.AssertNoErr(err)
	for k, v := range tmpURL.Query() {
		q.Add(k, strings.Join(v, ","))
	}
	r.URL.Path = tmpURL.Path
	r.URL.RawQuery = q.Encode()
}

// join an absolute path (must start with '/') with additional optional segments
func JoinWP(path string, words ...string) string {
	debug.Assert(path != "" && path[0] == '/', path)
	var l = len(path)
	for _, w := range words {
		l += 1 + len(w)
	}
	b := make([]byte, 0, l)
	b = append(b, path...)
	for _, w := range words {
		b = append(b, '/')
		b = append(b, w...)
	}
	return UnsafeS(b)
}

// join a first segment (that must NOT start with '/') with additional optional segments
func JoinW0(w0 string, words ...string) string {
	debug.Assert(w0 != "" && w0[0] != '/', w0)
	var l = len(w0) + 1
	for _, w := range words {
		l += 1 + len(w)
	}
	b := make([]byte, 0, l)
	b = append(b, '/')
	b = append(b, w0...)
	for _, w := range words {
		b = append(b, '/')
		b = append(b, w...)
	}
	return UnsafeS(b)
}

// join two paths
func JoinPath(url, path string) string {
	suffix := IsLastB(url, '/')
	prefix := path[0] == '/'
	if suffix && prefix {
		return url + path[1:]
	}
	if !suffix && !prefix {
		return url + "/" + path
	}
	return url + path
}

// merge new query parameters into the existing URL
func JoinQuery(base string, newQuery url.Values) string {
	url, valid := ParseURL(base)
	if !valid || len(newQuery) == 0 {
		return base
	}

	query := url.Query()
	for key, values := range newQuery {
		for _, value := range values {
			query.Add(key, value)
		}
	}
	url.RawQuery = query.Encode()
	return url.String()
}
