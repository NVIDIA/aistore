// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/OneOfOne/xxhash"
)

const (
	gsStorageURL    = "storage.googleapis.com"
	gsAPIURL        = "www.googleapis.com"
	gsAPIPathPrefix = "/storage/v1"

	s3UrlRegex = `(s3-|s3\.)?(.*)\.amazonaws\.com`

	azBlobURL = ".blob.core.windows.net"
)

func IsHTTPS(url string) bool { return strings.HasPrefix(url, "https://") }
func IsHTTP(url string) bool  { return strings.HasPrefix(url, "http://") }
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

// Splits url into [(scheme)://](address).
// It's not possible to use url.Parse as (from url.Parse() docs)
// 'Trying to parse a hostname and path without a scheme is invalid'
func ParseURLScheme(url string) (scheme, address string) {
	s := strings.SplitN(url, "://", 2)
	if len(s) == 1 {
		return "", s[0]
	}
	return s[0], s[1]
}

// WARNING: `ReparseQuery` might affect non-tensorflow clients using S3-compatible API
// with AIStore. To be used with caution.
func ReparseQuery(r *http.Request) {
	if strings.ContainsRune(r.URL.Path, '?') {
		q := r.URL.Query()
		tmpURL, err := url.Parse(r.URL.Path)
		debug.AssertNoErr(err)
		for k, v := range tmpURL.Query() {
			q.Add(k, strings.Join(v, ","))
		}

		r.URL.Path = tmpURL.Path
		r.URL.RawQuery = q.Encode()
	}
}

// JoinWords uses forward slash to join any number of words into a single path.
// Returned path is prefixed with a slash.
func JoinWords(w string, words ...string) (path string) {
	path = w
	if path[:1] != "/" {
		path = "/" + path
	}
	for _, s := range words {
		path += "/" + s
	}
	return
}

// JoinPath joins two path elements that may (or may not) be prefixed/suffixed with a slash.
func JoinPath(url, path string) string {
	suffix := url[len(url)-1:] == "/"
	prefix := path[:1] == "/"
	if suffix && prefix {
		return url + path[1:]
	}
	if !suffix && !prefix {
		return url + "/" + path
	}
	return url + path
}

func OrigURLBck2Name(origURLBck string) (bckName string) {
	_, b := ParseURLScheme(origURLBck)
	b1 := xxhash.ChecksumString64S(b, MLCG32)
	b2 := strconv.FormatUint(b1, 16)
	bckName = base64.RawURLEncoding.EncodeToString([]byte(b2))
	return
}
