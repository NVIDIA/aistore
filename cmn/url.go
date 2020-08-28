// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"path/filepath"
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

func JoinPath(urlBase, path string) string {
	url := strings.TrimSuffix(urlBase, "/")
	if !strings.HasPrefix(path, "/") {
		url += "/"
	}
	return url + path
}

func RawURL2BckObj(rawURL string) (bck Bck, objName, origURLBck string, err error) {
	var (
		urlObj *url.URL
	)
	urlObj, err = url.ParseRequestURI(rawURL)
	if err != nil {
		return
	}
	bck.Name, objName, origURLBck = URL2BckObj(urlObj)
	bck.Provider = ProviderHTTP
	bck.Ns = NsGlobal
	return
}

func URL2BckObj(u *url.URL) (bckName, objName, origURLBck string) {
	origURLBck, objName = filepath.Split(u.Path)
	origURLBck = u.Scheme + "://" + u.Host + origURLBck
	bckName = OrigURLBck2Name(origURLBck)
	return
}

func OrigURLBck2Name(origURLBck string) (bckName string) {
	_, b := ParseURLScheme(origURLBck)
	b1 := xxhash.ChecksumString64S(b, MLCG32)
	b2 := strconv.FormatUint(b1, 16)
	bckName = base64.RawURLEncoding.EncodeToString([]byte(b2))
	return
}
