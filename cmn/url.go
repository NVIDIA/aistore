// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"net/url"
	"regexp"
	"strings"
)

const (
	gsStorageURL    = "storage.googleapis.com"
	gsAPIURL        = "www.googleapis.com"
	gsAPIPathPrefix = "/storage/v1"

	s3UrlRegex = `(s3-|s3\.)?(.*)\.amazonaws\.com`

	azBlobURL = ".blob.core.windows.net"
)

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
