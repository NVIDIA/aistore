// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
)

func TestParseURLScheme(t *testing.T) {
	testCases := []struct{ expectedScheme, expectedAddress, url string }{
		{"http", "localhost:8080", "http://localhost:8080"},
		{"https", "localhost", "https://localhost"},
		{"", "localhost:8080", "localhost:8080"},
	}

	for _, tc := range testCases {
		scheme, address := cmn.ParseURLScheme(tc.url)
		tassert.Errorf(t, scheme == tc.expectedScheme, "expected scheme %s, got %s", tc.expectedScheme, scheme)
		tassert.Errorf(t, address == tc.expectedAddress, "expected address %s, got %s", tc.expectedAddress, address)
	}
}

func TestReparseQuery(t *testing.T) {
	var (
		versionID = "1"
		uuid      = "R9oLVoEsxx"
	)

	r := &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Path: fmt.Sprintf("/s3/imagenet-tar/oisubset-train-0000.tar?%s=%s", cmn.URLParamUUID, uuid),
		},
	}
	q := url.Values{}
	q.Add("versionID", versionID)
	r.URL.RawQuery = q.Encode()

	cmn.ReparseQuery(r)
	actualVersionID, actualUUID := r.URL.Query().Get("versionID"), r.URL.Query().Get(cmn.URLParamUUID)
	tassert.Errorf(t, actualVersionID == versionID, "expected versionID to be %q, got %q", versionID, actualVersionID)
	tassert.Errorf(t, actualUUID == uuid, "expected %s to be %q, got %q", cmn.URLParamUUID, uuid, actualUUID)
}
